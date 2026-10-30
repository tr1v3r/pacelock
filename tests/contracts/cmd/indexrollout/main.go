// Command indexrollout measures the PostgreSQL recovery index in a disposable
// database. It drops only schemas beginning with pacelock_validation_. Never use
// it against production; provide an isolated instance explicitly with -dsn.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	p "github.com/tr1v3r/pacelock"
	pg "github.com/tr1v3r/pacelock/driver/postgres"
)

var ctx = context.Background()
var dsn = flag.String("dsn", "", "isolated PostgreSQL DSN (required)")
var duration = flag.Duration("duration", 3*time.Second, "timed closed-loop write phase")
var warmup = flag.Duration("warmup", time.Second, "write warmup")
var trials = flag.Int("trials", 3, "alternating-order repetitions")
var rows = flag.Int("rows", 100000, "debt rows for prune/DDL")
var workers = flag.Int("workers", 8, "closed-loop write clients")
var backgroundRows = flag.Int("background-rows", 0, "additional unmodified active debts in each write-case table")
var fillfactor = flag.Int("fillfactor", 100, "debt table fillfactor (10..100)")
var mode = flag.String("mode", "all", "all, write, prune, ddl, or math")
var encoder = json.NewEncoder(os.Stdout)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func emit(v any)                             { must(encoder.Encode(v)) }
func exec(db *sql.DB, q string, args ...any) { _, err := db.ExecContext(ctx, q, args...); must(err) }
func open(schema string) *sql.DB {
	cfg, err := pgx.ParseConfig(*dsn)
	must(err)
	if schema != "" {
		cfg.RuntimeParams["search_path"] = schema
	}
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(1)
	return db
}
func scalar(db *sql.DB, q string, args ...any) int64 {
	var n int64
	must(db.QueryRowContext(ctx, q, args...).Scan(&n))
	return n
}
func setup(admin *sql.DB, name string, index bool) (*sql.DB, *pg.Driver) {
	exec(admin, "DROP SCHEMA IF EXISTS "+name+" CASCADE")
	exec(admin, "CREATE SCHEMA "+name)
	db := open(name)
	d, err := pg.New(db)
	must(err)
	must(d.Migrate(ctx))
	exec(db, fmt.Sprintf("ALTER TABLE pacelock_quotas SET (fillfactor=%d, autovacuum_enabled=false)", *fillfactor))
	if !index {
		exec(db, "DROP INDEX pacelock_quotas_recovered_idx")
	}
	return db, d
}

type stats struct{ ActualRows, Updates, HotUpdates, Dead, Live, WAL, TableBytes, PKBytes, RecoveryBytes int64 }

func snapshot(db *sql.DB) stats {
	exec(db, "SELECT pg_stat_clear_snapshot()")
	var s stats
	s.ActualRows = scalar(db, "SELECT count(*) FROM pacelock_quotas")
	must(db.QueryRowContext(ctx, `SELECT n_tup_upd,n_tup_hot_upd,n_dead_tup,n_live_tup FROM pg_stat_user_tables WHERE relid='pacelock_quotas'::regclass`).Scan(&s.Updates, &s.HotUpdates, &s.Dead, &s.Live))
	s.WAL = scalar(db, "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint")
	s.TableBytes = scalar(db, "SELECT pg_table_size('pacelock_quotas')")
	s.PKBytes = scalar(db, "SELECT pg_relation_size('pacelock_quotas_pkey')")
	var exists bool
	must(db.QueryRowContext(ctx, "SELECT to_regclass('pacelock_quotas_recovered_idx') IS NOT NULL").Scan(&exists))
	if exists {
		s.RecoveryBytes = scalar(db, "SELECT pg_relation_size('pacelock_quotas_recovered_idx')")
	}
	return s
}

type latency struct {
	Count                      int
	P50ms, P95ms, P99ms, Maxms float64
}

func dist(values []time.Duration) latency {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) == 0 {
		return latency{}
	}
	at := func(q float64) float64 {
		return float64(values[int(float64(len(values)-1)*q)]) / float64(time.Millisecond)
	}
	return latency{len(values), at(.5), at(.95), at(.99), at(1)}
}
func writeCase(admin *sql.DB, trial int, index bool, scopes, quotas, dims int) {
	name := "pacelock_validation_write"
	db, d := setup(admin, name, index)
	defer db.Close()
	defer exec(admin, "DROP SCHEMA "+name+" CASCADE")
	for i := 0; i < scopes; i++ {
		l, err := p.New(d, "validation", fmt.Sprint(i))
		must(err)
		qs := make([]p.Quota, quotas)
		for j := range qs {
			qs[j] = p.Quota{Name: fmt.Sprint(j), Policy: p.Policy{Revision: 1, Rate: 1000000, Period: time.Second, Burst: 1000000}}
		}
		must(l.UpdateQuotas(ctx, qs...))
	}
	// Prepopulate identical debt rows before either warmup, including unused quotas.
	exec(db, `INSERT INTO pacelock_quotas SELECT scope_id,quota_id,1,1,1000000,1 FROM pacelock_policies`)
	if *backgroundRows > 0 {
		exec(db, "INSERT INTO pacelock_domains VALUES('background')")
		exec(db, `INSERT INTO pacelock_quotas SELECT 'background',md5(i::text),floor(extract(epoch FROM clock_timestamp())*1000000)::bigint+3600000000,3600,1000000,floor(extract(epoch FROM clock_timestamp())*1000000)::bigint+3600000000 FROM generate_series(1,$1::int) i`, *backgroundRows)
	}
	run := func(length time.Duration) ([]time.Duration, int, int, time.Duration) {
		var wg sync.WaitGroup
		var ready sync.WaitGroup
		ready.Add(*workers)
		start := make(chan struct{})
		var mu sync.Mutex
		var all []time.Duration
		denied, failures := 0, 0
		var begin, end time.Time
		for worker := 0; worker < *workers; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				conn := open(name)
				defer conn.Close()
				driver, err := pg.New(conn)
				must(err)
				limiters := make([]*p.Limiter, scopes)
				for i := range limiters {
					limiters[i], err = p.New(driver, "validation", fmt.Sprint(i))
					must(err)
				}
				rng := rand.New(rand.NewPCG(uint64(worker+1), uint64(trial+1)))
				var samples []time.Duration
				localDenied, localErrors := 0, 0
				must(conn.Ping())
				ready.Done()
				<-start
				for time.Now().Before(end) {
					charges := make([]p.Charge, dims)
					offset := rng.IntN(quotas)
					for j := range charges {
						charges[j] = p.Use(fmt.Sprint((offset+j)%quotas), 1)
					}
					l := limiters[rng.IntN(scopes)]
					t := time.Now()
					decision, err := l.Allow(ctx, charges...)
					samples = append(samples, time.Since(t))
					if err != nil {
						localErrors++
					} else if !decision.Allowed {
						localDenied++
					}
				}
				// This connection is also the sole connection used by its Driver. Force its
				// local stats to flush, then synchronize completion before observing counters.
				exec(conn, "SELECT pg_stat_force_next_flush()")
				mu.Lock()
				all = append(all, samples...)
				denied += localDenied
				failures += localErrors
				mu.Unlock()
			}(worker)
		}
		ready.Wait()
		begin = time.Now()
		end = begin.Add(length)
		close(start)
		wg.Wait()
		return all, denied, failures, time.Since(begin)
	}
	exec(db, "ANALYZE pacelock_quotas")
	exec(admin, "CHECKPOINT")
	run(*warmup)
	rowCount := scalar(db, "SELECT count(*) FROM pacelock_quotas")
	if rowCount != int64(scopes*quotas+*backgroundRows) {
		panic("unexpected write row count")
	}
	before := snapshot(db)
	samples, denied, failures, elapsed := run(*duration)
	after := snapshot(db)
	if denied != 0 || failures != 0 || after.Updates-before.Updates != int64(len(samples)*dims) {
		panic("write outcome/stats mismatch")
	}
	emit(map[string]any{"kind": "write", "background_rows": *backgroundRows, "throughput_note": "closed-loop elapsed includes final stats flush, worker aggregation and connection close", "rows": rowCount, "fillfactor": *fillfactor, "trial": trial, "index": index, "scopes": scopes, "quotas_per_scope": quotas, "dimensions": dims, "workers": *workers, "warmup_seconds": warmup.Seconds(), "requested_seconds": duration.Seconds(), "elapsed_seconds": elapsed.Seconds(), "latency": dist(samples), "ops_per_second": float64(len(samples)) / elapsed.Seconds(), "allowed": len(samples) - denied - failures, "denied": denied, "errors": failures, "before": before, "after": after, "updates_delta": after.Updates - before.Updates, "hot_delta": after.HotUpdates - before.HotUpdates, "wal_bytes": after.WAL - before.WAL})
}
func seed(db *sql.DB, expired int) {
	exec(db, "INSERT INTO pacelock_domains VALUES('scope')")
	exec(db, `INSERT INTO pacelock_quotas SELECT 'scope',md5(i::text), CASE WHEN i<=$2 THEN 1 ELSE floor(extract(epoch FROM clock_timestamp())*1000000)::bigint+3600000000 END,1,1000000, CASE WHEN i<=$2 THEN 1 ELSE floor(extract(epoch FROM clock_timestamp())*1000000)::bigint+3600000000 END FROM generate_series(1,$1::int) i`, *rows, expired)
	exec(db, "ANALYZE pacelock_quotas")
}

const oldPrune = `DELETE FROM pacelock_quotas WHERE scope_id='scope' AND quota_id IN (SELECT quota_id FROM pacelock_quotas WHERE scope_id='scope' AND expires_us<=$1 AND tat_us<=$1 ORDER BY quota_id LIMIT 64)`
const newPrune = `DELETE FROM pacelock_quotas WHERE scope_id='scope' AND quota_id IN (SELECT quota_id FROM pacelock_quotas WHERE scope_id='scope' AND GREATEST(expires_us,tat_us)<=$1 ORDER BY GREATEST(expires_us,tat_us),quota_id LIMIT 64)`

func pruneCase(admin *sql.DB, trial int, index bool, percent int, legacy bool) {
	name := "pacelock_validation_prune"
	db, _ := setup(admin, name, index)
	defer db.Close()
	defer exec(admin, "DROP SCHEMA "+name+" CASCADE")
	seed(db, *rows*percent/100)
	query := newPrune
	if legacy {
		query = oldPrune
	}
	var samples []time.Duration
	for i := 0; i < 25; i++ {
		tx, err := db.Begin()
		must(err)
		var now int64
		must(tx.QueryRow("SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint").Scan(&now))
		t := time.Now()
		res, err := tx.Exec(query, now)
		must(err)
		n, err := res.RowsAffected()
		must(err)
		samples = append(samples, time.Since(t))
		if (percent == 0 && n != 0) || (percent > 0 && n != 64) {
			panic(fmt.Sprint("prune count ", n))
		}
		must(tx.Rollback())
	}
	emit(map[string]any{"kind": "prune", "legacy_query": legacy, "trial": trial, "index": index, "rows": *rows, "expired_percent": percent, "latency": dist(samples), "storage": snapshot(db), "note": "SQL DELETE latency; each sample rolled back, warm buffer cache; no network BEGIN/COMMIT included"})
}
func ddlCase(admin *sql.DB, trial int, method string) {
	name := "pacelock_validation_ddl"
	db, d := setup(admin, name, false)
	defer db.Close()
	defer exec(admin, "DROP SCHEMA "+name+" CASCADE")
	seed(db, 0)
	holder := open(name)
	defer holder.Close()
	tx, err := holder.Begin()
	must(err)
	_, err = tx.Exec("UPDATE pacelock_quotas SET tat_us=tat_us+1 WHERE quota_id=md5('1')")
	must(err)
	builder := open(name)
	defer builder.Close()
	builderDriver, err := pg.New(builder)
	must(err)
	_ = d
	var builderPID int
	must(builder.QueryRow("SELECT pg_backend_pid()").Scan(&builderPID))
	done := make(chan time.Duration, 1)
	go func() {
		t := time.Now()
		switch method {
		case "migrate":
			must(builderDriver.Migrate(ctx))
		case "concurrently":
			exec(builder, "CREATE INDEX CONCURRENTLY pacelock_quotas_recovered_idx ON pacelock_quotas(scope_id,(GREATEST(expires_us,tat_us)),quota_id)")
		default:
			exec(builder, "CREATE INDEX pacelock_quotas_recovered_idx ON pacelock_quotas(scope_id,(GREATEST(expires_us,tat_us)),quota_id)")
		}
		done <- time.Since(t)
	}()
	// Wait for an observed lock wait, not a guessed construction-start delay.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting bool
		must(admin.QueryRow("SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1", builderPID).Scan(&waiting))
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			panic("DDL did not enter lock wait")
		}
		time.Sleep(time.Millisecond)
	}
	probe := open(name)
	defer probe.Close()
	must(probe.Ping())
	var probePID int
	must(probe.QueryRow("SELECT pg_backend_pid()").Scan(&probePID))
	probeStart := time.Now()
	probeDone := make(chan time.Duration, 1)
	go func() {
		t := time.Now()
		exec(probe, "UPDATE pacelock_quotas SET tat_us=tat_us+1 WHERE quota_id=md5('2')")
		probeDone <- time.Since(t)
	}()
	time.Sleep(250 * time.Millisecond)
	var probeWaiting bool
	must(admin.QueryRow("SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1", probePID).Scan(&probeWaiting))
	releaseAfterProbe := time.Since(probeStart)
	completedBeforeRelease := false
	var earlyProbe time.Duration
	select {
	case earlyProbe = <-probeDone:
		completedBeforeRelease = true
	default:
	}
	must(tx.Commit())
	buildTime := <-done
	probeTime := earlyProbe
	if !completedBeforeRelease {
		probeTime = <-probeDone
	}
	var valid, ready bool
	must(db.QueryRow("SELECT indisvalid,indisready FROM pg_index WHERE indexrelid='pacelock_quotas_recovered_idx'::regclass").Scan(&valid, &ready))
	emit(map[string]any{"kind": "ddl", "probe_lock_wait_observed": probeWaiting, "holder_release_after_probe_ms": float64(releaseAfterProbe) / float64(time.Millisecond), "probe_completed_before_release": completedBeforeRelease, "index_valid": valid, "index_ready": ready, "trial": trial, "method": method, "rows": *rows, "held_writer_ms": 250, "build_total_ms": float64(buildTime) / float64(time.Millisecond), "probe_write_ms": float64(probeTime) / float64(time.Millisecond), "storage": snapshot(db)})
}
func mathCase() {
	charges := []p.Charge{p.Use("q", 1)}
	policies := []p.QuotaState{{Interval: 1, Burst: 1000000, Revision: 1}}
	debts := []p.QuotaState{{}}
	t := time.Now()
	for i := 0; i < 100000; i++ {
		decision, next, err := p.EvaluateManaged(1000000000+int64(i), charges, policies, debts)
		must(err)
		if !decision.Allowed {
			panic("math denied")
		}
		debts = next
	}
	emit(map[string]any{"kind": "math", "iterations": 100000, "elapsed_ns": time.Since(t).Nanoseconds(), "note": "pure EvaluateManaged; no database/index/I/O"})
}
func main() {
	flag.Parse()
	if *fillfactor < 10 || *fillfactor > 100 || *dsn == "" || *rows < 6400 || *trials < 1 || *workers < 1 || *duration <= 0 || *warmup < 0 {
		panic("invalid flags; isolated -dsn required, rows>=6400")
	}
	admin := open("")
	defer admin.Close()
	must(admin.Ping())
	var version string
	must(admin.QueryRow("SELECT version()").Scan(&version))
	settings := map[string]string{}
	for _, setting := range []string{"shared_buffers", "work_mem", "maintenance_work_mem", "max_connections", "fsync", "full_page_writes", "synchronous_commit", "wal_level", "checkpoint_timeout", "max_wal_size", "autovacuum", "track_counts"} {
		var value string
		must(admin.QueryRow("SHOW " + setting).Scan(&value))
		settings[setting] = value
	}
	emit(map[string]any{"kind": "environment", "background_rows": *backgroundRows, "fillfactor": *fillfactor, "go": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "settings": settings, "postgres": version, "rows": *rows, "trials": *trials, "workers": *workers, "duration": duration.String(), "warmup": warmup.String(), "mode": *mode})
	if *mode == "all" || *mode == "math" {
		mathCase()
	}
	for trial := 0; trial < *trials; trial++ {
		order := []bool{false, true}
		if trial%2 == 1 {
			order = []bool{true, false}
		}
		for _, index := range order {
			if *mode == "all" || *mode == "write" {
				writeCase(admin, trial, index, 1, 1, 1)
				writeCase(admin, trial, index, 16, 32, 4)
			}
			if *mode == "all" || *mode == "prune" {
				pruneCase(admin, trial, index, 0, false)
				pruneCase(admin, trial, index, 1, false)
			}
		}
		if *mode == "all" || *mode == "prune" {
			pruneCase(admin, trial, false, 0, true)
			pruneCase(admin, trial, false, 1, true)
		}
		if *mode == "all" || *mode == "ddl" {
			for _, method := range []string{"create", "migrate", "concurrently"} {
				ddlCase(admin, trial, method)
				ddlUncontended(admin, trial, method)
			}
		}
	}
}

// ddlUncontended isolates build duration from the separately controlled lock wait.
func ddlUncontended(admin *sql.DB, trial int, method string) {
	name := "pacelock_validation_ddl_free"
	db, d := setup(admin, name, false)
	defer db.Close()
	defer exec(admin, "DROP SCHEMA "+name+" CASCADE")
	seed(db, 0)
	t := time.Now()
	switch method {
	case "migrate":
		must(d.Migrate(ctx))
	case "concurrently":
		exec(db, "CREATE INDEX CONCURRENTLY pacelock_quotas_recovered_idx ON pacelock_quotas(scope_id,(GREATEST(expires_us,tat_us)),quota_id)")
	default:
		exec(db, "CREATE INDEX pacelock_quotas_recovered_idx ON pacelock_quotas(scope_id,(GREATEST(expires_us,tat_us)),quota_id)")
	}
	elapsed := time.Since(t)
	var valid, ready bool
	must(db.QueryRow("SELECT indisvalid,indisready FROM pg_index WHERE indexrelid='pacelock_quotas_recovered_idx'::regclass").Scan(&valid, &ready))
	emit(map[string]any{"kind": "ddl_uncontended", "trial": trial, "method": method, "rows": *rows, "elapsed_ms": float64(elapsed) / float64(time.Millisecond), "index_valid": valid, "index_ready": ready})
}
