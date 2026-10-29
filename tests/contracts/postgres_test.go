package contracts_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
)

func postgresFixture(t *testing.T) (*sql.DB, *postgres.Driver, pacelock.Scope, *contractLimiter) {
	t.Helper()
	dsn := os.Getenv("PACELOCK_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set PACELOCK_POSTGRES_DSN")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { db.Close() })
	d, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := pacelock.Scope{Namespace: "postgres-tests", Domain: freshDomain()}
	l, err := newContractLimiter(d, s.Namespace, s.Domain)
	if err != nil {
		t.Fatal(err)
	}
	return db, d, s, l
}
func pgScopeID(s pacelock.Scope) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(s.Namespace), s.Namespace, s.Domain))))
}
func pgItemID(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }

func TestPostgresRollbackAfterFirstQuotaWrite(t *testing.T) {
	db, _, s, l := postgresFixture(t)
	// A real server-side trigger raises only on this test's second quota. The first
	// quota has already been written when the transaction aborts.
	name := "fail_" + pgItemID(s.Domain)[:16]
	statement := fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.scope_id='%s' AND NEW.quota_id='%s' THEN RAISE EXCEPTION 'injected second quota failure'; END IF; RETURN NEW; END $$`, name, pgScopeID(s), pgItemID("second"))
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Exec("DROP TRIGGER IF EXISTS " + name + " ON pacelock_quotas")
		db.Exec("DROP FUNCTION IF EXISTS " + name + "()")
	})
	if _, err := db.Exec("CREATE TRIGGER " + name + " BEFORE INSERT OR UPDATE ON pacelock_quotas FOR EACH ROW EXECUTE FUNCTION " + name + "()"); err != nil {
		t.Fatal(err)
	}
	_, err := l.allowRules(context.Background(), rule("first", 5, 1), rule("second", 5, 1))
	if err == nil || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatal(err)
	}
	var quotas int
	if err = db.QueryRow(`SELECT count(*) FROM pacelock_quotas WHERE scope_id=$1`, pgScopeID(s)).Scan(&quotas); err != nil {
		t.Fatal(err)
	}
	if quotas != 0 {
		t.Fatalf("partial commit: quotas=%d", quotas)
	}
	if _, err = db.Exec("DROP TRIGGER " + name + " ON pacelock_quotas"); err != nil {
		t.Fatal(err)
	}
	admitted, err := l.allowRules(context.Background(), rule("first", 5, 1), rule("second", 5, 1))
	if err != nil || !admitted.Allowed {
		t.Fatal(admitted, err)
	}
}

type loseCommitConnector struct {
	driver.Connector
	armed atomic.Bool
}

func (c *loseCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &loseCommitConn{Conn: conn, owner: c}, nil
}

type loseCommitConn struct {
	driver.Conn
	owner *loseCommitConnector
}

func (c *loseCommitConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &loseCommitTx{Tx: tx, owner: c.owner}, nil
}
func (c *loseCommitConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return &loseCommitTx{Tx: tx, owner: c.owner}, nil
}

type loseCommitTx struct {
	driver.Tx
	owner *loseCommitConnector
}

func (tx *loseCommitTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.owner.armed.CompareAndSwap(true, false) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func TestPostgresLostCommitReplyIsUnknown(t *testing.T) {
	observerDB, _, s, _ := postgresFixture(t)
	config, err := pgx.ParseConfig(os.Getenv("PACELOCK_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	connector := &loseCommitConnector{Connector: stdlib.GetConnector(*config)}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { db.Close() })
	fault, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	broken, _ := newContractLimiter(fault, s.Namespace, s.Domain)
	if _, err := broken.publishRules(context.Background(), rule("q", 1, 1)); err != nil {
		t.Fatal(err)
	}
	connector.armed.Store(true)
	if _, err := broken.Allow(context.Background(), pacelock.Charge{Name: "q", Cost: 1}); !errors.Is(err, io.ErrUnexpectedEOF) || pacelock.OutcomeOf(err) != pacelock.Unknown {
		t.Fatal(err)
	}
	// Inspect state out of band: this is evidence of mutation, not a recovery API.
	// Do not dispatch another admission after the uncertain result.
	var count int
	if err := observerDB.QueryRow(`SELECT count(*) FROM pacelock_quotas WHERE scope_id=$1`, pgScopeID(s)).Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed mutation missing: count=%d err=%v", count, err)
	}
}

func TestPostgresLockWaitAndPostLockClock(t *testing.T) {
	db, _, s, l := postgresFixture(t)
	r := rule("q", 1, 1)
	r.Policy.Period = 350 * time.Millisecond
	mustAllow(t, l, r)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var id string
	if err = tx.QueryRow(`SELECT scope_id FROM pacelock_domains WHERE scope_id=$1 FOR UPDATE`, pgScopeID(s)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = l.Allow(ctx, pacelock.Charge{Name: r.Name, Cost: r.Cost})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		d, err := l.Allow(ctx, pacelock.Charge{Name: r.Name, Cost: r.Cost})
		if err == nil && !d.Allowed {
			err = fmt.Errorf("stale transaction clock: %+v", d)
		}
		done <- err
	}()
	time.Sleep(450 * time.Millisecond)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresPrunePreservesActiveDebt(t *testing.T) {
	db, d, s, l := postgresFixture(t)
	short := rule("expired", 1, 1)
	short.Policy.Period = 20 * time.Millisecond
	if _, err := l.allowRules(context.Background(), short); err != nil {
		t.Fatal(err)
	}
	mustAllow(t, l, rule("active", 1, 1))
	time.Sleep(40 * time.Millisecond)
	n, err := d.Prune(context.Background(), s, 10)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	decision, err := l.allowRules(context.Background(), rule("active", 1, 1))
	if err != nil || decision.Allowed {
		t.Fatal(decision, err)
	}
	var guards int
	if err = db.QueryRow(`SELECT count(*) FROM pacelock_domains WHERE scope_id=$1`, pgScopeID(s)).Scan(&guards); err != nil || guards != 1 {
		t.Fatal(guards, err)
	}
}
