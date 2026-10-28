// Package postgres implements Pacelock admission in short PostgreSQL transactions.
// All operations within a scope serialize on one domain row. Quota updates and
// retained decisions commit together. It uses database/sql without selecting a SQL
// transport; the application registers a PostgreSQL driver and owns the primary DB.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/tr1v3r/pacelock"
)

// Driver uses logged tables and Read Committed transactions. A domain lock covers
// initially absent quota/receipt rows as well as existing rows. No operation waits
// for quota recovery or executes external business work. Connection/lock waits are
// bounded by the caller's context and server timeouts. No automatic retries occur.
type Driver struct{ db *sql.DB }

var _ pacelock.Driver = (*Driver)(nil)

//go:embed schema.sql
var schema string

// New validates a caller-owned primary database handle without connecting.
// Migrate must be run explicitly before admission. The caller closes db.
func New(db *sql.DB) (*Driver, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: primary database is required", pacelock.ErrInvalid)
	}
	return &Driver{db: db}, nil
}

// Migrate creates the fixed logged tables in the connection's search_path.
// Use a controlled schema/search_path and migration role. Existing incompatible
// tables are not upgraded. Constructors never execute schema changes.
func (d *Driver) Migrate(ctx context.Context) error {
	if err := d.ready(ctx); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Split statements for transports that disallow multi-command prepared queries.
	for _, statement := range splitSchema() {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func splitSchema() []string {
	var statements []string
	for _, part := range strings.Split(schema, ";") {
		if statement := strings.TrimSpace(part); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

func (d *Driver) ready(ctx context.Context) error {
	if ctx == nil || d == nil || d.db == nil {
		return fmt.Errorf("%w: database and context are required", pacelock.ErrInvalid)
	}
	return ctx.Err()
}

// scopeID preserves arbitrary input bytes without exposing raw identifiers in SQL.
func scopeID(s pacelock.Scope) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(s.Namespace), s.Namespace, s.Domain))))
}
func itemID(name string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(name))) }

// lockScope protects missing rows too. Every mutator, including cleanup, follows
// this protocol. Domain rows are never automatically deleted or recreated.
func lockScope(ctx context.Context, tx *sql.Tx, id string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO pacelock_domains(scope_id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
		return err
	}
	var locked string
	return tx.QueryRowContext(ctx, `SELECT scope_id FROM pacelock_domains WHERE scope_id=$1 FOR UPDATE`, id).Scan(&locked)
}

func backendTime(ctx context.Context, tx *sql.Tx) (int64, error) {
	var now int64
	err := tx.QueryRowContext(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&now)
	return now, err
}

func fail(op string, outcome pacelock.Outcome, err error) error {
	if !errors.Is(err, pacelock.ErrInvalid) && !errors.Is(err, pacelock.ErrPolicyMismatch) && !errors.Is(err, pacelock.ErrConflict) && !errors.Is(err, pacelock.ErrState) {
		err = fmt.Errorf("%w: %w", pacelock.ErrBackend, err)
	}
	return &pacelock.Error{Op: op, Outcome: outcome, Err: err}
}

// Admit validates, locks the domain, samples backend time once, and calculates the
// shared GCRA transition. Every quota and optional receipt commits in one transaction.
// Before Commit is sent, failures are NotCharged; any Commit error is conservatively
// Unknown. A prior ambiguous invocation remains ambiguous even after a later error.
func (d *Driver) Admit(ctx context.Context, a pacelock.Admission) (pacelock.Receipt, error) {
	op := "allow"
	if a.ID != "" {
		op = "allow_once"
	}
	uncharged := func(err error) (pacelock.Receipt, error) {
		return pacelock.Receipt{}, fail(op, pacelock.NotCharged, err)
	}
	if err := a.Validate(); err != nil {
		return uncharged(err)
	}
	if err := d.ready(ctx); err != nil {
		return uncharged(err)
	}
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return uncharged(err)
	}
	defer tx.Rollback()
	scope := scopeID(a.Scope)
	if err = lockScope(ctx, tx, scope); err != nil {
		return uncharged(err)
	}
	now, err := backendTime(ctx, tx)
	if err != nil {
		return uncharged(err)
	}
	fingerprint := a.Fingerprint()
	if a.ID != "" {
		var saved string
		var raw []byte
		var expires int64
		err = tx.QueryRowContext(ctx, `SELECT fingerprint,decision,expires_us FROM pacelock_admissions WHERE scope_id=$1 AND admission_id=$2`, scope, itemID(a.ID)).Scan(&saved, &raw, &expires)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return uncharged(err)
		}
		if err == nil && expires > now {
			if saved != fingerprint {
				return uncharged(pacelock.ErrConflict)
			}
			decision, decodeErr := decodeDecision(raw)
			if decodeErr != nil {
				return pacelock.Receipt{}, fail(op, pacelock.Unknown, decodeErr)
			}
			// A retained replay performs no quota mutation; ending the transaction
			// with Rollback releases the lock without a second commit ambiguity.
			return pacelock.Receipt{Decision: decision, Replayed: true}, nil
		}
	}
	states := make([]pacelock.QuotaState, len(a.Rules))
	for i, r := range a.Rules {
		s := &states[i]
		err = tx.QueryRowContext(ctx, `SELECT tat_us,interval_us,burst,expires_us FROM pacelock_quotas WHERE scope_id=$1 AND quota_id=$2`, scope, itemID(r.Name)).Scan(&s.TAT, &s.Interval, &s.Burst, &s.ExpiresAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return uncharged(err)
		}
	}
	decision, next, err := pacelock.Evaluate(now, a.Rules, states)
	if err != nil {
		return uncharged(err)
	}
	if decision.Allowed {
		for i, s := range next {
			_, err = tx.ExecContext(ctx, `INSERT INTO pacelock_quotas(scope_id,quota_id,tat_us,interval_us,burst,expires_us) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(scope_id,quota_id) DO UPDATE SET tat_us=EXCLUDED.tat_us,interval_us=EXCLUDED.interval_us,burst=EXCLUDED.burst,expires_us=EXCLUDED.expires_us`, scope, itemID(a.Rules[i].Name), s.TAT, s.Interval, s.Burst, s.ExpiresAt)
			if err != nil {
				return uncharged(err)
			}
		}
	}
	if a.ID != "" {
		raw, encodeErr := json.Marshal(decision)
		if encodeErr != nil {
			return uncharged(encodeErr)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO pacelock_admissions(scope_id,admission_id,fingerprint,decision,expires_us) VALUES($1,$2,$3,$4,$5) ON CONFLICT(scope_id,admission_id) DO UPDATE SET fingerprint=EXCLUDED.fingerprint,decision=EXCLUDED.decision,expires_us=EXCLUDED.expires_us`, scope, itemID(a.ID), fingerprint, raw, now+a.Retention.Microseconds())
		if err != nil {
			return uncharged(err)
		}
	}
	if err = tx.Commit(); err != nil {
		return pacelock.Receipt{}, fail(op, pacelock.Unknown, err)
	}
	return pacelock.Receipt{Decision: decision}, nil
}

// Lookup reads a complete retained result from the caller-configured primary DB.
// It neither locks the domain nor waits for an in-flight admission to finish. A
// missing row therefore cannot prove no debit. It never refreshes expiry.
func (d *Driver) Lookup(ctx context.Context, r pacelock.LookupRequest) (pacelock.Decision, bool, error) {
	if err := r.Validate(); err != nil {
		return pacelock.Decision{}, false, fail("lookup", pacelock.NotCharged, err)
	}
	if err := d.ready(ctx); err != nil {
		return pacelock.Decision{}, false, fail("lookup", pacelock.NotCharged, err)
	}
	var raw []byte
	err := d.db.QueryRowContext(ctx, `SELECT decision FROM pacelock_admissions WHERE scope_id=$1 AND admission_id=$2 AND expires_us>floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`, scopeID(r.Scope), itemID(r.ID)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return pacelock.Decision{}, false, nil
	}
	if err != nil {
		return pacelock.Decision{}, false, fail("lookup", pacelock.NotCharged, err)
	}
	decision, err := decodeDecision(raw)
	if err != nil {
		return pacelock.Decision{}, false, fail("lookup", pacelock.Unknown, err)
	}
	return decision, true, nil
}

func decodeDecision(raw []byte) (pacelock.Decision, error) {
	var decision pacelock.Decision
	bad := func() (pacelock.Decision, error) {
		return pacelock.Decision{}, fmt.Errorf("%w: malformed admission record", pacelock.ErrState)
	}
	if len(raw) > 65536 || !utf8.Valid(raw) {
		return bad()
	}
	// Accept only the current Decision object, not missing fields, case aliases,
	// duplicate keys or unknown extensions. Limited:null is emitted for allowances.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return bad()
	}
	seen := make(map[string]bool, 3)
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] {
			return bad()
		}
		seen[name] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return bad()
		}
		switch name {
		case "Allowed":
			if bytes.Equal(value, []byte("null")) || json.Unmarshal(value, &decision.Allowed) != nil {
				return bad()
			}
		case "RetryAfter":
			if bytes.Equal(value, []byte("null")) || json.Unmarshal(value, &decision.RetryAfter) != nil {
				return bad()
			}
		case "Limited":
			if json.Unmarshal(value, &decision.Limited) != nil {
				return bad()
			}
		default:
			return bad()
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(seen) != 3 {
		return bad()
	}
	if _, err := decoder.Token(); err != io.EOF {
		return bad()
	}
	if err := decision.Validate(); err != nil {
		return pacelock.Decision{}, err
	}
	return decision, nil
}

// Prune removes up to limit expired quota rows and limit expired admission rows
// in one scope under the same domain lock as Admit. Logical expiry works even if
// cleanup never runs. Domain guard rows remain to avoid lock-identity races. The
// caller schedules maintenance; this driver starts no worker. Commit errors are
// returned without retry, and removal counts must then be ignored.
func (d *Driver) Prune(ctx context.Context, s pacelock.Scope, limit int) (int64, error) {
	if err := s.Validate(); err != nil {
		return 0, err
	}
	if limit < 1 || limit > 10000 {
		return 0, pacelock.ErrInvalid
	}
	if err := d.ready(ctx); err != nil {
		return 0, err
	}
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	id := scopeID(s)
	if err = lockScope(ctx, tx, id); err != nil {
		return 0, err
	}
	now, err := backendTime(ctx, tx)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, statement := range []string{
		`DELETE FROM pacelock_quotas WHERE scope_id=$1 AND quota_id IN (SELECT quota_id FROM pacelock_quotas WHERE scope_id=$1 AND expires_us<=$2 ORDER BY quota_id LIMIT $3)`,
		`DELETE FROM pacelock_admissions WHERE scope_id=$1 AND admission_id IN (SELECT admission_id FROM pacelock_admissions WHERE scope_id=$1 AND expires_us<=$2 ORDER BY admission_id LIMIT $3)`,
	} {
		result, e := tx.ExecContext(ctx, statement, id, now, limit)
		if e != nil {
			return 0, e
		}
		n, e := result.RowsAffected()
		if e != nil {
			return 0, e
		}
		count += n
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}
