// Package postgres implements Pacelock admission in short PostgreSQL transactions.
// All operations within a scope serialize on one domain row. Quota updates and
// retained decisions commit together. It uses database/sql without selecting a SQL
// transport; the application registers a PostgreSQL driver and owns the primary DB.
package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tr1v3r/pacelock"
)

// Driver uses logged tables and Read Committed transactions. A domain lock covers
// initially absent quota/receipt rows as well as existing rows. No operation waits
// for quota recovery or executes external business work. Connection/lock waits are
// bounded by the caller's context and server timeouts. No automatic retries occur.
type Driver struct{ db *sql.DB }

var _ pacelock.Driver = (*Driver)(nil)
var _ pacelock.PolicyDriver = (*Driver)(nil)
var _ pacelock.StaticDriver = (*Driver)(nil)

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
// Use a controlled schema/search_path and migration role. Existing quota and
// receipt tables remain unchanged; a separate persistent policy table is added.
// Incompatible schemas are not supported. Constructors never execute schema changes.
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
	if !errors.Is(err, pacelock.ErrInvalid) && !errors.Is(err, pacelock.ErrPolicyMismatch) && !errors.Is(err, pacelock.ErrConflict) && !errors.Is(err, pacelock.ErrState) && !errors.Is(err, pacelock.ErrPaused) && !errors.Is(err, pacelock.ErrPolicyMissing) {
		err = fmt.Errorf("%w: %w", pacelock.ErrBackend, err)
	}
	return &pacelock.Error{Op: op, Outcome: outcome, Err: err}
}

// AdmitStatic validates, locks the domain, samples backend time once, and calculates the
// shared GCRA transition. Every quota and optional receipt commits in one transaction.
// Before Commit is sent, failures are NotCharged; any Commit error is conservatively
// Unknown. A prior ambiguous invocation remains ambiguous even after a later error.
func (d *Driver) AdmitStatic(ctx context.Context, a pacelock.StaticAdmission) (pacelock.Receipt, error) {
	return d.admit(ctx, a, nil)
}

// Admit resolves authoritative policies after locking and receipt replay.
// Callers provide only quota names and costs, never cached policy snapshots.
func (d *Driver) Admit(ctx context.Context, a pacelock.Admission) (pacelock.Receipt, error) {
	return d.admit(ctx, pacelock.StaticAdmission{Scope: a.Scope, ID: a.ID, Retention: a.Retention}, &a)
}

func (d *Driver) admit(ctx context.Context, a pacelock.StaticAdmission, managed *pacelock.Admission) (pacelock.Receipt, error) {
	op := "allow_static"
	if managed != nil {
		op = "allow"
	}
	if a.ID != "" {
		op += "_once"
	}
	uncharged := func(err error) (pacelock.Receipt, error) {
		return pacelock.Receipt{}, fail(op, pacelock.NotCharged, err)
	}
	var names []string
	var fingerprint string
	if managed != nil {
		if err := managed.Validate(); err != nil {
			return uncharged(err)
		}
		fingerprint = managed.Fingerprint()
		for _, c := range managed.Charges {
			names = append(names, c.Name)
		}
	} else {
		if err := a.Validate(); err != nil {
			return uncharged(err)
		}
		for _, r := range a.Rules {
			if r.Policy.Revision != 0 {
				return uncharged(fmt.Errorf("%w: use managed admission for managed policies", pacelock.ErrInvalid))
			}
			names = append(names, r.Name)
		}
		fingerprint = a.Fingerprint()
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
	policies := make([]pacelock.QuotaState, len(names))
	states := make([]pacelock.QuotaState, len(names))
	for i, name := range names {
		policies[i], err = loadPolicy(ctx, tx, scope, name)
		if err != nil {
			return uncharged(err)
		}
		if managed == nil && policies[i] != (pacelock.QuotaState{}) {
			return uncharged(pacelock.ErrPolicyMismatch)
		}
		s := &states[i]
		err = tx.QueryRowContext(ctx, `SELECT tat_us,interval_us,burst,expires_us FROM pacelock_quotas WHERE scope_id=$1 AND quota_id=$2`, scope, itemID(name)).Scan(&s.TAT, &s.Interval, &s.Burst, &s.ExpiresAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return uncharged(err)
		}
	}
	var decision pacelock.Decision
	var next []pacelock.QuotaState
	if managed != nil {
		decision, next, err = pacelock.EvaluateManaged(now, managed.Charges, policies, states)
	} else {
		decision, next, err = pacelock.Evaluate(now, a.Rules, states)
	}
	if err != nil {
		return uncharged(err)
	}
	if decision.Allowed {
		for i, s := range next {
			err = storeQuota(ctx, tx, scope, names[i], s)
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

func storeQuota(ctx context.Context, tx *sql.Tx, scope, name string, s pacelock.QuotaState) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO pacelock_quotas(scope_id,quota_id,tat_us,interval_us,burst,expires_us) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(scope_id,quota_id) DO UPDATE SET tat_us=EXCLUDED.tat_us,interval_us=EXCLUDED.interval_us,burst=EXCLUDED.burst,expires_us=EXCLUDED.expires_us`, scope, itemID(name), s.TAT, s.Interval, s.Burst, s.ExpiresAt)
	return err
}

func loadPolicy(ctx context.Context, tx *sql.Tx, scope, name string) (pacelock.QuotaState, error) {
	var s pacelock.QuotaState
	err := tx.QueryRowContext(ctx, `SELECT interval_us,burst,revision FROM pacelock_policies WHERE scope_id=$1 AND quota_id=$2`, scope, itemID(name)).Scan(&s.Interval, &s.Burst, &s.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return pacelock.QuotaState{}, nil
	}
	return s, err
}

// UpdatePolicies atomically installs managed configuration under the admission
// scope lock. It never reads or writes debt. Same-revision identical updates do
// not rewrite configuration.
// No automatic retry occurs. A commit error is Unknown: policies may have changed,
// even though this control-plane operation never debits quota. Retry the identical
// revisions explicitly to resolve uncertainty (a newer update may supersede them).
func (d *Driver) UpdatePolicies(ctx context.Context, u pacelock.PolicyUpdate) error {
	uncharged := func(err error) error { return fail("update_policies", pacelock.NotCharged, err) }
	if err := u.Validate(); err != nil {
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
	scope := scopeID(u.Scope)
	if err = lockScope(ctx, tx, scope); err != nil {
		return uncharged(err)
	}
	now, err := backendTime(ctx, tx)
	if err != nil {
		return uncharged(err)
	}
	states := make([]pacelock.QuotaState, len(u.Quotas))
	for i, q := range u.Quotas {
		states[i], err = loadPolicy(ctx, tx, scope, q.Name)
		if err != nil {
			return uncharged(err)
		}
	}
	next, err := pacelock.ApplyPolicies(now, u.Quotas, states)
	if err != nil {
		return uncharged(err)
	}
	changed := false
	for i, s := range next {
		if s == states[i] {
			continue
		}
		changed = true
		_, err = tx.ExecContext(ctx, `INSERT INTO pacelock_policies(scope_id,quota_id,interval_us,burst,revision) VALUES($1,$2,$3,$4,$5) ON CONFLICT(scope_id,quota_id) DO UPDATE SET interval_us=EXCLUDED.interval_us,burst=EXCLUDED.burst,revision=EXCLUDED.revision`, scope, itemID(u.Quotas[i].Name), s.Interval, s.Burst, s.Revision)
		if err != nil {
			return uncharged(err)
		}
	}
	// An identical retry is read-only, so rollback avoids commit ambiguity.
	if !changed {
		return nil
	}
	if err = tx.Commit(); err != nil {
		return fail("update_policies", pacelock.Unknown, err)
	}
	return nil
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
	if len(raw) > 65536 {
		return decision, pacelock.ErrState
	}
	if err := json.Unmarshal(raw, &decision); err != nil {
		return decision, fmt.Errorf("%w: malformed admission record", pacelock.ErrState)
	}
	if err := decision.Validate(); err != nil {
		return pacelock.Decision{}, err
	}
	return decision, nil
}

// Prune removes up to limit expired debt rows and limit expired admission rows
// under the same domain lock as Admit. The separate policy table is never pruned.
// Logical debt expiry works even if
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
