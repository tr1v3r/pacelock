// Package postgres implements Pacelock admission in short PostgreSQL transactions.
// All operations within a scope serialize on one domain row. Each admission's
// quota charges commit atomically. It uses database/sql without selecting a SQL
// transport; the application registers a PostgreSQL driver and owns the primary DB.
package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/tr1v3r/pacelock"
)

// Driver uses logged tables and Read Committed transactions. A domain lock covers
// initially absent quota rows as well as existing rows. No operation waits
// for quota recovery or executes external business work. Connection/lock waits are
// bounded by the caller's context and server timeouts. No automatic retries occur.
type Driver struct{ db *sql.DB }

var _ pacelock.Driver = (*Driver)(nil)
var _ pacelock.QuotaDriver = (*Driver)(nil)

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
// Use a controlled schema/search_path and migration role. Repeated calls preserve
// current quota debt, policies, and unrelated tables.
// Constructors never execute schema changes.
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
	if !errors.Is(err, pacelock.ErrInvalid) && !errors.Is(err, pacelock.ErrPolicyMismatch) && !errors.Is(err, pacelock.ErrState) && !errors.Is(err, pacelock.ErrPaused) && !errors.Is(err, pacelock.ErrPolicyMissing) {
		err = fmt.Errorf("%w: %w", pacelock.ErrBackend, err)
	}
	return &pacelock.Error{Op: op, Outcome: outcome, Err: err}
}

// Admit resolves authoritative policies after locking the domain and sampling
// backend time once. Callers provide only quota names and costs. Every quota
// commits in one transaction; failures before Commit are NotCharged, and any
// Commit error is conservatively Unknown. No automatic retry occurs.
func (d *Driver) Admit(ctx context.Context, a pacelock.Admission) (pacelock.Decision, error) {
	uncharged := func(err error) (pacelock.Decision, error) {
		return pacelock.Decision{}, fail("allow", pacelock.NotCharged, err)
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

	ids := make([]string, len(a.Charges))
	for i, charge := range a.Charges {
		ids[i] = itemID(charge.Name)
	}
	policies, err := loadBatch(ctx, tx, scope, ids, true)
	if err != nil {
		return uncharged(err)
	}
	states, err := loadBatch(ctx, tx, scope, ids, false)
	if err != nil {
		return uncharged(err)
	}

	decision, next, err := pacelock.EvaluateManaged(now, a.Charges, policies, states)
	if err != nil {
		return uncharged(err)
	}
	if decision.Allowed {
		if err = storeBatch(ctx, tx, scope, ids, next, false); err != nil {
			return uncharged(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return pacelock.Decision{}, fail("allow", pacelock.Unknown, err)
	}
	return decision, nil
}

// placeholders only interpolates locally generated parameter numbers, never data.
func placeholders(first, count int) string {
	var b strings.Builder
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "$%d", first+i)
	}
	return b.String()
}

// loadBatch restores request order explicitly: PostgreSQL need not return rows in
// IN-list order. Absent rows remain zero states for the core's missing semantics.
// The domain lock protects both tables, including rows not yet present.
func loadBatch(ctx context.Context, tx *sql.Tx, scope string, ids []string, policy bool) ([]pacelock.QuotaState, error) {
	table, columns := "pacelock_quotas", "tat_us,interval_us,burst,expires_us"
	if policy {
		table, columns = "pacelock_policies", "interval_us,burst,revision"
	}
	args := make([]any, 1, len(ids)+1)
	args[0] = scope
	positions := make(map[string]int, len(ids))
	for i, id := range ids {
		args = append(args, id)
		positions[id] = i
	}
	rows, err := tx.QueryContext(ctx, "SELECT quota_id,"+columns+" FROM "+table+" WHERE scope_id=$1 AND quota_id IN ("+placeholders(2, len(ids))+")", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := make([]pacelock.QuotaState, len(ids))
	for rows.Next() {
		var id string
		var s pacelock.QuotaState
		if policy {
			err = rows.Scan(&id, &s.Interval, &s.Burst, &s.Revision)
		} else {
			err = rows.Scan(&id, &s.TAT, &s.Interval, &s.Burst, &s.ExpiresAt)
		}
		if err != nil {
			return nil, err
		}
		i, ok := positions[id]
		if !ok {
			return nil, fmt.Errorf("%w: unexpected quota row", pacelock.ErrState)
		}
		states[i] = s
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return states, rows.Close()
}

// storeBatch uses scalar database/sql arguments, not transport-specific arrays.
// Validation of every quota finishes before the single atomic upsert is issued.
func storeBatch(ctx context.Context, tx *sql.Tx, scope string, ids []string, states []pacelock.QuotaState, policy bool) error {
	table, columns := "pacelock_quotas", "tat_us,interval_us,burst,expires_us"
	if policy {
		table, columns = "pacelock_policies", "interval_us,burst,revision"
	}
	var values []string
	var args []any
	for i, s := range states {
		first := len(args) + 1
		args = append(args, scope, ids[i])
		if policy {
			args = append(args, s.Interval, s.Burst, s.Revision)
		} else {
			args = append(args, s.TAT, s.Interval, s.Burst, s.ExpiresAt)
		}
		values = append(values, "("+placeholders(first, len(args)-first+1)+")")
	}
	var assignments []string
	for _, column := range strings.Split(columns, ",") {
		assignments = append(assignments, column+"=EXCLUDED."+column)
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+table+"(scope_id,quota_id,"+columns+") VALUES"+strings.Join(values, ",")+" ON CONFLICT(scope_id,quota_id) DO UPDATE SET "+strings.Join(assignments, ","), args...)
	return err
}

// UpdateQuotas atomically installs managed configuration under the admission
// scope lock. It never reads or writes debt. Same-revision identical updates do
// not rewrite configuration.
// No automatic retry occurs. A commit error is Unknown: policies may have changed,
// even though this control-plane operation never debits quota. Retry the identical
// revisions explicitly to resolve uncertainty (a newer update may supersede them).
func (d *Driver) UpdateQuotas(ctx context.Context, u pacelock.QuotaBatch) error {
	uncharged := func(err error) error { return fail("update_quotas", pacelock.NotCharged, err) }
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

	ids := make([]string, len(u.Quotas))
	for i, q := range u.Quotas {
		ids[i] = itemID(q.Name)
	}
	states, err := loadBatch(ctx, tx, scope, ids, true)
	if err != nil {
		return uncharged(err)
	}

	next, err := pacelock.ApplyQuotas(now, u.Quotas, states)
	if err != nil {
		return uncharged(err)
	}

	var changedIDs []string
	var changed []pacelock.QuotaState
	for i, s := range next {
		if s != states[i] {
			changedIDs = append(changedIDs, ids[i])
			changed = append(changed, s)
		}
	}
	// An identical retry is read-only, so rollback avoids commit ambiguity.
	if len(changed) == 0 {
		return nil
	}
	if err = storeBatch(ctx, tx, scope, changedIDs, changed, true); err != nil {
		return uncharged(err)
	}

	if err = tx.Commit(); err != nil {
		return fail("update_quotas", pacelock.Unknown, err)
	}
	return nil
}

// Prune removes up to limit expired debt rows
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
	// Require recovered TAT as well as expiry: a premature expiry must
	// never cause cleanup to erase active debt.
	result, err := tx.ExecContext(ctx, `DELETE FROM pacelock_quotas WHERE scope_id=$1 AND quota_id IN (SELECT quota_id FROM pacelock_quotas WHERE scope_id=$1 AND expires_us<=$2 AND tat_us<=$2 ORDER BY quota_id LIMIT $3)`, id, now, limit)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}
