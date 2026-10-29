// Package redisdriver implements Pacelock atomic admission using Redis Lua.
// It commits quotas in one MSET, with logical expiry and caller-scheduled
// bounded cleanup. Policies and debt use only P1 and D1 respectively;
// unsupported formats fail closed. One domain occupies one Redis Cluster
// hash slot; transport commands prevent ambiguous replay.
package redisdriver

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "embed"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"strconv"
)

// Driver is concurrency-safe when its caller-owned Scripter is concurrency-safe.
// It does not own or close the client. Concrete go-redis clients receive command
// replay protection while retaining bounded MOVED/ASK redirects. Custom clients,
// hooks, factories and proxies must preserve this contract. Ring is unsupported.
type Driver struct{ client redis.Scripter }

var _ pacelock.Driver = (*Driver)(nil)
var _ pacelock.QuotaDriver = (*Driver)(nil)

// New validates an initialized go-redis client without connecting or changing its
// options. Configure the client before use. The constructor accepts Client,
// ClusterClient and custom Scripters with an audited no-replay policy.
func New(client redis.Scripter) (*Driver, error) {
	if _, err := newLimiter(client, "validate", "validate"); err != nil {
		return nil, err
	}
	return &Driver{client: client}, nil
}

// Admit reads authoritative policies and atomically admits named costs.
func (d *Driver) Admit(ctx context.Context, request pacelock.Admission) (pacelock.Decision, error) {
	op := "allow"
	if err := request.Validate(); err != nil {
		return pacelock.Decision{}, inputError(op, err)
	}
	if d == nil {
		return pacelock.Decision{}, inputError(op, pacelock.ErrInvalid)
	}
	l, err := newLimiter(d.client, request.Namespace, request.Domain)
	if err != nil {
		return pacelock.Decision{}, inputError(op, err)
	}
	return l.Admit(ctx, request)
}

// UpdateQuotas atomically changes managed policies while preserving all debt.
// Unknown failures may have committed; explicitly retrying the same revision and
// effective policy is idempotent. The transport never retries ambiguous commands.
func (d *Driver) UpdateQuotas(ctx context.Context, request pacelock.QuotaUpdate) error {
	if err := request.Validate(); err != nil {
		return inputError("update_quotas", err)
	}
	if d == nil {
		return inputError("update_quotas", pacelock.ErrInvalid)
	}
	l, err := newLimiter(d.client, request.Namespace, request.Domain)
	if err != nil {
		return inputError("update_quotas", err)
	}
	return l.UpdateQuotas(ctx, request.Quotas...)
}

// Prune checks at most limit candidate values and performs at most one SCAN page
// on the scope's current primary. Limit must be 1..MaxPruneKeys. SCAN COUNT is a
// hint, so discovery has no hard latency bound and overflow is buffered in Cursor.
// Logically expired values are deleted only after an atomic recheck. Malformed
// candidates abort the batch; active debt and legacy records are preserved. The
// application schedules repeated cycles; constructors and admissions never start
// cleanup goroutines.
func (d *Driver) Prune(ctx context.Context, scope pacelock.Scope, cursor PruneCursor, limit int) (PruneResult, error) {
	if err := scope.Validate(); err != nil {
		return PruneResult{}, inputError("prune", err)
	}
	if d == nil {
		return PruneResult{}, inputError("prune", pacelock.ErrInvalid)
	}
	l, err := newLimiter(d.client, scope.Namespace, scope.Domain)
	if err != nil {
		return PruneResult{}, inputError("prune", err)
	}
	return l.Prune(ctx, cursor, limit)
}

func inputError(op string, err error) error {
	return &pacelock.Error{Op: op, Outcome: pacelock.NotCharged, Err: err}
}

// Shared contracts come from the backend-independent core.
const MaxRules = pacelock.MaxRules
const MaxHorizon = pacelock.MaxHorizon

var (
	ErrInvalid        = pacelock.ErrInvalid
	ErrPolicyMismatch = pacelock.ErrPolicyMismatch
	ErrState          = pacelock.ErrState
	ErrBackend        = pacelock.ErrBackend
)

type Rule = pacelock.Rule
type Decision = pacelock.Decision

type Outcome = pacelock.Outcome

const NotCharged = pacelock.NotCharged
const Unknown = pacelock.Unknown

type Error = pacelock.Error

// failure attaches accounting metadata without discarding error identity.
func failure(op string, outcome Outcome, err error) error {
	return &Error{Op: op, Outcome: outcome, Err: err}
}

// scriptFailure recognizes SDK pre-write errors and conservatively classifies all
// other failures.
func scriptFailure(op string, err error) error {
	// Only exact SDK-owned pre-write error tokens are known not to debit.
	var server redis.Error
	token := ""
	if errors.As(err, &server) {
		token, _, _ = strings.Cut(strings.TrimPrefix(server.Error(), "ERR "), " ")
	}
	switch token {
	case "PACE_COMMIT":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrBackend, err))
	case "PACE_MISSING":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", pacelock.ErrPolicyMissing, err))
	case "PACE_INVALID":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrInvalid, err))
	case "PACE_PAUSED":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", pacelock.ErrPaused, err))
	case "PACE_POLICY":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrPolicyMismatch, err))
	case "PACE_STATE":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrState, err))
	default:
		return failure(op, Unknown, fmt.Errorf("%w: %w", ErrBackend, err))
	}
}

// limiter is safe for concurrent use when its Redis client is safe for concurrent
// use. It neither owns nor closes the client. One domain is one Redis Cluster
// hash slot. Nil/zero receivers and nil operation contexts return ErrInvalid
// wrapped in Error before dispatch.
type limiter struct {
	// client is the caller-owned transport, configured to avoid command replay.
	client redis.Scripter
	// prefix hashes namespace and domain into a shared Redis Cluster hash tag.
	prefix string
}

// newLimiter constructs a limiter. Namespace isolates applications; domain
// selects the set of quotas that can be checked together. Neither may be empty
// or exceed 256 bytes. Concrete go-redis clients support default, positive and
// disabled MaxRetries budgets. SDK script commands carry NoRetry to prevent
// ambiguous replay while preserving bounded MOVED/ASK recovery. Other commands
// retain the client's retry policy. Unsupported negative budgets return
// ErrInvalid. go-redis clients must be initialized with their constructors;
// their zero values are unsupported. Custom Scripters, hooks and node factories
// must preserve the no-replay contract. redis.Ring is unsupported.
// newLimiter does not connect to Redis or change the supplied client's configuration.
func newLimiter(client redis.Scripter, namespace, domain string) (*limiter, error) {
	if nilClient(client) || namespace == "" || domain == "" || len(namespace) > 256 || len(domain) > 256 {
		return nil, fmt.Errorf("%w: client, namespace and domain are required (names <= 256 bytes)", ErrInvalid)
	}
	if err := validateTransport(client); err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(namespace), namespace, domain)))
	return &limiter{client: safeScripter(client), prefix: fmt.Sprintf("pacelock:{%x}:", digest)}, nil
}

// nilClient rejects nil interfaces and nil concrete clients without invoking them.
func nilClient(client redis.Scripter) bool {
	if client == nil {
		return true
	}
	switch c := client.(type) {
	case *redis.Client:
		return c == nil
	case *redis.ClusterClient:
		return c == nil
	}
	return false
}

// protocolSource contains shared quota and policy readers.
//
//go:embed protocol.lua
var protocolSource string

// admissionSource computes the decision before its only mutation, MSET.
//
//go:embed gcra.lua
var admissionSource string

// source is the complete script, also used by model and failure-injection tests.
var source = protocolSource + "\n" + admissionSource

// admissionScript caches the script hash and falls back to EVAL on NOSCRIPT.
var admissionScript = redis.NewScript(source)

// key hashes a quota name while preserving the domain hash slot.
func (l *limiter) key(name string) string {
	return fmt.Sprintf("%s%x", l.prefix, sha256.Sum256([]byte(name)))
}

func (l *limiter) policyKey(name string) string {
	return fmt.Sprintf("%spolicy:%x", l.prefix, sha256.Sum256([]byte(name)))
}

// ready rejects unusable receivers and contexts before any backend request.
func (l *limiter) ready(ctx context.Context) error {
	if ctx == nil || l == nil || l.prefix == "" || nilClient(l.client) {
		return fmt.Errorf("%w: initialized limiter and non-nil context required", ErrInvalid)
	}
	return ctx.Err()
}

// Admit resolves current backend policy in the same atomic admission.
func (l *limiter) Admit(ctx context.Context, request pacelock.Admission) (decision Decision, err error) {
	op := "allow"
	if err := l.ready(ctx); err != nil {
		return Decision{}, failure(op, NotCharged, err)
	}
	if err := request.Validate(); err != nil {
		return Decision{}, failure(op, NotCharged, err)
	}

	n := len(request.Charges)
	keys := make([]string, 2*n)
	names := make([]string, n)
	args := make([]interface{}, 0, n)
	for i, c := range request.Charges {
		keys[i] = l.key(c.Name)
		keys[n+i] = l.policyKey(c.Name)
		names[i] = c.Name
		args = append(args, c.Cost)
	}

	cmd := admissionScript.Run(ctx, l.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return Decision{}, scriptFailure(op, err)
	}
	values, err := cmd.Int64Slice()
	if err != nil {
		return Decision{}, failure(op, Unknown, fmt.Errorf("%w: invalid decision reply", ErrState))
	}
	d, err := decodeDecision(values, names)
	if err != nil {
		return Decision{}, failure(op, Unknown, err)
	}
	return d, nil
}

// decodeDecision converts microsecond waits to durations and checks the script
// result against invariants the caller already validated.
func decodeDecision(values []int64, names []string) (Decision, error) {
	if len(values) != len(names)+2 || (values[0] != 0 && values[0] != 1) {
		return Decision{}, fmt.Errorf("%w: unexpected script response", ErrState)
	}
	d := Decision{Allowed: values[0] == 1, RetryAfter: time.Duration(values[1]) * time.Microsecond}
	for i := range names {
		if values[i+2] != 0 && values[i+2] != 1 {
			return Decision{}, fmt.Errorf("%w: invalid quota flag in script response", ErrState)
		}
		if values[i+2] == 1 {
			d.Limited = append(d.Limited, names[i])
		}
	}

	if (d.Allowed && (d.RetryAfter != 0 || len(d.Limited) != 0)) || (!d.Allowed && (d.RetryAfter == 0 || len(d.Limited) == 0)) {
		return Decision{}, fmt.Errorf("%w: inconsistent script response", ErrState)
	}
	return d, nil
}

//go:embed update.lua
var updateSource string

var updateScript = redis.NewScript(protocolSource + updateSource)

// UpdateQuotas atomically installs managed policies without resetting debt.
// Transport ambiguity is Unknown; no implicit retries are performed.
func (l *limiter) UpdateQuotas(ctx context.Context, quotas ...pacelock.Quota) error {
	const op = "update_quotas"
	if err := l.ready(ctx); err != nil {
		return failure(op, NotCharged, err)
	}
	// Scope was already validated by construction; validate the quota contract here.
	request := pacelock.QuotaUpdate{Scope: pacelock.Scope{Namespace: "validated", Domain: "validated"}, Quotas: quotas}
	if err := request.Validate(); err != nil {
		return failure(op, NotCharged, err)
	}
	keys := make([]string, len(quotas))
	args := make([]interface{}, 0, 3*len(quotas))
	for i, q := range quotas {
		step, _ := (Rule{Name: q.Name, Policy: q.Policy, Cost: 1}).Interval()
		keys[i] = l.policyKey(q.Name)
		args = append(args, step.Microseconds(), q.Policy.Burst, q.Policy.Revision)
	}
	cmd := updateScript.Run(ctx, l.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return scriptFailure(op, err)
	}
	reply, err := cmd.Int64()
	if err != nil || reply != 1 {
		return failure(op, Unknown, fmt.Errorf("%w: invalid policy update reply", ErrState))
	}
	return nil
}

// MaxPruneKeys is the maximum number of candidate values read by one cleanup call.
const MaxPruneKeys = 64

// PruneCursor is an opaque, caller-owned scan position. Start with its zero value.
// Keep returned values between calls; cursors are scope-bound and not serializable.
// Restart from zero after topology changes or process restarts. Concurrent changes
// make discovery best effort, so applications must schedule repeated full cycles.
type PruneCursor struct {
	// scope prevents using a cursor with a different namespace/domain.
	scope string
	// position is the SCAN position on the domain's current primary.
	position string
	// pending buffers overflow because SCAN COUNT is not a hard bound.
	pending []string
	// done distinguishes a completed cycle from the initial zero cursor.
	done bool
}

// Done reports completion of the scan cycle and any buffered candidates.
func (c PruneCursor) Done() bool { return c.done }

// PruneResult describes one cleanup step. Ignore it on error.
type PruneResult struct {
	// Cursor is the next position; repeat until Cursor.Done().
	Cursor PruneCursor
	// Examined is the number of candidate values checked, at most the requested limit.
	Examined int
	// Deleted is the number of logically expired values physically removed.
	Deleted int64
}

// scanScript routes discovery to the domain's primary using a same-slot anchor.
// It does not access values or derive keys for a mutation inside the script.
var scanScript = redis.NewScript(`return redis.call('SCAN', ARGV[1], 'MATCH', KEYS[1] .. '*', 'COUNT', ARGV[2])`)

// pruneSource rechecks candidate expiry before its only deletion command.
//
//go:embed prune.lua
var pruneSource string

// pruneScript shares the admission protocol's clock and expiry readers.
var pruneScript = redis.NewScript(protocolSource + pruneSource)

// Prune performs at most one SCAN page and checks at most limit candidate values,
// with 1 <= limit <= MaxPruneKeys. SCAN COUNT is only a discovery-work hint;
// overflow is buffered in the cursor. This is not a hard Redis latency bound.
// Active debt, permanent policy fences, and keys outside the owned key grammar
// are preserved. Malformed candidates abort the batch without deleting any keys.
// The caller owns scheduling; no worker is started.
func (l *limiter) Prune(ctx context.Context, cursor PruneCursor, limit int) (PruneResult, error) {
	if err := l.ready(ctx); err != nil {
		return PruneResult{}, failure("prune", NotCharged, err)
	}
	if limit < 1 || limit > MaxPruneKeys || (cursor.scope != "" && cursor.scope != l.prefix) {
		return PruneResult{}, failure("prune", NotCharged, fmt.Errorf("%w: cleanup limit or cursor scope", ErrInvalid))
	}
	if cursor.done {
		return PruneResult{Cursor: cursor}, nil
	}

	next := cursor
	next.scope = l.prefix
	// Discovery: at most one SCAN page, buffering overflow into pending.
	if len(next.pending) == 0 {
		position := next.position
		if position == "" {
			position = "0"
		}
		cmd := scanScript.Run(ctx, l.client, []string{l.prefix}, position, limit)
		if err := cmd.Err(); err != nil {
			return PruneResult{}, scriptFailure("prune", err)
		}
		values, err := cmd.Slice()
		if err != nil || len(values) != 2 {
			return PruneResult{}, failure("prune", Unknown, ErrState)
		}
		position, ok := values[0].(string)
		if _, err := strconv.ParseUint(position, 10, 64); !ok || err != nil {
			return PruneResult{}, failure("prune", Unknown, ErrState)
		}
		keys, ok := values[1].([]interface{})
		if !ok {
			return PruneResult{}, failure("prune", Unknown, ErrState)
		}
		next.position = position
		next.pending = nil
		for _, value := range keys {
			key, ok := value.(string)
			if !ok {
				return PruneResult{}, failure("prune", Unknown, ErrState)
			}
			if l.ownedKey(key) {
				next.pending = append(next.pending, key)
			}
		}
	}

	// Consumption: take at most limit buffered candidates for this step.
	n := len(next.pending)
	if n > limit {
		n = limit
	}
	candidates := next.pending[:n]
	next.pending = next.pending[n:]
	next.done = next.position == "0" && len(next.pending) == 0
	result := PruneResult{Cursor: next, Examined: n}
	if n == 0 {
		return result, nil
	}

	cmd := pruneScript.Run(ctx, l.client, candidates)
	if err := cmd.Err(); err != nil {
		return PruneResult{}, scriptFailure("prune", err)
	}
	deleted, err := cmd.Int64()
	if err != nil || deleted < 0 || deleted > int64(n) {
		return PruneResult{}, failure("prune", Unknown, ErrState)
	}
	result.Deleted = deleted
	return result, nil
}

// ownedKey recognizes only SDK-generated quota and policy keys in this scope.
func (l *limiter) ownedKey(key string) bool {
	suffix, ok := strings.CutPrefix(key, l.prefix)
	if !ok {
		return false
	}
	suffix = strings.TrimPrefix(suffix, "policy:")
	if len(suffix) != 64 {
		return false
	}
	for _, c := range suffix {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// validateTransport checks normalized options, not the values originally passed
// to go-redis constructors. It must not connect, instantiate Cluster nodes, or
// change caller-owned options. Wrappers, hooks and custom node factories remain
// the caller's responsibility: redis.Scripter cannot expose their replay policy.
func validateTransport(client redis.Scripter) error {
	switch c := client.(type) {
	case *redis.Ring:
		return fmt.Errorf("%w: redis.Ring is unsupported; use redis.Client or redis.ClusterClient", ErrInvalid)
	case *redis.Client:
		// NoRetry protects script commands independently of the client retry budget.
		// Negative normalized values are invalid (input -1 normalizes to zero).
		if c.Options() == nil || c.Options().MaxRetries < 0 {
			return fmt.Errorf("%w: redis.Client requires a nonnegative normalized retry budget", ErrInvalid)
		}
	case *redis.ClusterClient:
		// NoRetry suppresses ambiguous replay, while the redirect budget permits
		// safe MOVED/ASK recovery. Only reject unsupported negative budgets.
		options := c.Options()
		if options == nil || options.MaxRedirects < 0 || options.MaxRetries < -1 {
			return fmt.Errorf("%w: redis.ClusterClient requires MaxRetries >= -1 and a nonnegative normalized redirect budget", ErrInvalid)
		}
	}
	return nil
}

// noRetryScripter adapts Pacelock's internal EVAL/EVALSHA calls for the supported
// go-redis clients. It retains Script.Run cache-miss handling and safe MOVED/ASK
// redirects while preventing ambiguous replay, including within ASK pipelines.
type noRetryScripter struct {
	redis.Scripter
	process func(context.Context, redis.Cmder) error
}

// noRetryCmd changes only the command replay policy. Routing and execution
// remain owned by go-redis.
type noRetryCmd struct{ *redis.Cmd }

func (noRetryCmd) NoRetry() bool { return true }

// safeScripter deliberately recognizes concrete clients: a custom transport
// exposing Process is not necessarily guaranteed to honor NoRetry.
func safeScripter(c redis.Scripter) redis.Scripter {
	switch c := c.(type) {
	case *redis.Client:
		return noRetryScripter{c, c.Process}
	case *redis.ClusterClient:
		return noRetryScripter{c, c.Process}
	default:
		return c // Custom transports own their replay policy.
	}
}
func (s noRetryScripter) send(ctx context.Context, command, script string, keys []string, args ...interface{}) *redis.Cmd {
	values := make([]interface{}, 0, 3+len(keys)+len(args))
	values = append(values, command, script, len(keys))
	for _, key := range keys {
		values = append(values, key)
	}
	values = append(values, args...)
	cmd := redis.NewCmd(ctx, values...)
	cmd.SetFirstKeyPos(3)
	err := s.process(ctx, noRetryCmd{cmd})
	cmd.SetErr(err)
	return cmd
}
func (s noRetryScripter) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.send(ctx, "eval", script, keys, args...)
}
func (s noRetryScripter) EvalSha(ctx context.Context, hash string, keys []string, args ...interface{}) *redis.Cmd {
	return s.send(ctx, "evalsha", hash, keys, args...)
}
