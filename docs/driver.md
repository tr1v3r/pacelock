# Atomic admission drivers

`pacelock.New(driver, namespace, domain)` is the backend-independent constructor.
It replaces the pre-v1 Redis-client constructor. `Allow`, `AllowOnce`, `Lookup`,
`Rule`, decisions, errors and observers retain their business semantics.

## Ownership and persistence boundary

The core owns validation, interval rounding, public operation names, observation,
and defensive result validation. It imports only the standard library. Applications
own their clients, deadlines and execution state. Drivers own authoritative backend
time, synchronization, persistence, logical expiry and error classification.

`Driver.Admit` receives a scope, ordered rules and an optional ID/retention. The
whole operation is the atomic boundary: all quotas pass together or an ordinary
denial debits none. `Driver.Lookup` recovers a retained result on the authoritative
primary without refreshing retention. The interface does not expose separate
Get/Set calls or a network-spanning read/modify/write callback.

Drivers must be concurrency-safe, must not retain/mutate request slices, and must
not retry an ambiguous mutation. Validate direct requests too. Return `*Error` with
`NotCharged` only for an invocation known not to debit. Core wraps untyped driver
errors conservatively as `ErrBackend` / `Unknown`; errors never expose a usable
decision. `NotCharged` on a later call cannot resolve an earlier unknown outcome.

## Blocking wrapper

`Limiter.Wait` is implemented entirely in the core using ordinary `Admit` calls
with an empty ID. Drivers remain immediate: the core sleeps for a validated
`RetryAfter` only after a normal denial. Any error stops the loop and preserves
its accounting certainty. No driver API, persistence protocol or backend-side
queue is added. Sleeping holds no driver resources; the caller owns the context
deadline. One `wait` observation covers the complete operation.

## GCRA and policy identity

Use `Rule.Interval()` for conservative microsecond rounding. Effective interval
and Burst identify an active policy; Cost varies per attempt. `Evaluate` provides
a pure Go transition used by PostgreSQL. Redis executes the same transition in
Lua to keep computation and persistence together. `QuotaState` holds integer Unix
microseconds; expiry is a logical deadline, not a requirement to run a timer.
Backend clocks are wall clocks, not monotonic clocks; jumps/failover need operational
validation. Keep periods and burst recovery within 24h and each request within 32 rules.

## Retained results

ID scope is namespace/domain. `Admission.Fingerprint()` binds ordered names,
effective intervals, bursts, costs and retention with the Redis v1 encoding.
Names must be UTF-8 for idempotent requests; all other identity strings may be
opaque bytes. Both allow and deny are retained; replay/lookup does not extend
retention. A changed fingerprint returns `ErrConflict`. A retained denial stays
denied; its RetryAfter is historical. A genuinely new business attempt needs a new
ID. Expired/lost IDs must not be reused to depend on deduplication.

Redis validates and encodes before one final MSET of quotas and the complete
receipt. It uses logical expiry and application-scheduled bounded physical cleanup.
It never creates new Pending, but preserves legacy Pending uncertainty.
PostgreSQL commits quota and receipt writes in one transaction,
so pre-commit failure rolls them all back. A PostgreSQL Commit error is still
`Unknown`: the database may have committed before the reply was lost. Missing
lookup results do not prove no debit or upstream exactly-once execution.

## Backend modules and migration

- `driver/redis`: go-redis transport, single-command MSET commit, v2 logical
  expiry, bounded cursor cleanup and guarded command replay/Cluster redirects.
  Keys stay unchanged; v1 values remain readable. Quiesce old writers before
  upgrading; mixed writers/unconverted rollback are unsupported. See its guide.
- `driver/postgres`: database/sql primary handle, explicit `Migrate`, one locked
  domain row and short Read Committed transactions. `Prune` is optional bounded
  maintenance; no worker starts automatically. See its deployment guide.

Each module has its own go.mod. Backend tests live with their driver; shared real
backend contracts live in `tests/contracts` to avoid a core-to-driver import cycle.
Local replace directives support this checkout; published consumers resolve the
pinned core dependency. Run `sh scripts/check.sh` from the root to check all modules.

Do not run Redis and PostgreSQL as interchangeable failover stores for one live
quota: they have separate debt, receipts and clocks. There is no automatic state
migration, cross-backend transaction, or API to join a caller's business transaction.
Selecting such APIs or migration protocols is future work, not an implemented promise.
