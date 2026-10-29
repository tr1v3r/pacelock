# Atomic admission drivers

`pacelock.New(driver, namespace, domain)` constructs the default dynamic `*Limiter`.
`NewStatic(driver, namespace, domain, quotas...)` independently constructs a
`*StaticLimiter`; `NewStaticWithOptions` adds `Options` before the quota arguments.
Static constructors validate and copy fixed Revision-0 quotas without backend I/O
or a parent limiter. Later input mutation does not change the fixed policies.
Both Redis and PostgreSQL implement the same admission contract.
Dynamic admission is the default; see the breaking pre-v1 [migration](compatibility.md).

## Ownership and persistence boundary

The core owns validation, interval rounding, public operation names, observation,
and defensive result validation. It imports only the standard library. Applications
own their clients, deadlines and execution state. Drivers own authoritative backend
time, synchronization, persistence, logical expiry and error classification.

Mandatory `Driver.Admit(ctx, Admission) (Decision, error)` receives only a scope
and ordered name/cost charges, resolving current backend policies atomically.
Optional `StaticDriver.AdmitStatic(ctx, StaticAdmission) (Decision, error)` receives
only a scope and caller-owned Revision-0 rules. Optional `QuotaDriver.UpdateQuotas`
publishes revisioned permanent configurations. Missing optional capabilities return
`ErrUnsupported` / `NotCharged`. Constructors perform no backend I/O.

The whole operation is the atomic boundary: all selected quotas pass together or
an ordinary denial debits none. There are no retained results or recovery methods.
`StaticLimiter.Allow` / `Wait` accept public `Charge{Name, Cost}` values selecting
any nonempty subset of fixed quotas. The core validates charges and sends only
selected rules in binding order; omitted quotas are neither checked nor charged.
Empty calls, duplicate or unknown names and invalid costs fail locally with
`ErrInvalid` / `NotCharged`. Separate calls are not atomic together.

Drivers must be concurrency-safe, must not retain/mutate request slices, and must
not retry an ambiguous mutation. Validate direct requests too. Return `*Error` with
`NotCharged` only for an invocation known not to debit. Core wraps untyped driver
errors conservatively as `ErrBackend` / `Unknown`; errors never expose a usable
decision. `NotCharged` on a later call cannot resolve an earlier unknown outcome.

## Blocking wrapper

Both `Limiter.Wait(ctx, charges...) error` and `StaticLimiter.Wait(ctx, charges...) error`
use a shared private core waiting loop. Dynamic `Wait` copies the caller's charge
slice once before dispatch, then makes ordinary `Admit` calls: each attempt resolves
the latest backend policy atomically, without taking a policy snapshot. Static
`Wait` uses the fixed policies and `AdmitStatic`. Neither path attempts recovery after an uncertain mutation.

Drivers remain immediate: only a valid ordinary capacity denial is retried, after
its validated advisory `RetryAfter`. Every error stops the loop, including
`ErrPaused`, `ErrPolicyMissing`, `ErrInvalid` (such as a newly reduced Burst below
Cost), transport errors even when `NotCharged`, `Unknown`, unsupported capabilities
and invalid driver results. Cancellation between attempts is `NotCharged`;
cancellation in flight preserves that invocation's accounting certainty. A confirmed
successful debit remains success even if cancellation races with the response.

No driver API, persistence protocol, queue, worker, background goroutine, fairness
or refund is added. Sleeping holds no driver resources; the caller owns the context
deadline. Policy updates do not proactively wake sleepers, so a previous RetryAfter
can delay use of a relaxed policy. One `wait` (dynamic) or `wait_static` (static)
observation covers the complete call, with no per-attempt observations.

## GCRA and policy identity

Use `Rule.Interval()` for conservative microsecond rounding. Effective interval
and Burst identify an active policy; Cost varies per attempt. `Evaluate` provides
a pure Go static transition used by PostgreSQL. `EvaluateManaged` remains a pure
dynamic transition; no helper polls or publishes policies. Redis executes the same transition in
Lua to keep computation and persistence together. `QuotaState` holds integer Unix
microseconds; expiry is a logical deadline, not a requirement to run a timer.
Backend clocks are wall clocks, not monotonic clocks; jumps/failover need operational
validation. Keep periods and burst recovery within 24h and each request within 32 rules.

## Failure boundary

Redis validates and encodes before one final MSET of admitted quotas. PostgreSQL
commits quota writes in one transaction; confirmed rollback means `NotCharged`.
A PostgreSQL Commit error is `Unknown`: the database may have committed before the
reply was lost. Never retry an uncertain admission automatically. `Unknown` cannot
be resolved through this SDK; callers must apply an explicit failure policy.

The pre-v1 removal deletes `AllowOnce`, `Lookup`, receipt types, admission identity
fields/fingerprints and replay events. Legacy receipt storage is not automatically
dropped. See [migration and cleanup](compatibility.md).

## Backend modules

- `driver/redis`: go-redis transport, single-command MSET commit, logical expiry,
  bounded cursor cleanup and guarded command replay/Cluster redirects. Keys use
  `pacelock:{scope_hash}:`; quotas use `tat:interval:burst`, with active-policy
  checks and expiry at `tat <= now`. See its guide.
- `driver/postgres`: database/sql primary handle, explicit `Migrate`, one locked
  domain row and short Read Committed transactions. `Prune` is optional bounded
  maintenance; no worker starts automatically. Run `Migrate` before upgraded binaries
  serve any traffic, including static traffic. See its deployment guide.

Before activating dynamic policies, upgrade every writer and maintenance process.
Updates preserve absolute TAT without rescaling or reset; `Rate=0` pauses and a
higher revision resumes. Dynamic Wait rechecks policy on each attempt, but does not
wait through policy errors or perform automatic configuration-source polling.

Each module has its own go.mod. Backend tests live with their driver; shared real
backend contracts live in `tests/contracts` to avoid a core-to-driver import cycle.
Local replace directives support this checkout; published consumers resolve the
pinned core dependency. Run `sh scripts/check.sh` from the root to check all modules.

Do not run Redis and PostgreSQL as interchangeable failover stores for one live
quota: they have separate debt and clocks. There is no automatic state
migration, cross-backend transaction, or API to join a caller's business transaction.
Selecting such APIs or migration protocols is future work, not an implemented promise.
