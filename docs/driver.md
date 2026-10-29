# Atomic admission drivers

`pacelock.New(driver, namespace, domain)` constructs the default dynamic `*Limiter`.
Both Redis and PostgreSQL implement the same admission contract.
See the [current API and storage contract](compatibility.md).

## Ownership and persistence boundary

The core owns validation, interval rounding, public operation names, observation,
and defensive result validation. It imports only the standard library. Applications
own their clients, deadlines and execution state. Drivers own authoritative backend
time, synchronization, persistence, logical expiry and error classification.

Mandatory `Driver.Admit(ctx, Admission) (Decision, error)` receives only a scope
and ordered name/cost charges, resolving current backend policies atomically.
Optional `QuotaDriver.UpdateQuotas`
publishes revisioned permanent configurations. Missing optional capabilities return
`ErrUnsupported` / `NotCharged`. Constructors perform no backend I/O.

The whole operation is the atomic boundary: all selected quotas pass together or
an ordinary denial debits none. There are no retained results or recovery methods.
`Limiter.Allow` / `Wait` accept public `Charge{Name, Cost}` values; omitted quotas
are neither checked nor charged. Include every applicable quota. Limited names
follow charge order. Empty calls, duplicate names and nonpositive costs fail
locally. Unknown names fail closed with `ErrPolicyMissing`; costs above the
current backend burst return `ErrInvalid`. Separate calls are not atomic together.

Drivers must be concurrency-safe, must not retain/mutate request slices, and must
not retry an ambiguous mutation. Validate direct requests too. Return `*Error` with
`NotCharged` only for an invocation known not to debit. Core wraps untyped driver
errors conservatively as `ErrBackend` / `Unknown`; errors never expose a usable
decision. `NotCharged` on a later call cannot resolve an earlier unknown outcome.

## Blocking wrapper

`Limiter.Wait(ctx, charges...) error` copies charges once before dispatch, then
makes ordinary `Admit` calls: each attempt resolves the latest backend policies
atomically, without a policy snapshot. It never recovers an uncertain mutation.

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
can delay use of a relaxed policy. One `wait`
observation covers the complete call, with no per-attempt observations.

## GCRA and policy identity

Use `Rule.Interval()` for conservative microsecond rounding. Effective interval
and Burst describe the effective policy; Cost varies per attempt. `Evaluate` provides
an offline math transition only. `EvaluateManaged` remains a pure
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

There is no upgrade, migration or historical storage compatibility guarantee.

## Backend modules

- `driver/redis`: go-redis transport, single-command MSET commit, logical expiry,
  bounded cursor cleanup and guarded command replay/Cluster redirects. Keys use
  `pacelock:{scope_hash}:`; only exact `D1|tat` debt and
  `P1|interval:burst:revision` policies are accepted. Unknown, malformed or wrong-role
  stored formats fail closed before mutation. Debt expires at `tat <= now`.
  Marker `1` is independent of the positive monotonic policy revision. Prune deletes
  only valid recovered D1 debt, never policy or unknown keys.
- `driver/postgres`: database/sql primary handle, explicit `Migrate`, one locked
  domain row and short Read Committed transactions. `Prune` is optional bounded
  maintenance; no worker starts automatically. Run `Migrate` during setup before
  serving traffic. Only the current schema is supported. See its deployment guide.

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
