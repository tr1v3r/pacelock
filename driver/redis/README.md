# Redis driver

[简体中文](README.zh-CN.md)

Independent module: `github.com/tr1v3r/pacelock/driver/redis`. Requires Redis 7+.
Applications own the initialized go-redis Client/ClusterClient and close it.

```go
driver, err := redisdriver.New(client)
if err != nil { return err }
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
if err != nil { return err }
if err := limiter.UpdateQuotas(ctx, pacelock.Quota{Name: "rpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 60, Period: time.Minute, Burst: 10,
}}); err != nil { return err }
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: "rpm", Cost: 1})
// On error ignore decision and inspect pacelock.OutcomeOf(err).
```

Import `redisdriver "github.com/tr1v3r/pacelock/driver/redis"` and
`"github.com/tr1v3r/pacelock"`. Construction performs no I/O or option mutation.
One domain is one Cluster slot. Every caller must use matching scope and names, and include every applicable
quota in an admission. Backend configuration owns the current dynamic policies.

## Single-command admission

Lua reads all requested quotas and policies, calculates GCRA, and encodes all
values and the response before its only mutation: one `MSET`. An allowance commits
at most 32 small quota values; a denial writes nothing. It does not load or rewrite
an aggregate domain object.

A rejected MSET command (for example, a script-visible ACL/OOM rejection) is
`NotCharged`. Errors without that affirmative server response, including lost
responses, remain `Unknown`. There is no retained admission result or recovery
query. Never blindly retry an uncertain admission or refund its potential debit.
Redis crash/failover durability and upstream execution deduplication remain outside
the limiter's contract.

Publish positive-revision policies with `UpdateQuotas`, then call
`Allow(ctx, charges...)` or `Wait(ctx, charges...)`; no local binding or policy
refresh is required. This is the only runtime admission path. The driver implements
`Admit(ctx, Admission) (Decision, error)`; requests contain names and costs only.
`Rule`, `Validate` and `Evaluate` are pure math helpers, not a runtime policy channel.
Each call checks only its supplied unique quota names; include every applicable quota.
Empty calls, duplicate names and invalid costs fail locally. Unknown backend names
return `ErrPolicyMissing`. Separate calls are not atomic together.

Concrete clients receive per-command NoRetry while retaining bounded MOVED/ASK
redirects. Ring is unsupported. Custom Scripters/hooks/factories/proxies must
preserve no-replay and primary-routing behavior. All admissions pass explicit debt
and policy keys. During Cluster resharding, mixed/missing
keys can yield `TRYAGAIN`; this remains a conservative `Unknown` transport error,
never an implicit replay. Retry/recovery decisions belong to the caller.

## Backend-managed dynamic policies

All admission is backend-managed. Publish positive, monotonically
increasing revisions and `limiter.UpdateQuotas(ctx, quotas...)` (or the driver's
`UpdateQuotas(ctx, pacelock.QuotaUpdate{Scope: scope, Quotas: quotas})`). Each batch
is atomic with admission, using one final MSET and the same NoRetry transport.
Quota configuration changes do not refill capacity or reset/rescale debt.
Updates preserve absolute TAT exactly, including debt above a reduced capacity.
Same revision plus the same effective interval/burst is idempotent; lower revisions
or same-revision conflicts return `ErrPolicyMismatch`. Roll back parameters only at
a higher revision. `NotCharged` describes quota debit, not an assertion that an
ambiguous update failed: transport/lost-reply failures remain `Unknown` and need
an explicit same-revision retry if desired.

The data plane passes names and costs only: `limiter.Allow(ctx,
pacelock.Charge{Name: "rpm", Cost: 1})`. The driver's mandatory `Admit(Admission)` operation resolves
current backend policies in the admission Lua call; callers do not refresh bindings
or send revisions. Missing configuration returns `ErrPolicyMissing` / `NotCharged`.
There is no caller-policy override, including after idle periods or pruning.

Managed `Rate=0` returns `ErrPaused` / `NotCharged` without debit,
even when cost exceeds the paused burst. Otherwise over-burst cost returns `ErrInvalid`.
Resume with a higher revision; debt recovers naturally.

`Limiter.Wait(ctx, charges...)` copies charges once before dispatch and resolves the
latest backend policies atomically on each ordinary attempt, without a policy
snapshot. It retries only valid capacity denials after advisory `RetryAfter`; every
error stops, including pause, missing policy, a new Burst below Cost, transport
errors even when `NotCharged`, `Unknown`, unsupported capabilities and invalid
driver results. Cancellation between attempts is `NotCharged`; in-flight uncertainty
is preserved, and confirmed success stays success despite cancellation. One
`wait` observation covers the whole call. Updates
do not proactively wake sleepers, so an old RetryAfter may delay using relaxed policy.
No queue, worker, background goroutine, fairness or refund is added. Use a context
deadline; see [blocking admission](../../README.md#blocking-admission).

Configuration and debt are separate. The same-slot `policy:<name_hash>` key holds
permanent `P1|interval_us:burst:revision` (zero interval means paused). Updates read
and change only configuration, never debt or its expiry; same-revision equivalent
updates perform no write, so an equivalent publication leaves an existing P3 value
unchanged. New policies and higher-revision updates write P1. Existing quota keys
hold managed debt as `D1|tat_us`; D3 debt, P3 policies and legacy tuple debt remain
strictly validated, read-only compatibility formats. Reading does not rewrite them.
Prune reclaims expired debt but validates and retains configuration and its revision
fence forever. Memory therefore includes all configured names; there is no automatic
policy retirement/reset. The `1` in D1/P1 numbers the first dynamic storage format,
not `Policy.Revision`, which remains positive and monotonically increasing.

**Stop old binaries before marker migration**, including every old reader, writer
and maintenance process. D3/P3-only readers cannot read newly written D1/P1 values;
older static writers can also bypass configuration. Do not mix old/new writers or
reset, rename or delete keys to migrate. Deploy protocol-aware binaries everywhere
before resuming traffic or policy publication. Mixed-version activation and
unconverted rollback are unsupported; idle time or pruning does not make rollback
safe. Preserve old receipt data separately. Callers own configuration
polling/publication; the SDK starts no worker. See the
[runnable managed example](examples/managed/main.go).

## Logical expiry and bounded physical cleanup

Keys use `pacelock:{scope_hash}:...`. Values use:

```text
quota:   D1|tat_us (all new debt writes)
policy:  P1|interval_us:burst:revision (all new policy writes; separate same-slot key)
legacy:  D3|tat_us / tat_us:interval_us:burst (strict read-only debt compatibility)
legacy:  P3|interval_us:burst:revision (strict read-only policy compatibility)
```

Debt logically expires at TAT, when capacity is fully replenished. Policy identity
never expires. D3 and legacy tuple debt are preserved, not reset on policy publication;
the next successful admission writes D1. Every admission requires policy keys.
The Lua protocol is `KEYS = [N debt keys, N policy keys]`, `ARGV = [N costs]`;
there is no mode selector or caller-supplied interval/burst.
These encodings are internal; applications must not construct them. Malformed
quota/policy state is rejected before mutation; obsolete quota formats remain rejected.

MSET clears native TTLs. Physical deletion is therefore an application-owned
operational requirement for bounded memory, separate from admission correctness.
Schedule repeated cleanup cycles for every used scope, including idle scopes;
constructors/admissions start no worker and do not run a full scan.

```go
scope := pacelock.Scope{Namespace: "llm-service", Domain: "provider-account-a"}
var cursor redisdriver.PruneCursor // Keep this across scheduled maintenance ticks.
// Example: at most eight steps during this tick; maintenanceCtx has a deadline.
for step := 0; step < 8; step++ {
    page, err := driver.Prune(maintenanceCtx, scope, cursor, 32)
    if err != nil { return err } // Ignore page; retry the previous cursor later.
    cursor = page.Cursor
    if cursor.Done() {
        cursor = redisdriver.PruneCursor{} // Begin a new cycle on a later tick.
        break
    }
}
```

- `Prune` requires `1 <= limit <= MaxPruneKeys` (64). Each call checks at most
  `limit` candidate values and performs at most one SCAN discovery page. COUNT is
  a hint, not a hard discovery/latency limit; overflow is retained in the cursor.
- Discovery routes to the scope's current primary, including Cluster. SCAN walks
  that node's keyspace, filtered by scope; many scopes can make repeated scans
  expensive. Measure and budget maintenance separately from admissions.
- A short Lua script re-reads quota and policy values, validates all candidates,
  and deletes expired debt in one DEL. Policies are permanent. Malformed or
  wrong-role values fail the batch without deletion. A stale candidate cannot
  authorize deleting updated active debt.
- `Examined`, `Deleted` and `Cursor` are valid only on success. Lost cleanup replies
  can be retried safely; counts are not an exactly-once audit ledger.
- Cursors are opaque, in-memory and scope-bound. Restart from zero after restart
  or topology changes. Concurrent modification makes SCAN best effort; repeat
  complete cycles. Cleanup failure leaves extra memory, not permission to erase
  live state. Monitor memory/cleanup lag and handle prolonged cleanup outages.
- Admission ACL: `+evalsha +eval +get +mset +time`, plus client handshake grants.
  Maintenance additionally needs `+scan +del`; a separate maintenance identity is
  supported. Cluster also needs `+cluster|slots`. See [security](../../SECURITY.md).

## Upgrading from retained admissions

Admission IDs, retention, receipts and recovery queries are no longer supported.
Quota keys and existing debt remain readable; new debt writes use D1. Do not
reset or rename keys. Existing `:attempt:` keys are ignored by admission and
excluded from `Prune` discovery, including expired or malformed records. They
are **not** automatically deleted, and persistent legacy records can occupy memory.
Retire all old receipt readers/writers first, then arrange a separate, explicitly
scoped cleanup of legacy `:attempt:` keys only after the application's recovery
requirements have ended. Never delete quota or policy keys as part of that cleanup.

Prepare cleanup and ACLs before deployment. Do not rename/delete keys to get fresh
capacity.

Use primary routing, TLS/authentication, controlled ACLs, noeviction and explicit
durability settings. [Operations](../../docs/operations.md), [core usage](../../README.md)
and [Driver contract](../../docs/driver.md) describe the remaining limits.
There is no automatic PostgreSQL failover or cross-backend state migration.

Run `REDIS_ADDR=localhost:16379 go run ./examples/llm` from this module.
Run `sh scripts/check.sh` at the root for all modules; Redis tests include model,
concurrency, logical expiry, state validation, cleanup, ACL/OOM
and lost-response faults.

Short local baseline/MSET and candidate-cleanup measurements are in the
[verification record](../../docs/validation/redis-mset-local.md); they are not production SLOs.
The historical [Lua encoding experiment](bench/lua-parse/README.md) includes a
archived raw timings for the former quota encoding; the removed scalar
prototype drops policy checks and is not a contract-preserving replacement.
