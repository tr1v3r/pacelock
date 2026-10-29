# Pacelock

[![CI](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml)
[简体中文](README.zh-CN.md)

**Atomic, weighted, multidimensional GCRA admission for Go, with Redis and PostgreSQL drivers.**

Pacelock coordinates shared quotas across service instances. One admission can
charge request count, known input tokens and tenant capacity together: all pass
and are charged, or a quota denial charges none. LLM APIs are a primary use case;
the core accepts any known positive cost.

The API is pre-v1. This SDK is a rate-admission component, with explicit failure
contracts and bounded idempotency; it does not implement an LLM gateway or billing
ledger. Requires Go 1.26+. Choose Redis 7+ or PostgreSQL 16+. Production builds should use a currently security-maintained Go release with its latest patches. See [compatibility](docs/compatibility.md)
and the [verification record](docs/verification.md) for tested configurations.

## Fully commented SDK for agents

Pacelock is a **fully commented SDK** intended to help coding agents use it correctly.
Source comments cover packages, types, interfaces, functions, methods, fields,
constants and variables, explaining input constraints, concurrency and lifecycle
rules, and error handling. Read the comments for your pinned version alongside
the examples when integrating the SDK.

Start with the [default dynamic API](managed.go), [standalone static API](static.go), [Rule and pure math helpers](pacelock.go), [driver and recovery contract](driver.go), and [error outcomes](errors.go).

For a visual walkthrough, see the [historical interactive admission workflows](docs/workflows/README.md)
for the public API, PostgreSQL transactions and Redis Lua (diagrams in Simplified Chinese).

## Capabilities

- Weighted GCRA and atomic admission across 1–32 named quotas.
- Publish revisioned policies; default admission submits named `Charge` costs only.
- Backend server time, shared state and logical expiry with explicit physical cleanup.
- Redis Lua with one MSET commit; bounded, application-scheduled `Driver.Prune`.
- A standard-library-only core and independent Redis/PostgreSQL driver modules.
- Immediate decisions, advisory `RetryAfter` and all blocking quota names.
- Context-aware blocking `StaticLimiter.Wait` for weighted, multidimensional admission.
- Bounded idempotent admission with parameter conflict detection and result lookup.
- Typed accounting outcomes, offline rule validation and synchronous observation.
- Real Redis/Cluster, race, model, fuzz and failure-injection tests; benchmarks.

There is no queue, tokenization, output reservation, refund,
streaming reconciliation or concurrency ownership. [Airlock](https://github.com/tr1v3r/airlock)
handles concurrent work; Pacelock handles consumption over time. Their operations
are not a joint transaction.

## Install and use

This README documents the pre-release `dev` line. Install that line explicitly:

```sh
go get github.com/tr1v3r/pacelock@dev github.com/tr1v3r/pacelock/driver/redis@dev
```

The default `@latest` currently selects an older API and does not include all APIs
shown here. Keep the resolved pseudo-version in your application's `go.mod` for
reproducible builds; `@dev` moves as development continues and is not a stable release.

The constructor accepts `pacelock.Driver`.
Import `redisdriver "github.com/tr1v3r/pacelock/driver/redis"` for the example.

```go
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    ContextTimeoutEnabled: true,
})
defer client.Close()

driver, err := redisdriver.New(client)
if err != nil {
    return err
}
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
if err != nil {
    return err
}
rpm := pacelock.Quota{Name: "account:rpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 60, Period: time.Minute, Burst: 10,
}}
tokens := pacelock.Quota{Name: "model-a:input-tpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 60000, Period: time.Minute, Burst: 12000,
}}
// Publish once from the control plane; requests send only names and costs.
if err := limiter.UpdateQuotas(ctx, rpm, tokens); err != nil { return err }
charges := []pacelock.Charge{{Name: rpm.Name, Cost: 1}, {Name: tokens.Name, Cost: 1500}}
decision, err := limiter.Allow(ctx, charges...)
if err != nil {
    // Ignore decision. OutcomeOf(err) is NotCharged or Unknown.
    // Choose an explicit application failure policy; do not blindly retry.
    return err
}
if !decision.Allowed {
    // Reject, or let an external scheduler consider RetryAfter and Limited.
    // RetryAfter does not reserve future capacity.
}
```

`Allow` consumes capacity for this attempt and is **not idempotent**. Business
failure after admission does not refund capacity. For the Redis driver, the limiter neither owns nor
closes the Redis client. Constructors do not connect or change client settings.
Use go-redis constructors to initialize clients; zero-value client structs are
unsupported. For these initialized concrete `*redis.Client` / `*redis.ClusterClient`
instances, default, positive and disabled (`-1`) `MaxRetries` budgets are supported.
Unsupported negative budgets return `ErrInvalid`. Configure the client before
passing it in; do not mutate its options afterward.

`redis.Ring` is unsupported and rejected with `ErrInvalid`, including when retries are disabled. Custom Scripter wrappers remain responsible for their replay and routing behavior.

For Cluster, replace the client construction above with:

```go
client := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{"localhost:17000", "localhost:17001", "localhost:17002"},
    MaxRedirects: 3,
    ContextTimeoutEnabled: true,
})
defer client.Close()
```

Pacelock marks commands on concrete Client/ClusterClient instances with go-redis
`NoRetry`. This prevents ambiguous network replay while allowing bounded MOVED/ASK
recovery, independently of `MaxRetries`. Other commands on a shared client keep
the configured retry policy. A default or positive Cluster MaxRedirects budget
is supported;
`MaxRedirects: -1` still disables redirects. Custom Scripter wrappers, hooks,
node factories and proxies must preserve this contract. See [operations](docs/operations.md).

Instances sharing quotas use the same namespace, domain and quota names. The
backend owns the current dynamic policy; request nodes send names/costs only.
Include all applicable account/model/tenant quotas; omitted quotas cannot be
inferred. Different namespaces/domains are independent.

## Explicit static alternative: fixed policies and costs

Construct a standalone `StaticLimiter` when policies are fixed in application code.
The default `New(driver, namespace, domain)` still returns a dynamic `*Limiter`.
Static construction does not require a parent limiter or publish backend policies:

```go
NewStatic(driver Driver, namespace, domain string, quotas ...Quota) (*StaticLimiter, error)
NewStaticWithOptions(driver Driver, namespace, domain string, options Options, quotas ...Quota) (*StaticLimiter, error)
```

`Policy` contains `Revision`, `Rate`, `Period` and `Burst` and is embedded by the
low-level `Rule`; `Quota` has public `Name` and `Policy` fields. Both dynamic and
static admission take public `Charge{Name, Cost}` values, never per-call policies.

- Construction validates and copies 1–32 uniquely named quotas, all with
  `Revision == 0`. It performs no backend I/O or policy publication. Invalid
  definitions return an error wrapping `ErrInvalid`.
- A `StaticLimiter` is immutable and concurrency-safe under its driver's contract.
  Changing the original quota slice, quota or policy does not change its fixed
  policies. Each call assembles its own rules without mutating inputs.
  A driver without `StaticDriver` may be constructed; admission then returns
  `ErrUnsupported` / `NotCharged`, while `Lookup` remains available.
- `static.Allow(ctx, charges...)`, `static.AllowOnce(ctx, id, retention, charges...)`
  and `static.Wait(ctx, charges...)` accept any nonempty subset of fixed quotas as
  `...Charge`, each selected name exactly once with `0 < Cost <= Burst`.
  Omitted quotas are neither checked nor charged. Empty calls, duplicate or unknown
  names and invalid costs fail locally with `ErrInvalid` / `NotCharged` before
  dispatch. There are no default costs or zero-cost exemptions. Each call is atomic
  only across its selected quotas; separate calls are not atomic together.
- Charge order is arbitrary. Selected quotas are normalized to construction
  (binding) order for `Decision.Limited` and idempotency. Fingerprints include only
  selected names, normalized order, effective policies, costs and retention.
  Reordering charges preserves replay; changing the subset or selected quotas'
  relative binding order with the same retained ID returns `ErrConflict`.
  Unselected quotas do not affect the fingerprint.
- Build quota names from trusted identity and choose the applicable set in
  application code. The SDK cannot infer omitted applicable quotas.
- Policy changes require a new `StaticLimiter`; construction neither migrates debt
  nor bypasses backend `ErrPolicyMismatch` checks. Keep the original fixed policies,
  costs and retention available for retained-attempt recovery.
- `static.Lookup(ctx, id)` queries its namespace/domain, not just its fixed quota
  set. It can recover results created by another limiter in the same scope.

```go
// Use a distinct static name with no published dynamic configuration.
q := pacelock.Quota{Name: "static:rpm", Policy: pacelock.Policy{
    Rate: 60, Period: time.Minute, Burst: 10, // Revision defaults to 0
}}
static, err := pacelock.NewStatic(driver, "llm-service", "provider-account-a", q)
if err != nil { return err }
decision, err := static.Allow(ctx, pacelock.Charge{Name: q.Name, Cost: 1})
if err != nil { return err }
if !decision.Allowed { /* reject */ }
```

### Low-level driver and pure helpers

`Rule` remains the low-level caller-policy value used by
`StaticDriver.AdmitStatic(StaticAdmission)` and the pure `Validate` / `Evaluate`
helpers. `EvaluateManaged` also remains pure. Public static admission is through
`StaticLimiter`, not rule-taking methods on `Limiter`. Static admission cannot
bypass configured dynamic quotas.

## PostgreSQL and the Driver boundary

Install `github.com/tr1v3r/pacelock/driver/postgres@dev` for PostgreSQL.
The application registers a `database/sql` transport and owns a primary DB:

```go
driver, err := postgres.New(db) // import github.com/tr1v3r/pacelock/driver/postgres
if err != nil { return err }
if err := driver.Migrate(ctx); err != nil { return err } // explicit setup, not per request
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

Both drivers support dynamic `Limiter` (`UpdateQuotas`, `Allow`, `AllowOnce`, `Lookup`)
and standalone `StaticLimiter` (`Allow`, `AllowOnce`, `Wait`, `Lookup`), with shared
outcomes and observers. PostgreSQL
locks one domain row, samples `clock_timestamp()` after locking, and commits every
quota and retained result in one short transaction. Pre-commit failures roll back;
commit errors are `Unknown`. This driver does not join a caller's business transaction.
Domain contention, WAL, pools and vacuum need deployment testing; no throughput
advantage is claimed. `Prune` offers bounded caller-scheduled physical cleanup;
logical expiry works without cleanup. The SDK starts no background workers.

See the [PostgreSQL guide](driver/postgres/README.md), [Redis guide](driver/redis/README.md),
and [custom-driver contract](docs/driver.md). Drivers are alternatives: switching
backends does not transfer quota debt or retained IDs and is not automatic failover.

## Default dynamic policies and pause

This is a breaking pre-v1 rename: `UpdatePolicies` → `UpdateQuotas`,
`PolicyDriver` → `QuotaDriver`, and `PolicyUpdate` → `QuotaUpdate`; the old public
names are removed. `Policy` and the pure `ApplyPolicies` helper keep their names.
See the [migration map](docs/compatibility.md#dynamic-default-api-migration-breaking-pre-v1)
for former static `Allow` / dynamic `AllowManaged`, `NamedCharge`, `Bind` and custom Driver changes.

Use `UpdateQuotas` for the control plane and `Allow` for the data plane.
Redis and PostgreSQL store persistent configuration separately from expiring TAT
(debt). Request processes need only quota names and costs, not local policies,
revisions, bindings or configuration watchers:

```go
// Control plane: application-owned polling/watch callback publishes source revisions.
q := pacelock.Quota{Name: "tenant:rpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 100, Period: time.Minute, Burst: 20,
}}
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }

// Data plane: this charge stays identical across rate changes and pause/resume.
charge := pacelock.Charge{Name: "tenant:rpm", Cost: 1}
decision, err := limiter.Allow(ctx, charge)
if err != nil { return err }
if !decision.Allowed { /* reject or schedule another attempt */ }

// Later configuration publication: no debt reset and no request-node refresh.
q.Policy.Revision = 2
q.Policy.Rate = 200
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }
q.Policy.Revision = 3
q.Policy.Rate = 0 // emergency pause; fresh requests return ErrPaused
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }
q.Policy.Revision = 4
q.Policy.Rate = 100 // resume; debt recovered naturally during the pause
if err := limiter.UpdateQuotas(ctx, q); err != nil { return err }
```

- Revisions come from one authoritative configuration source, not per-process
  counters. Multiple instances may publish the same batch: higher revisions win,
  identical revision/effective policy is a read-only success, and stale or conflicting
  revisions return `ErrPolicyMismatch`. Rollback publishes old parameters at a NEW
  revision. A batch of up to `MaxRules` configurations updates atomically per scope.
- Quota configuration changes do not refill capacity or reset debt. Updates do
  not read, rescale, reset or shorten the lifetime of TAT. Admission
  reads current configuration and debt under the same atomic boundary. Outstanding
  debt exceeding a newly reduced burst is retained until natural recovery.
- Redis uses one persistent P3 configuration key and one D3 debt key per managed
  quota, all explicit and in the same slot. `Allow` is one script roundtrip,
  not a client-side GET followed by a write. PostgreSQL uses a separate policies
  table and the existing scope transaction lock. No distributed-lock key is needed.
- `Rate=0` requires positive Period and Burst. Fresh admission returns `ErrPaused`
  / `NotCharged` without a debit or receipt. Missing configuration returns
  `ErrPolicyMissing`, never unlimited admission. Cost above current burst is
  `ErrInvalid`. There is no managed blocking Wait API or SDK polling goroutine;
  applications own publication, retry scheduling and configuration-source polling.
- `AllowOnce(ctx, id, retention, charges...)` retains ordinary allow/deny
  decisions. Its fingerprint binds ordered names/costs and retention, NOT policy
  versions. Same-attempt retries replay the original result even after updates or
  pause; `Lookup` reads either API's receipt. Changed costs or reuse across static
  and managed APIs produce `ErrConflict`. Replay never authorizes executing twice.
- `Prune` may reclaim expired debt and receipts, but never configuration/revisions.
  Plan configuration cardinality accordingly; deleting a config loses its version
  fence. All dynamic updates must use the SDK/version-check script, not raw SET.
- `NewStatic` / `NewStaticWithOptions` require Revision-0 fixed policies. Once a backend
  configuration exists, fresh static admission for that name fails closed with
  `ErrPolicyMismatch`; old static receipts remain recoverable. A managed policy
  cannot be passed to these static constructors. Use dynamic name/cost admission instead.
- For PostgreSQL, run explicit `Migrate` to add the policies table BEFORE upgraded
  request binaries serve traffic, including static traffic. Deploy protocol-capable
  binaries everywhere BEFORE enabling managed policies. Unmodified old writers
  cannot be mixed with managed quotas. Policy updates and admissions
  serialize normally; slow Lua scripts would still block other Redis commands.
- Transport/commit errors may leave publication uncertain. No automatic retry is
  performed; explicitly retry the SAME revision/policy to converge. Optional
  `QuotaDriver` publishes configuration; dynamic `Driver.Admit(Admission)` is mandatory.
  Optional `StaticDriver.AdmitStatic(StaticAdmission)` supports caller-policy admission;
  missing optional capabilities return `ErrUnsupported` without mutation.

Runnable SDK example: [managed admission](driver/redis/examples/managed/main.go).

## Blocking admission

`static.Wait(ctx, charges...) error` uses the fixed policies of a standalone
`StaticLimiter`. It snapshots and validates the supplied name/cost charges before
waiting, tries immediately, sleeps for `RetryAfter` only after ordinary denials,
and rechecks the selected quotas atomically until admitted or an error occurs:

```go
quotas := []pacelock.Quota{
    {Name: "static:account:rpm", Policy: pacelock.Policy{Rate: 60, Period: time.Minute, Burst: 10}},
    {Name: "static:model-a:input-tpm", Policy: pacelock.Policy{Rate: 60000, Period: time.Minute, Burst: 12000}},
}
static, err := pacelock.NewStatic(driver, "llm-service", "provider-account-a", quotas...)
if err != nil { return err }
charges := []pacelock.Charge{
    {Name: quotas[0].Name, Cost: 1},
    {Name: quotas[1].Name, Cost: 1500},
}
waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
if err := static.Wait(waitCtx, charges...); err != nil {
    // All errors stop waiting. Unknown may have charged: never retry automatically.
    return err
}
// Quota was consumed once. Execute this business attempt.
```

A nil error confirms admission and consumption; business failure does not refund
it. Cancellation between attempts is `NotCharged`. Cancellation during a backend
call preserves its accounting outcome; a confirmed successful debit returns nil
even if cancellation arrives concurrently. Every backend error stops waiting,
including `NotCharged` errors. Use `errors.Is` and `OutcomeOf` on returned errors;
`Error.Op` is `wait_static`.

Waiting uses the calling goroutine and a snapshot of the supplied charges. It holds
no backend lock or connection while sleeping and adds no queue or background
worker. `RetryAfter` reserves no capacity: competing callers can win repeatedly,
so there is no FIFO or starvation guarantee. Bound waiting with a context deadline.

`StaticLimiter.Wait` is non-idempotent and creates no retained ID for `Lookup`. A denied
`AllowOnce` ID replays its original denial, so polling one retained ID cannot
implement blocking admission. Use the explicit recovery flow below when durable
attempt identity is required.

## Idempotent admission and recovery

```go
// Persist an admission ID alongside one business attempt before dispatch.
receipt, err := limiter.AllowOnce(ctx, admissionID, 10*time.Minute, charges...)
if err != nil {
    return err
}
// receipt.Decision is the original decision; receipt.Replayed identifies a replay.
// Replayed=true does not authorize executing the upstream request again.
```

After an ambiguous response, use a fresh context to query:

```go
decision, found, err := limiter.Lookup(recoveryCtx, admissionID)
```

- An ID is scoped to namespace/domain. Default dynamic fingerprints bind **ordered
  names, costs and retention**, not policy versions. Static fingerprints additionally
  bind effective intervals and bursts, preserving the effective-rule encoding. Static
  calls normalize to construction order. Cross-mode reuse conflicts through the fingerprint discriminator.
  Names must be valid UTF-8 for retained admissions; IDs contain 1–256 bytes.
  Static fingerprints contain only selected quotas, normalized to construction
  order; changing the subset conflicts, while omitted quotas do not affect identity.
- Retention is an explicit whole number of milliseconds from 1 ms to 24 h. Choose
  a value covering your recovery window. Replays and queries never extend it.
- Both allow and deny decisions are retained. A denied ID stays denied while
  retained; a later, genuinely new attempt needs a new ID. Replayed `RetryAfter`
  is the original delay, not a countdown or fresh capacity estimate.
- Redis writes quotas and the complete decision in one final MSET, after all
  checks/encoding. Malformed receipts return `ErrState` / `Unknown`, never a
  missing result or permission for a new admission.
  Redis expiry is logical; schedule bounded physical cleanup for every scope.
  See the [Redis protocol and cleanup guide](driver/redis/README.md).
- `found=false` does **not** prove no debit. The command may still be in flight,
  or the record may have expired or been lost. Expiry, eviction, deletion and
  failover loss end deduplication. Never reuse an old ID after that boundary.
- Deduplicating admission does not deduplicate upstream execution. Coordinate the
  business attempt separately. A new actual model call needs a new admission ID.

For a complete application-owned flow using explicit static policies, see the [durable admission example](driver/redis/examples/durable/README.md): trusted quota assembly, persisted attempt IDs, restart recovery and explicit handling of unknown upstream execution.

## Errors and observation

Use `errors.Is` for `ErrInvalid`, `ErrPolicyMismatch`, `ErrState`, `ErrConflict`,
`ErrBackend`, and wrapped context/transport errors. Admission
errors expose `*pacelock.Error` with `Op` and `Outcome`:

| Result | Meaning | Application action |
|---|---|---|
| `Allowed=false`, no error | Quota denied; no quota debit | Reject or schedule a new attempt |
| `NotCharged` error | This invocation is known not to debit | Fix input/policy, or handle cancellation |
| `Unknown` error | May have charged; confirmation is unavailable | Query retained ID; use explicit recovery policy |
| `Lookup`: missing | No retained complete result was found | Preserve uncertainty about previous execution |

A `NotCharged` result never clears uncertainty from an earlier call. On **any**
error, ignore the decision. `ErrState` alone does not mean no debit: a malformed
reply after a debit also returns it. Check `OutcomeOf(err)`; it is conservative for
unrecognized errors.

`Validate(rules...)` checks static bounds offline. `NewWithOptions` and
`NewStaticWithOptions` accept a synchronous `Observer`: one event per
dynamic/static admission or `Lookup`; constructors and `UpdateQuotas` emit none. Operation names are `allow`, `allow_once`,
`allow_static`, `allow_static_once`, `wait_static` and `lookup`. `StaticLimiter.Allow`
/ `.AllowOnce` emit static operations. `StaticLimiter.Wait` emits one event for the whole
wait, not each internal attempt. Events expose counts, duration, result, replay,
delay and error without raw names/IDs as fields. Errors can contain identifiers:
never use them as metric labels. Callbacks must be fast and concurrency-safe;
panics propagate.

## Quota and operational contracts

- `Rate / Period` is replenishment speed. `Burst` is total instantaneous capacity,
  not extra capacity. `0 < Cost <= Burst`; periods and burst refill horizons are
  at most 24 h. Intervals round upward to microseconds, conservatively reducing
  effective rates at high throughput.
- GCRA is a rate/burst envelope, not an exact fixed/sliding-minute counter. Model
  provider limits explicitly. Known input tokens do not enforce output TPM;
  character, byte and SSE chunk counts are not token counts.
- Effective interval and burst identify a static (Revision=0) policy. Changing
  active static state returns `ErrPolicyMismatch`. Managed policies additionally
  require the persistent revision installed explicitly through `UpdateQuotas`.
- One domain occupies one Redis Cluster slot. A shared global quota can force a
  large atomicity domain onto one shard; arbitrary sharding cannot preserve a
  cross-slot transaction.
- SDK script commands disable ambiguous retries with `NoRetry`; client retry
  settings need not be disabled, and Cluster redirects may remain enabled.
  Only `NOSCRIPT` falls back from `EVALSHA` to `EVAL`. Constructor validation is
  not a substitute for auditing custom clients, hooks, factories and proxies.
- Pre-dispatch cancellation does not debit; cancellation after dispatch cannot
  undo Redis writes. The SDK creates no goroutines and imposes no hidden timeout.
- Use dedicated keys, primary routing, TLS/authentication and Redis `noeviction`.
  The Redis protocol uses a single commit command; crash/failover durability
  still depends on deployment.
  Clock jumps, eviction and failover data loss can weaken limits and deduplication.

See [operations and incident recovery](docs/operations.md),
[design and proposed extensions](docs/design.md), and [compatibility](docs/compatibility.md).

## Examples and validation

```sh
docker compose up -d --wait
(cd driver/redis && REDIS_ADDR=localhost:16379 go run ./examples/llm)
(cd driver/redis && REDIS_ADDR=localhost:16379 go run ./examples/idempotency)
(cd examples/postgres && go run .)
(cd examples/redis && go run . -redis localhost:16379)
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres sh scripts/check.sh
(cd driver/redis/internal/engine && go test -run '^$' -fuzz FuzzInterval -fuzztime 10s .)
(cd driver/redis/internal/engine && go test -run '^$' -fuzz FuzzRecord -fuzztime 10s .)
docker compose down
```

CI covers Go 1.26/stable, Redis 7.0/7/8, a three-master Cluster, and PostgreSQL 16/17/18.
Shared contracts and PG rollback/commit-recovery tests run with race detection.
The default suite includes synchronized bursts of 100/500/1000 callers, up to
32 quota dimensions, weighted/shared quotas, tenant isolation, idempotency races,
cancellation, pool exhaustion, lost-response recovery, and four-process contention.
CI also repeats the concurrency suite three times with `-race` and randomized order.
See [concurrency test contracts and reproduction](docs/concurrency-testing.md). Benchmark
results depend on deployment; there is no universal throughput or failover SLO.
See [contribution/release checks](CONTRIBUTING.md) and [security policy](SECURITY.md).

Deployment load, memory and failover tools with raw local observations are documented in [deployment validation](docs/deployment-validation.md). These are bounded fixture measurements, not production SLOs.

MIT licensed.

The core, both runtime drivers, the optional pgx example and the
integration-test modules require Go 1.26+. Production applications should
use a maintained Go release.
