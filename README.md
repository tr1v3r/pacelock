# Pacelock

[![CI](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml)
[简体中文](README.zh-CN.md)

**Atomic, weighted, multidimensional GCRA admission for Go, with Redis and PostgreSQL drivers.**

Pacelock coordinates shared quotas across service instances. One admission can
charge request count, known input tokens and tenant capacity together: all pass
and are charged, or a quota denial charges none. LLM APIs are a primary use case;
the core accepts any known positive cost.

The API is pre-v1. This SDK is a rate-admission component, with explicit failure
contracts; it does not implement an LLM gateway or billing
ledger. Requires Go 1.26+. Choose Redis 7+ or PostgreSQL 16+. Production builds should use a currently security-maintained Go release with its latest patches. See [compatibility](docs/compatibility.md)
and the [verification record](docs/verification.md) for tested configurations.

## Fully commented SDK for agents

Pacelock is a **fully commented SDK** intended to help coding agents use it correctly.
Source comments cover packages, types, interfaces, functions, methods, fields,
constants and variables, explaining input constraints, concurrency and lifecycle
rules, and error handling. Read the comments for your pinned version alongside
the examples when integrating the SDK.

Start with the [default dynamic API](managed.go), [Rule and pure math helpers](pacelock.go), [driver contract](driver.go), and [error outcomes](errors.go).

For a visual walkthrough, see the [historical interactive admission workflows](docs/workflows/README.md)
for the public API, PostgreSQL transactions and Redis Lua (diagrams in Simplified Chinese).

## Capabilities

- Weighted GCRA and atomic admission across 1–32 named quotas.
- Publish revisioned policies; default admission submits named `Charge` costs only.
- Backend server time, shared state and logical expiry with explicit physical cleanup.
- Redis Lua with one MSET commit; bounded, application-scheduled `Driver.Prune`.
- A standard-library-only core and independent Redis/PostgreSQL driver modules.
- Immediate decisions, advisory `RetryAfter` and all blocking quota names.
- Context-aware blocking `Limiter.Wait` for weighted, multidimensional admission.
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

## Policies in code, execution in the backend

`New` / `NewWithOptions` return the sole `Limiter`, exposing `UpdateQuotas`,
`Allow` and `Wait`. Configuration can remain in application code: publish `Quota`
values with positive `Policy.Revision` explicitly, then send only `Charge{Name, Cost}`.
Every admission reads backend-authoritative policy, so rolling pods share the
latest policy even if their code carries different configuration revisions.
Stale publication returns `ErrPolicyMismatch`: explicitly handle or surface it;
do not silently treat every update failure as success. Rollback publishes the old
parameters at a **higher revision**, never an older revision or renamed quota.
Include every applicable quota; omitted quotas are not checked or charged.
`Decision.Limited` follows request charge order.

`Rule`, `Validate` and `Evaluate` remain offline math helpers only; they do not
provide a caller-policy backend path. `EvaluateManaged` is also pure.

## PostgreSQL and the Driver boundary

Install `github.com/tr1v3r/pacelock/driver/postgres@dev` for PostgreSQL.
The application registers a `database/sql` transport and owns a primary DB:

```go
driver, err := postgres.New(db) // import github.com/tr1v3r/pacelock/driver/postgres
if err != nil { return err }
if err := driver.Migrate(ctx); err != nil { return err } // explicit setup, not per request
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

Both drivers support `Limiter` (`UpdateQuotas`, `Allow`, `Wait`), with shared
outcomes and observers. PostgreSQL
locks one domain row, samples `clock_timestamp()` after locking, and commits every
quota mutation in one short transaction. Pre-commit failures roll back;
commit errors are `Unknown`. This driver does not join a caller's business transaction.
Domain contention, WAL, pools and vacuum need deployment testing; no throughput
advantage is claimed. `Prune` offers bounded caller-scheduled physical cleanup;
logical expiry works without cleanup. The SDK starts no background workers.

See the [PostgreSQL guide](driver/postgres/README.md), [Redis guide](driver/redis/README.md),
and [custom-driver contract](docs/driver.md). Drivers are alternatives: switching
backends does not transfer quota debt and is not automatic failover.

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
- Redis uses one persistent P1 configuration key and one D1 debt key per managed
  quota, all explicit and in the same slot. `Allow` is one script roundtrip,
  not a client-side GET followed by a write. PostgreSQL uses a separate policies
  table and the existing scope transaction lock. No distributed-lock key is needed.
- D1/P1 number the first dynamic storage format, not `Policy.Revision` (still
  positive and monotonic). D3/P3 and old tuple debt have strict read-only
  compatibility. Equivalent same-revision updates leave P3 unchanged; new policies
  or higher revisions write P1, and the next allowed debt write uses D1.
  **Stop old binaries before marker migration**: D3/P3-only readers cannot read
  D1/P1. Upgrade readers, writers and maintenance before resuming; no mixed old/new
  writers or resets/deletions to migrate. Preserve revision fences and old receipts.
  See [Redis migration](docs/compatibility.md#redis-state).
- `Rate=0` requires positive Period and Burst. Fresh admission returns `ErrPaused`
  / `NotCharged` without a debit. Missing configuration returns
  `ErrPolicyMissing`, never unlimited admission. Cost above current burst is
  `ErrInvalid`. Dynamic `Wait` stops on these errors rather than waiting for a
  policy change. Applications own publication and configuration-source polling;
  the SDK starts no polling goroutine.
- `Prune` may reclaim expired debt, but never configuration/revisions.
  Plan configuration cardinality accordingly; deleting a config loses its version
  fence. All dynamic updates must use the SDK/version-check script, not raw SET.
- For PostgreSQL, run explicit `Migrate` before upgraded request binaries serve
  traffic. Retire old static writers or stop their static path **before installing
  policies**. Migration preserves existing debt and legacy receipt data; it does
  not make old writers safe to mix with backend-authoritative admission.
- Transport/commit errors may leave publication uncertain. No automatic retry is
  performed; explicitly retry the SAME revision/policy to converge. Optional
  `QuotaDriver` publishes configuration; dynamic `Driver.Admit(Admission)` is mandatory.
  missing optional capabilities return `ErrUnsupported` without mutation.

Runnable SDK example: [managed admission](driver/redis/examples/managed/main.go).

## Blocking admission

`Limiter.Wait(ctx, charges...) error` tries immediately, then sleeps for advisory
`RetryAfter` only after a valid ordinary capacity denial. It copies the caller's
name/cost charge slice once; each attempt atomically reads the latest backend
policies, not a policy snapshot.

```go
// limiter and charge are from the dynamic setup above; its policy is already published.
charges := []pacelock.Charge{charge}
waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
if err := limiter.Wait(waitCtx, charges...); err != nil {
    // All errors stop waiting. Unknown may have charged: never retry automatically.
    return err
}
// Quota was consumed once. Execute this business attempt.
```

A nil error confirms admission and consumption; business failure does not refund
it. Cancellation between attempts is `NotCharged`. Cancellation during a backend
call preserves its accounting outcome, including uncertainty; a confirmed successful
debit returns nil even if cancellation arrives concurrently. Every error stops
waiting: `ErrPaused`, `ErrPolicyMissing`, `ErrInvalid` (including a new Burst below
Cost), transport errors even when `NotCharged`, `Unknown`, unsupported capabilities
and invalid driver results. Use `errors.Is` and `OutcomeOf` on returned errors;
`Error.Op` is `wait`.

Wait uses a private waiting loop in the calling goroutine. Sleeping holds
no backend lock or connection and adds no queue, worker or background goroutine.
Policy updates do not proactively wake sleepers: a previous `RetryAfter` may delay
use of a relaxed policy. `RetryAfter` reserves no capacity; competing callers can
win repeatedly, so there is no FIFO or starvation guarantee. Bound waiting with a
context deadline.

## Breaking pre-v1 removal

Admission is non-idempotent. `AllowOnce`, `Lookup`, `Receipt`, `LookupRequest`,
admission IDs/retention/fingerprints and `Event.Replayed` have been removed.
The sole `Limiter` exposes `Allow(ctx, ...Charge) (Decision, error)`,
`Wait(ctx, ...Charge) error` and `UpdateQuotas`. `Driver.Admit(ctx, Admission)`
returns `(Decision, error)`. Static constructors, limiter, driver and admission
interfaces are removed, with no compatibility aliases.
There is no result recovery after an uncertain mutation: **do not blindly retry Unknown**.
This is rate admission, not billing, refunds, business execution, queue or worker ownership.

Upgrading does not automatically drop legacy receipt storage. Old Redis attempt
keys and PostgreSQL admission tables/rows may remain; coordinate retirement of old
writers, backups and explicit operator cleanup. See the [migration guide](docs/compatibility.md).

## Errors and observation

Use `errors.Is` for `ErrInvalid`, `ErrPolicyMismatch`, `ErrState`,
`ErrBackend`, and wrapped context/transport errors. Admission
errors expose `*pacelock.Error` with `Op` and `Outcome`:

| Result | Meaning | Application action |
|---|---|---|
| `Allowed=false`, no error | Quota denied; no quota debit | Reject or schedule a new attempt |
| `NotCharged` error | This invocation is known not to debit | Fix input/policy, or handle cancellation |
| `Unknown` error | May have charged; confirmation is unavailable | Do not blindly retry; apply explicit application failure policy |

A `NotCharged` result never clears uncertainty from an earlier call. On **any**
error, ignore the decision. `ErrState` alone does not mean no debit: a malformed
reply after a debit also returns it. Check `OutcomeOf(err)`; it is conservative for
unrecognized errors.

`Validate(rules...)` checks math bounds offline. `NewWithOptions`
accepts a synchronous `Observer`: one event per admission; constructors and `UpdateQuotas` emit none. Operation names are
`allow` and `wait`. Each Wait emits one event for the
whole call, not each internal attempt. Events expose counts, duration, result,
delay and error without raw names as fields. Errors can contain identifiers:
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
- Policies require positive revisions installed explicitly through `UpdateQuotas`.
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
  Clock jumps, eviction and failover data loss can weaken limits.

See [operations and incident recovery](docs/operations.md),
[design and proposed extensions](docs/design.md), and [compatibility](docs/compatibility.md).

## Examples and validation

```sh
docker compose up -d --wait
(cd driver/redis && REDIS_ADDR=localhost:16379 go run ./examples/llm)
(cd examples/postgres && go run .)
(cd examples/redis && go run . -redis localhost:16379)
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres sh scripts/check.sh
(cd driver/redis/internal/engine && go test -run '^$' -fuzz FuzzInterval -fuzztime 10s .)
docker compose down
```

CI covers Go 1.26/stable, Redis 7.0/7/8, a three-master Cluster, and PostgreSQL 16/17/18.
Shared contracts and PG rollback/commit-loss tests run with race detection.
The default suite includes synchronized bursts of 100/500/1000 callers, up to
32 quota dimensions, weighted/shared quotas, tenant isolation,
cancellation, pool exhaustion, lost-response accounting, and four-process contention.
CI also repeats the concurrency suite three times with `-race` and randomized order.
See [concurrency test contracts and reproduction](docs/concurrency-testing.md). Benchmark
results depend on deployment; there is no universal throughput or failover SLO.
See [contribution/release checks](CONTRIBUTING.md) and [security policy](SECURITY.md).

Deployment load, memory and failover tools with raw local observations are documented in [deployment validation](docs/deployment-validation.md). These are bounded fixture measurements, not production SLOs.

MIT licensed.

The core, both runtime drivers, the optional pgx example and the
integration-test modules require Go 1.26+. Production applications should
use a maintained Go release.
