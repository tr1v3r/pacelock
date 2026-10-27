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

Start with the [bound policy API](bound.go), [low-level admission API](pacelock.go), [driver and recovery contract](driver.go), and [error outcomes](errors.go).

For a visual walkthrough, see the [interactive admission workflows](docs/workflows/README.md)
for the public API, PostgreSQL transactions and Redis Lua (diagrams in Simplified Chinese).

## Capabilities

- Weighted GCRA and atomic admission across 1–32 named quotas.
- Bind fixed `Policy`/`Quota` definitions and submit per-attempt `Charge` costs.
- Backend server time, shared state and logical expiry with explicit physical cleanup.
- Redis Lua with one MSET commit; bounded, application-scheduled `Driver.Prune`.
- A standard-library-only core and independent Redis/PostgreSQL driver modules.
- Immediate decisions, advisory `RetryAfter` and all blocking quota names.
- Context-aware blocking `Wait` for weighted, multidimensional admission.
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
go get github.com/tr1v3r/pacelock@dev github.com/tr1v3r/pacelock/drivers/redis@dev
```

The default `@latest` currently selects an older API and does not include all APIs
shown here. Keep the resolved pseudo-version in your application's `go.mod` for
reproducible builds; `@dev` moves as development continues and is not a stable release.

The constructor now accepts `pacelock.Driver`, a breaking pre-v1 change.
Import `redisdriver "github.com/tr1v3r/pacelock/drivers/redis"` for the example.

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
    Rate: 60, Period: time.Minute, Burst: 10,
}}
tokens := pacelock.Quota{Name: "model-a:input-tpm", Policy: pacelock.Policy{
    Rate: 60000, Period: time.Minute, Burst: 12000,
}}
// Bind fixed policies once; reuse bound for concurrent requests.
bound, err := limiter.Bind(rpm, tokens)
if err != nil {
    return err
}
// Each charge captures a quota snapshot and this attempt's cost; order does not matter.
charges := []pacelock.Charge{rpm.Charge(1), tokens.Charge(1500)}
decision, err := bound.Allow(ctx, charges...)
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

Every instance must use the same namespace, domain, quota names and effective
policies. Bind all applicable account/model/tenant quotas; omitted quotas cannot
be inferred. Different namespaces/domains are independent.

## Fixed policies and per-attempt costs

`Policy` contains `Rate`, `Period` and `Burst`; `Quota` retains its public `Name`
and `Policy` fields. Construct each `Charge` with `q.Charge(cost)`: this
value-receiver method captures a copy of the entire quota and the known cost in
private fields. It performs no validation or I/O; bound admission validates them.
Do not construct charges with field literals.

- `limiter.Bind(quotas...)` validates and copies 1–32 uniquely named quotas locally.
  It performs no backend I/O and does not register or update server-side policies.
  Invalid definitions return an error wrapping `ErrInvalid`.
- A `BoundLimiter` is immutable and safe to share across concurrent calls under the
  parent's driver contract. Changing the original quota slice, quota or policy
  afterward affects neither the binding nor previously constructed charges. Each
  call assembles its own rules without mutating inputs.
- `bound.Allow(ctx, charges...)` and `bound.AllowOnce(ctx, id, retention, charges...)`
  still accept `...Charge`. Each call must supply every bound quota exactly once,
  constructed with `q.Charge(cost)`, with `0 < cost <= Burst`. Missing, duplicate,
  unknown, zero-value or otherwise invalid charges return `ErrInvalid` with
  `NotCharged` before dispatch. There are no default costs or zero-cost exemptions.
- Each charge's captured policy must be fully valid, and its name, effective
  `Interval` (rounded upward to microseconds) and `Burst` must match the binding.
  Different valid `Rate` / `Period` expressions with the same effective interval
  are accepted. An invalid or nonmatching captured policy is a local `ErrInvalid` /
  `NotCharged`, not the backend's `ErrPolicyMismatch`.
- Charge order is arbitrary. The binding order determines `Decision.Limited` and
  the ordered rules used for idempotency. Reordering charges preserves a replay;
  changing the binding order changes the retained attempt's parameters.
- Build quota names from trusted application identity and select policies in
  application code. For dynamic tenants/models, create a binding for the applicable
  set; binding is local and needs no prior registration or mandatory cache.
- To change a policy, create a new binding. Binding does not migrate existing debt
  or bypass `ErrPolicyMismatch` against active backend state. Keep the original
  binding, costs and retention available for retained-attempt recovery.
- `bound.Lookup` delegates to the parent's namespace/domain lookup. It can return
  a receipt created by another binding or the low-level API; it does not filter
  results by the bound quota set.

### Existing low-level API

`Rule`, `Validate`, `limiter.Allow(ctx, rules...)` and
`limiter.AllowOnce(ctx, id, retention, rules...)` remain supported without a
signature, Driver contract or storage/protocol change from the bound API. A `Rule`
combines a quota's name, policy and per-attempt cost. This path checks only the
supplied rules and cannot detect omitted applicable quotas. Bound admissions use the same execution path and quota
state; they add complete-set validation and normalize charges to binding order.
The versioned trusted builder in the [durable example](drivers/redis/examples/durable/README.md)
continues to demonstrate the low-level API.

## PostgreSQL and the Driver boundary

Install `github.com/tr1v3r/pacelock/drivers/postgres@dev` for PostgreSQL.
The application registers a `database/sql` transport and owns a primary DB:

```go
driver, err := postgres.New(db) // import github.com/tr1v3r/pacelock/drivers/postgres
if err != nil { return err }
if err := driver.Migrate(ctx); err != nil { return err } // explicit setup, not per request
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

Both drivers share `Bind`, `Allow`, `Wait`, `AllowOnce`, `Lookup`, outcomes and observers. PostgreSQL
locks one domain row, samples `clock_timestamp()` after locking, and commits every
quota and retained result in one short transaction. Pre-commit failures roll back;
commit errors are `Unknown`. This driver does not join a caller's business transaction.
Domain contention, WAL, pools and vacuum need deployment testing; no throughput
advantage is claimed. `Prune` offers bounded caller-scheduled physical cleanup;
logical expiry works without cleanup. The SDK starts no background workers.

See the [PostgreSQL guide](drivers/postgres/README.md), [Redis guide](drivers/redis/README.md),
and [custom-driver contract](docs/drivers.md). Drivers are alternatives: switching
backends does not transfer quota debt or retained IDs and is not automatic failover.

## Blocking admission

`limiter.Wait(ctx, rules...) error` uses the existing low-level `Rule` API;
there is no `BoundLimiter.Wait`. It tries immediately, sleeps for `RetryAfter`
after ordinary denials, and rechecks every quota atomically until admitted or an
error occurs:

```go
rules := []pacelock.Rule{
    {Name: "account:rpm", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1},
    {Name: "model-a:input-tpm", Rate: 60000, Period: time.Minute, Burst: 12000, Cost: 1500},
}
waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
if err := limiter.Wait(waitCtx, rules...); err != nil {
    // All errors stop waiting. Unknown may have charged: do not blindly retry.
    return err
}
// Quota was consumed once. Execute this business attempt.
```

A nil error confirms admission and consumption; business failure does not refund
it. Cancellation between attempts is `NotCharged`. Cancellation during a backend
call preserves its accounting outcome; a confirmed successful debit returns nil
even if cancellation arrives concurrently. Every backend error stops waiting,
including `NotCharged` errors. Use `errors.Is` and `OutcomeOf` on returned errors;
`Error.Op` is `wait`.

Waiting uses the calling goroutine and a snapshot of the supplied rules. It holds
no backend lock or connection while sleeping and adds no queue or background
worker. `RetryAfter` reserves no capacity: competing callers can win repeatedly,
so there is no FIFO or starvation guarantee. Bound waiting with a context deadline.

`Wait` is non-idempotent and creates no retained ID for `Lookup`. A denied
`AllowOnce` ID replays its original denial, so polling one retained ID cannot
implement blocking admission. Use the explicit recovery flow below when durable
attempt identity is required.

## Idempotent admission and recovery

```go
// Persist an admission ID alongside one business attempt before dispatch.
receipt, err := bound.AllowOnce(ctx, admissionID, 10*time.Minute, charges...)
if err != nil {
    return err
}
// receipt.Decision is the original decision; receipt.Replayed identifies a replay.
// Replayed=true does not authorize executing the upstream request again.
```

After an ambiguous response, use a fresh context to query:

```go
decision, found, err := bound.Lookup(recoveryCtx, admissionID)
```

- An ID is scoped to namespace/domain and binds the **ordered** quota names,
  effective intervals, bursts, costs and retention. Changed parameters return
  `ErrConflict`. Bound calls normalize charges to binding order; low-level calls
  retain supplied rule order. Names must be valid UTF-8 for `AllowOnce`; IDs contain 1–256 bytes.
- Retention is an explicit whole number of milliseconds from 1 ms to 24 h. Choose
  a value covering your recovery window. Replays and queries never extend it.
- Both allow and deny decisions are retained. A denied ID stays denied while
  retained; a later, genuinely new attempt needs a new ID. Replayed `RetryAfter`
  is the original delay, not a countdown or fresh capacity estimate.
- Redis writes quotas and the complete decision in one final MSET, after all
  checks/encoding. New operations do not create Pending. Legacy pending records
  still return `ErrOutcomeUnknown`; do not delete or refund them automatically.
  Redis expiry is logical; schedule bounded physical cleanup for every scope.
  See the [Redis protocol and cleanup guide](drivers/redis/README.md).
- `found=false` does **not** prove no debit. The command may still be in flight,
  or the record may have expired or been lost. Expiry, eviction, deletion and
  failover loss end deduplication. Never reuse an old ID after that boundary.
- Deduplicating admission does not deduplicate upstream execution. Coordinate the
  business attempt separately. A new actual model call needs a new admission ID.

For a complete application-owned flow, see the [durable admission example](drivers/redis/examples/durable/README.md): trusted quota assembly, persisted attempt IDs, restart recovery and explicit handling of unknown upstream execution.

## Errors and observation

Use `errors.Is` for `ErrInvalid`, `ErrPolicyMismatch`, `ErrState`, `ErrConflict`,
`ErrOutcomeUnknown`, `ErrBackend`, and wrapped context/transport errors. Admission
errors expose `*pacelock.Error` with `Op` and `Outcome`:

| Result | Meaning | Application action |
|---|---|---|
| `Allowed=false`, no error | Quota denied; no quota debit | Reject or schedule a new attempt |
| `NotCharged` error | This invocation is known not to debit | Fix input/policy, or handle cancellation |
| `Unknown` error | May have charged; legacy writes can be partial | Query retained ID; use explicit recovery policy |
| `Lookup`: missing | No retained complete result was found | Preserve uncertainty about previous execution |

A `NotCharged` result never clears uncertainty from an earlier call. On **any**
error, ignore the decision. `ErrState` alone does not mean no debit: a malformed
reply after a debit also returns it. Check `OutcomeOf(err)`; it is conservative for
unrecognized errors.

`Validate(rules...)` checks bounds offline. `NewWithOptions` accepts an optional
`Observer` that runs once per `Allow`, `Wait`, `AllowOnce` or `Lookup` call.
`Wait` emits one `wait` event for the whole call, including sleep time; internal
attempts do not emit separate `allow` events. Events expose
operation, duration, result, supplied rule/charge count, replay status, delay and error without
raw IDs or quota names as fields. Callbacks must be fast and concurrency-safe;
panics propagate. Bound calls emit one event including local charge validation
and assembly; `Bind` emits no event. Do not label metrics with error text or user identifiers. See
[runnable observation/recovery example](drivers/redis/examples/idempotency/main.go).

## Quota and operational contracts

- `Rate / Period` is replenishment speed. `Burst` is total instantaneous capacity,
  not extra capacity. `0 < Cost <= Burst`; periods and burst refill horizons are
  at most 24 h. Intervals round upward to microseconds, conservatively reducing
  effective rates at high throughput.
- GCRA is a rate/burst envelope, not an exact fixed/sliding-minute counter. Model
  provider limits explicitly. Known input tokens do not enforce output TPM;
  character, byte and SSE chunk counts are not token counts.
- Effective interval and burst identify a policy. Changing active state returns
  `ErrPolicyMismatch`. This is a deployment safeguard, not configuration storage.
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
  The new Redis protocol uses a single commit command; crash/failover durability
  still depends on deployment.
  Clock jumps, eviction and failover data loss can weaken limits and deduplication.

See [operations and incident recovery](docs/operations.md),
[design and proposed extensions](docs/design.md), and [compatibility](docs/compatibility.md).

## Examples and validation

```sh
docker compose up -d --wait
(cd drivers/redis && REDIS_ADDR=localhost:16379 go run ./examples/llm)
(cd drivers/redis && REDIS_ADDR=localhost:16379 go run ./examples/idempotency)
(cd examples/postgres && go run .)
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres sh scripts/check.sh
(cd drivers/redis/internal/engine && go test -run '^$' -fuzz FuzzInterval -fuzztime 10s .)
(cd drivers/redis/internal/engine && go test -run '^$' -fuzz FuzzRecord -fuzztime 10s .)
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
integration-test modules require Go 1.26+ (the pgx/text floor was 1.25; the
go-redis v9.23 dependency raises it to 1.26). Production applications should
use a maintained Go release.
