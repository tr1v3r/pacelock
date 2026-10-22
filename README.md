# Pacelock

[![CI](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml)
[简体中文](README.zh-CN.md)

**Atomic, weighted, multidimensional rate admission for Go and Redis.**

Pacelock coordinates shared quotas across service instances. One admission can
charge request count, known input tokens and tenant capacity together: all pass
and are charged, or a quota denial charges none. LLM APIs are a primary use case;
the core accepts any known positive cost.

The API is pre-v1. This SDK is a rate-admission component, with explicit failure
contracts and bounded idempotency; it does not implement an LLM gateway or billing
ledger. Requires Go 1.24+ and Redis 7+ to compile/run. Production builds should use a currently security-maintained Go release with its latest patches. See [compatibility](docs/compatibility.md)
and the [verification record](docs/verification.md) for tested configurations.

## Capabilities

- Weighted GCRA and atomic admission across 1–32 named quotas.
- Redis server time, shared state and automatic expiry when capacity replenishes.
- Immediate decisions, advisory `RetryAfter` and all blocking quota names.
- Bounded idempotent admission with parameter conflict detection and result lookup.
- Typed accounting outcomes, offline rule validation and synchronous observation.
- Real Redis/Cluster, race, model, fuzz and failure-injection tests; benchmarks.

There is no queue, blocking `Wait`, tokenization, output reservation, refund,
streaming reconciliation or concurrency ownership. [Airlock](https://github.com/tr1v3r/airlock)
handles concurrent work; Pacelock handles consumption over time. Their operations
are not a joint transaction.

## Install and use

```sh
go get github.com/tr1v3r/pacelock
```

```go
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    MaxRetries: -1,
    ContextTimeoutEnabled: true,
})
defer client.Close()

limiter, err := pacelock.New(client, "llm-service", "provider-account-a")
if err != nil {
    return err
}
rules := []pacelock.Rule{
    {Name: "account:rpm", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1},
    {Name: "model-a:input-tpm", Rate: 60000, Period: time.Minute, Burst: 12000, Cost: 1500},
}
decision, err := limiter.Allow(ctx, rules...)
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
failure after admission does not refund capacity. The limiter neither owns nor
closes the Redis client. Constructors do not connect or change client settings.

Every instance must use the same namespace, domain, quota names and effective
policies. Include all applicable account/model/tenant quotas in every call; omitted
rules cannot be inferred. Different namespaces/domains are independent.

## Idempotent admission and recovery

```go
// Persist an admission ID alongside one business attempt before dispatch.
receipt, err := limiter.AllowOnce(ctx, admissionID, 10*time.Minute, rules...)
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

- An ID is scoped to namespace/domain and binds the **ordered** quota names,
  effective intervals, bursts, costs and retention. Changed parameters return
  `ErrConflict`. Names must be valid UTF-8 for `AllowOnce`; IDs contain 1–256 bytes.
- Retention is an explicit whole number of milliseconds from 1 ms to 24 h. Choose
  a value covering your recovery window. Replays and queries never extend it.
- Both allow and deny decisions are retained. A denied ID stays denied while
  retained; a later, genuinely new attempt needs a new ID. Replayed `RetryAfter`
  is the original delay, not a countdown or fresh capacity estimate.
- A pending marker is written before quota mutations. A runtime error may leave
  partial debits and a pending record; retries then return `ErrOutcomeUnknown`
  without debiting again. Do not delete the marker or refund automatically.
- `found=false` does **not** prove no debit. The command may still be in flight,
  or the record may have expired or been lost. Expiry, eviction, deletion and
  failover loss end deduplication. Never reuse an old ID after that boundary.
- Deduplicating admission does not deduplicate upstream execution. Coordinate the
  business attempt separately. A new actual model call needs a new admission ID.

## Errors and observation

Use `errors.Is` for `ErrInvalid`, `ErrPolicyMismatch`, `ErrState`, `ErrConflict`,
`ErrOutcomeUnknown`, `ErrBackend`, and wrapped context/transport errors. Admission
errors expose `*pacelock.Error` with `Op` and `Outcome`:

| Result | Meaning | Application action |
|---|---|---|
| `Allowed=false`, no error | Quota denied; no quota debit | Reject or schedule a new attempt |
| `NotCharged` error | This invocation is known not to debit | Fix input/policy, or handle cancellation |
| `Unknown` error | May have charged, including partial writes | Query retained ID; use explicit recovery policy |
| `Lookup`: missing | No retained complete result was found | Preserve uncertainty about previous execution |

A `NotCharged` result never clears uncertainty from an earlier call. On **any**
error, ignore the decision. `OutcomeOf` is conservative for unrecognized errors.

`Validate(rules...)` checks bounds offline. `NewWithOptions` accepts an optional
`Observer` that runs once per `Allow`, `AllowOnce` or `Lookup` call. Events expose
operation, duration, result, rule count, replay status, delay and error without
raw IDs or quota names as fields. Callbacks must be fast and concurrency-safe;
panics propagate. Do not label metrics with error text or user identifiers. See
[runnable observation/recovery example](examples/idempotency/main.go).

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
- Disable ordinary client retries (`MaxRetries: -1`). Only `NOSCRIPT` falls back
  from `EVALSHA` to `EVAL`. Audit hooks, proxies and Cluster redirects separately.
- Pre-dispatch cancellation does not debit; cancellation after dispatch cannot
  undo Redis writes. The SDK creates no goroutines and imposes no hidden timeout.
- Use dedicated keys, primary routing, TLS/authentication and Redis `noeviction`.
  Lua execution isolation is not runtime-error rollback or crash durability.
  Clock jumps, eviction and failover data loss can weaken limits and deduplication.

See [operations and incident recovery](docs/operations.md),
[design and proposed extensions](docs/design.md), and [compatibility](docs/compatibility.md).

## Examples and validation

```sh
# Disposable standalone Redis and three-master Cluster (requires Docker Compose).
docker compose up -d --wait
REDIS_ADDR=localhost:16379 go run ./examples/llm
REDIS_ADDR=localhost:16379 go run ./examples/idempotency

go test ./...
go vet ./...
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
  go test -race -count=1 -cover ./...
# Dedicated standalone test Redis must allow ACL user creation for fault tests.
# Unset integration variables means integration tests are skipped.
go test -run '^$' -fuzz FuzzInterval -fuzztime 10s .
go test -run '^$' -fuzz FuzzRecord -fuzztime 10s .
PACELOCK_REDIS_ADDR=localhost:16379 go test -run '^$' -bench . -benchmem .
docker compose down
```

CI covers Go 1.24/stable, Redis 7.0/7/8, and a three-master Cluster. Benchmark
results depend on deployment; there is no universal throughput or failover SLO.
See [contribution/release checks](CONTRIBUTING.md) and [security policy](SECURITY.md).

MIT licensed.
