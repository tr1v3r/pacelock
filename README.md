# Pacelock

[![CI](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml)
[简体中文](README.zh-CN.md)

**Distributed rate limiting for LLM APIs in Go.**

Pacelock uses Redis and weighted GCRA to enforce shared quotas across service instances. A single admission attempt can check request and token quotas together: either every dimension passes and is charged, or the request is denied without charging any dimension.

This is an initial implementation with an experimental API, not a complete LLM gateway. It requires Go 1.24+ and Redis 7+. Redis 7 is exercised in CI; Redis Cluster uses colocated keys by construction but is not yet covered by cluster integration tests.

## What works today

- Weighted GCRA: charge one request or a known number of tokens.
- Atomic admission across up to 32 named quotas per call.
- Shared state across processes, Redis server time, and automatic idle-state expiry.
- `Allowed`, advisory `RetryAfter`, and all blocking quota names.
- Validation of oversized charges and active-state policy mismatches.

There is no queue, blocking `Wait`, concurrency limiter, tokenization, reservation, refund, streaming reconciliation, or idempotent replay in this version. [Airlock](https://github.com/tr1v3r/airlock) handles concurrency admission; Pacelock handles consumption over time. Combining the two does not make their operations atomic.

## Install

```sh
go get github.com/tr1v3r/pacelock
```

## Use

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
decision, err := limiter.Allow(ctx,
    pacelock.Rule{
        Name: "model-a:rpm", Rate: 60, Period: time.Minute,
        Burst: 10, Cost: 1,
    },
    pacelock.Rule{
        Name: "model-a:input-tpm", Rate: 60000, Period: time.Minute,
        Burst: 12000, Cost: 1500,
    },
)
if err != nil {
    return err // Backend failure; debit outcome may be unknown.
}
if !decision.Allowed {
    // Reject or schedule a later attempt using RetryAfter and Limited.
    // RetryAfter does not reserve capacity.
}
```

See the [runnable example](examples/llm/main.go):

```sh
REDIS_ADDR=localhost:6379 go run ./examples/llm
```

All instances enforcing the same quotas must use the same namespace, domain, quota names, and effective policies. Include shared provider and tenant quotas in each call when both apply; the library cannot infer omitted rules. Different namespaces/domains are independent. The example only charges known input tokens; it does not enforce output TPM or automatically match a provider's accounting rules.

## Semantics and operational limits

- `Rate / Period` is the replenishment rate. `Burst` is total instantaneous capacity, not extra capacity. Costs must be positive and no larger than the burst.
- GCRA is a rate/burst envelope, not an exact fixed or sliding minute counter. Configure it to fit the provider's actual quota contract.
- Emission intervals round upward to whole microseconds. Period and burst refill horizon are limited to 24 hours. Very high rates may be conservatively rounded down in effect.
- Quota policy identity is the effective interval plus burst. A mismatch while state exists returns `ErrPolicyMismatch`; this is a deployment safeguard, not permanent configuration storage. State expires when fully replenished.
- A domain maps all its keys to one Redis Cluster slot. Choose domains around atomicity needs; putting everything in one domain concentrates load on one shard.
- Disable automatic client retries (`MaxRetries: -1`). Pacelock only falls back from `EVALSHA` to `EVAL` on `NOSCRIPT`. Middleware, hooks, proxies, and cluster routing may introduce additional replay behavior; admission is **not idempotent**.
- Cancellation before dispatch does not debit. A lost response, timeout, or cancellation after dispatch may leave an unknown outcome. Do not blindly retry or refund it. On any error, ignore the decision and apply an explicit application failure policy.
- Use dedicated quota keys and Redis `noeviction`; eviction, manual deletion, clock jumps, or failover data loss can weaken limits. Script isolation is not crash durability or rollback of runtime errors. This library is not a billing ledger.

## Design and roadmap

The [design document (Chinese)](docs/design.md) explains the GCRA equation, Redis state, atomicity boundary, errors, and proposed LLM extensions:

1. Joint request/token quotas — implemented for known costs.
2. Output-token reservation and usage reconciliation — proposed.
3. Idempotent admission and explicit attempt identities — proposed.

## Test

```sh
go test ./...
go vet ./...
# Real Redis tests are skipped unless this variable is set.
PACELOCK_REDIS_ADDR=localhost:6379 go test -race -count=1 ./...
```

CI runs these integration tests against Redis 7 with Go 1.24 and stable Go. Tests cover multidimensional rejection, weighted contention across clients, policy mismatch, corrupt state, expiry, namespace isolation, and cancellation before dispatch.

MIT licensed.
