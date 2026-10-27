# Redis driver

[简体中文](README.zh-CN.md)

Independent module: `github.com/tr1v3r/pacelock/drivers/redis`. Requires Redis 7+.
Uses go-redis v9; applications own the initialized Client/ClusterClient and close it.

```go
driver, err := redisdriver.New(client)
if err != nil { return err }
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

Import `redisdriver "github.com/tr1v3r/pacelock/drivers/redis"` and
`"github.com/tr1v3r/pacelock"`. Constructors do not connect or change client options.
One domain is one Cluster slot. All instances need matching scope, names and
effective policies. Each call must include every applicable quota.

Use `limiter.Bind(quotas...)` to copy fixed policies locally, then call
`bound.Allow(ctx, charges...)` or `bound.AllowOnce(ctx, id, retention, charges...)`.
Every call must charge all bound quotas exactly once; order is normalized to binding
order. See [policy and cost contracts](../../README.md#fixed-policies-and-per-attempt-costs).

The v1 Lua script, key hashes and retained record protocol remain unchanged.
Concrete clients receive per-command NoRetry while retaining bounded MOVED/ASK
redirects; Ring is unsupported. Custom Scripters/hooks/factories/proxies must
preserve no-replay and primary-routing behavior. Runtime script errors can leave
partial writes: inspect OutcomeOf, recover retained IDs, and never blindly refund.

Use a primary, TLS/authentication, controlled ACLs, noeviction and explicit durability
settings. See [operations](../../docs/operations.md), [core usage](../../README.md)
and [Driver contract](../../docs/drivers.md). No automatic PG failover/state migration.

Run examples from this module: `REDIS_ADDR=localhost:16379 go run ./examples/llm`.
Full root verification is `sh scripts/check.sh`; backend-specific tests are under
`internal/engine` and retain concurrency, Cluster, ACL, model and transport coverage.
