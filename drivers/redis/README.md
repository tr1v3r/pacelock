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

`Wait` and `WaitOnce` are core wrappers over immediate driver operations.
`AdmitWaitOnce` implements success-only retention: denials write nothing, successes
use the existing v1 pending/complete records, and retained AllowOnce denials
conflict. Success replay/Lookup do not charge or extend retention. Key hashes,
record encoding, and ordinary Allow/AllowOnce arguments remain compatible; the
Lua script adds an optional trailing mode argument for success-only retention.
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
