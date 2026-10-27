# PostgreSQL driver

[简体中文](README.zh-CN.md)

Independent module: `github.com/tr1v3r/pacelock/drivers/postgres`. Targets PostgreSQL
16+. Runtime dependencies are the standard library and the Pacelock core. The
application chooses/registers its database/sql transport; shared tests use pgx.

```go
// db is an initialized *sql.DB pointing at the authoritative primary.
driver, err := postgres.New(db)
if err != nil { return err }
if err := driver.Migrate(setupCtx); err != nil { return err }
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
if err != nil { return err }
rpm := pacelock.Quota{
    Name: "requests", Policy: pacelock.Policy{Rate: 60, Period: time.Minute, Burst: 10},
}
tokens := pacelock.Quota{
    Name: "input-tokens", Policy: pacelock.Policy{Rate: 60000, Period: time.Minute, Burst: 12000},
}
bound, err := limiter.Bind(rpm, tokens)
if err != nil { return err }
decision, err := bound.Allow(ctx, rpm.Charge(1), tokens.Charge(1500))
// On error ignore decision and inspect pacelock.OutcomeOf(err).
```

Import `"github.com/tr1v3r/pacelock/drivers/postgres"` and the core package. For pgx,
register `_ "github.com/jackc/pgx/v5/stdlib"` in the application and use
`sql.Open("pgx", dsn)`. Pin the transport version in the application. Constructors do
not connect, migrate or close db. Run Migrate explicitly during setup with a
migration-capable role. It creates the fixed logged tables from schema.sql; it does
not upgrade incompatible existing tables. Use a trusted, stable search_path and
consistent schema/primary routing for every caller. Do not use a read-replica pool.

The common `Bind` API validates and copies fixed quotas locally. Construct charges
with `q.Charge(cost)`, a value-receiver method that captures the complete quota and
cost in private fields without validation or I/O; do not use charge field literals.
`Quota.Name` / `Policy` remain public. Later quota mutation affects neither existing
charges nor bindings; policy changes require a new binding. `Allow` / `AllowOnce`
still take `...Charge`, one positive cost per bound quota exactly once on every call.
Bind all applicable quotas; charge order is normalized to binding order. Each
captured policy must be fully valid, and its name, effective interval (rounded upward
to microseconds) and burst must match the binding. Equivalent valid policy
expressions are accepted. Invalid, incomplete or mismatched charges return local
`ErrInvalid` / `NotCharged` before admission, not backend `ErrPolicyMismatch`.
Existing Rule APIs, Driver and storage are unchanged; blocking admission still
uses `limiter.Wait` with rules, not a bound wait method.
See [policy and cost contracts](../../README.md#fixed-policies-and-per-attempt-costs).

## Transactions and failure contract

Each admission uses Read Committed, INSERT ON CONFLICT for a permanent domain guard,
then SELECT FOR UPDATE on that guard. This serializes even initially missing quota
and receipt rows. Backend time is sampled once with clock_timestamp after locking.
All quota rows and a retained allow/deny result commit together. PostgreSQL uses
the core Evaluate transition; Redis's equivalent runs inside Lua.

Before Commit is sent, failures roll back and are NotCharged. Every Commit error
is conservatively Unknown; recover by retained ID with a fresh context. There is
no automatic transaction retry. Lookup does not lock/wait for an in-flight admission,
so missing does not prove zero debit. Replays and queries never extend retention.
Upstream execution still needs application coordination.

## Deployment limits and cleanup

- One domain is one serialization point. Lock/pool waiting may occur; configure
  context deadlines, server statement/lock timeouts and a bounded connection pool.
  Never execute a provider call or sleep for RetryAfter inside the transaction.
- Use logged tables, fsync and a suitable synchronous_commit/replication policy.
  Async commit or failover to a lagging standby may lose debt and retained IDs.
  Wall-clock jumps are not eliminated by PostgreSQL.
- Expired rows are logically ignored. Schedule `driver.Prune(ctx, scope, limit)`
  externally to remove at most limit rows from each data table per call (1..10000).
  It takes the same domain lock and preserves active debt. Domain guard rows are
  deliberately retained; plan for domain cardinality. Do not independently delete
  guard rows or delete active state. Tune vacuum for frequently updated quota rows.
- This version owns its transaction and cannot join a business transaction. It
  does not provide automatic state transfer/failover from Redis, billing, queues
  or a throughput/SLO guarantee. Validate latency and hot domains in your deployment.

Use the root Compose fixture and `scripts/check.sh`. `tests/contracts` exercises
both drivers plus real PG rollback, lost-commit-reply recovery, lock cancellation,
post-lock time and cleanup. See [Driver contract](../../docs/drivers.md).

[Runnable pgx example](../../examples/postgres/main.go): from the repository root,
run `(cd examples/postgres && go run .)` against the Compose PostgreSQL fixture.

Retention starts at the sampled backend decision time; SQL/commit latency consumes
part of that window. Choose retention to cover dispatch and recovery latency.

The core, both runtime drivers, the optional pgx example and integration-test
modules require Go 1.26+. Production applications should use a maintained Go
release with its latest patches.
