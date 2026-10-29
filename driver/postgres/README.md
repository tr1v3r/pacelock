# PostgreSQL driver

[简体中文](README.zh-CN.md)

Independent module: `github.com/tr1v3r/pacelock/driver/postgres`. Targets PostgreSQL
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
    Name: "requests", Policy: pacelock.Policy{Revision: 1, Rate: 60, Period: time.Minute, Burst: 10},
}
tokens := pacelock.Quota{
    Name: "input-tokens", Policy: pacelock.Policy{Revision: 1, Rate: 60000, Period: time.Minute, Burst: 12000},
}
if err := limiter.UpdateQuotas(ctx, rpm, tokens); err != nil { return err }
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tokens.Name, Cost: 1500})
// On error ignore decision and inspect pacelock.OutcomeOf(err).
```

Import `"github.com/tr1v3r/pacelock/driver/postgres"` and the core package. For pgx,
register `_ "github.com/jackc/pgx/v5/stdlib"` in the application and use
`sql.Open("pgx", dsn)`. Pin the transport version in the application. Constructors do
not connect, migrate or close db. Run Migrate explicitly during setup with a
setup-capable role. It creates the current fixed logged tables from schema.sql,
including the separate `pacelock_policies` configuration table. Only the current
schema is supported; no upgrade or historical schema compatibility is guaranteed.
Use a trusted, stable search_path and
consistent schema/primary routing for every caller. Do not use a read-replica pool.

Publish positive-revision policies with `UpdateQuotas`, then call
`Allow(ctx, ...Charge{Name, Cost})` or `Wait(ctx, charges...)`. No local binding or
policy refresh is required. `Limiter` is dynamic-only; `Rule` and `Evaluate` remain
pure calculation APIs, not backend admission entry points.

## Dynamic policies

The driver implements mandatory dynamic `pacelock.Driver`, optional
`pacelock.QuotaDriver` interfaces. The control plane publishes configuration; the data plane sends **only
names and costs** and uses the latest persisted configuration under the scope lock.
No local policy refresh, new binding or SDK worker is needed on admission nodes.
The caller owns polling its configuration source and publishing updates.

```go
// Control plane: publish an authoritative version from your configuration source.
err = limiter.UpdateQuotas(ctx, pacelock.Quota{
    Name: "requests",
    Policy: pacelock.Policy{Revision: 1, Rate: 60, Period: time.Minute, Burst: 10},
})
if err != nil { return err }
// Data plane: the same call automatically uses future published versions.
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: "requests", Cost: 1})
_ = decision
```

Publish positive, monotonically increasing revisions (at most 9007199254740991).
Updates take the same scope lock as admission and validate the complete batch with
`ApplyQuotas` before writing. Only the separate policy table is read or written:
TAT and debt expiry remain untouched, including during pause/resume. Higher revisions
replace configuration; identical revisions with equal effective interval/burst are
read-only successes. Older revisions or same-version conflicts return
`ErrPolicyMismatch`. Debt can exceed a newly reduced capacity and recovers naturally;
quota configuration changes never refill capacity or reset/rescale debt.

Managed admission fails closed with `ErrPolicyMissing` / `NotCharged` if any policy
is absent. Costs above the current burst return `ErrInvalid`, not retryable denial.
A managed `Rate: 0` pauses admission (positive Period/Burst still required):
`ErrPaused` / `NotCharged` is returned before the over-burst check, without a debit
write. Resume by publishing a higher positive-rate revision.

`Limiter.Wait(ctx, charges...)` copies charges once before dispatch and resolves the
latest backend policies atomically on each ordinary attempt, without a policy
snapshot. It retries only valid capacity denials after advisory `RetryAfter`; every
error stops, including pause, missing policy, a new Burst below Cost, transport
errors even when `NotCharged`, `Unknown`, unsupported capabilities and invalid
driver results. Cancellation between attempts is `NotCharged`; in-flight uncertainty
is preserved, and confirmed success stays success despite cancellation. Each attempt
is an independent admission. One
`wait` observation covers the whole call. Updates
do not proactively wake sleepers, so an old RetryAfter may delay using relaxed policy.
Sleeping holds no transaction, lock or connection. No queue, worker, background
goroutine, fairness or refund is added. Use a context deadline; configuration-source
polling/publication remains outside the SDK. See [blocking admission](../../README.md#blocking-admission).

Update failures before commit roll back; commit errors are `Unknown`, meaning the
policy update may have committed, not that any quota was debited. Retry identical
revisions explicitly to resolve uncertainty; a newer update may supersede them.
There are no hidden retries. Cancellation before dispatch causes no mutation.

Run `Migrate` during controlled setup before serving traffic, with no concurrent
setup runners; DDL takes table locks.

## Transactions and failure contract

Each admission uses Read Committed, INSERT ON CONFLICT for a permanent domain guard,
then SELECT FOR UPDATE on that guard. This serializes even initially missing quota
rows. Backend time is sampled once with clock_timestamp after locking.
All quota rows commit together. PostgreSQL uses
the core EvaluateManaged transition; Redis's equivalents run inside Lua.
Managed admission validates original debt fields, then applies current configuration
in memory. Debt rows store interval/burst for integrity checks, not policy authority.

Admission batches policy and debt reads into two SELECTs and, only when all quotas
allow, writes debt with one multi-row upsert. Rows are mapped back to request order
by quota ID; missing policies fail closed and missing debt starts empty. The limit
remains `MaxRules = 32`. Including guard insertion, locking and time sampling,
allowed admission uses six SQL statements and denial five, plus BEGIN/COMMIT,
independent of quota count. Denial performs no policy/debt writes (the permanent
guard may still be inserted). SQL size and parameter count still grow with quotas.
`UpdateQuotas` similarly uses one policy SELECT and at most one changed-row upsert
(five statements when changed, four when identical, plus transaction boundaries).
Identical revisions are not rewritten and an entirely identical update rolls back
its read-only transaction instead of committing. All parameters are scalar
`database/sql` arguments; no array codecs, transport batching API or new dependency
is required. Batching reduces round trips, not domain serialization or lock waits.

Before Commit is sent, failures roll back and are NotCharged. Every Commit error
is conservatively Unknown: quota may already have been debited. There is no
receipt lookup, replay or automatic transaction retry. Retrying is a fresh admission
and can debit again. Application-owned business idempotency is outside the limiter.

## Deployment limits and cleanup

- One domain is one serialization point. Lock/pool waiting may occur; configure
  context deadlines, server statement/lock timeouts and a bounded connection pool.
  Never execute a provider call or sleep for RetryAfter inside the transaction.
- Use logged tables, fsync and a suitable synchronous_commit/replication policy.
  Async commit or failover to a lagging standby may lose debt.
  Wall-clock jumps are not eliminated by PostgreSQL.
- Expired debt is logically ignored, but managed policy identity persists forever.
  Schedule `driver.Prune(ctx, scope, limit)` externally to remove at most limit
  expired debt rows per call (1..10000).
  Both expiry and TAT must be at or before database time, preserving active debt.
  It takes the same domain lock and never touches the policy table.
  Account for persistent policy cardinality. Domain guard rows are
  deliberately retained; plan for domain cardinality. Do not independently delete
  guard rows or delete active state. Tune vacuum for frequently updated quota rows.
- This version owns its transaction and cannot join a business transaction. It
  does not provide automatic state transfer/failover from Redis, billing, queues
  or a throughput/SLO guarantee. Validate latency and hot domains in your deployment.

Use the root Compose fixture and `scripts/check.sh`. `tests/contracts` exercises
both drivers plus real PG rollback, lost-commit-reply accounting, lock cancellation,
post-lock time and cleanup. See [Driver contract](../../docs/driver.md).

[Runnable pgx example](../../examples/postgres/main.go): from the repository root,
run `(cd examples/postgres && go run .)` against the Compose PostgreSQL fixture.

The core, both runtime drivers, the optional pgx example and integration-test
modules require Go 1.26+. Production applications should use a maintained Go
release with its latest patches.

Prune uses an index on `(scope_id, GREATEST(expires_us, tat_us), quota_id)` and
orders candidates by that recovery time, skipping active debt instead of scanning
all quota IDs while holding the domain lock. LIMIT bounds recovered candidates;
it is not a hard latency bound (MVCC visibility, locks and storage still matter).
Run `Migrate` during controlled setup to add the index to an existing current-schema
database. Index creation can block writes and consume disk; schedule it before
serving traffic or provision the equivalent index concurrently outside Migrate.
The extra index adds maintenance cost to debt writes; measure that tradeoff.
