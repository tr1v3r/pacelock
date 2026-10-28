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
if err := limiter.UpdatePolicies(ctx, rpm, tokens); err != nil { return err }
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tokens.Name, Cost: 1500})
// On error ignore decision and inspect pacelock.OutcomeOf(err).
```

Import `"github.com/tr1v3r/pacelock/driver/postgres"` and the core package. For pgx,
register `_ "github.com/jackc/pgx/v5/stdlib"` in the application and use
`sql.Open("pgx", dsn)`. Pin the transport version in the application. Constructors do
not connect, migrate or close db. Run Migrate explicitly during setup with a
migration-capable role. It creates the fixed logged tables from schema.sql and
adds a separate `pacelock_policies` configuration table. The existing quota/debt
and receipt schemas are unchanged, retaining all debt and receipts. Incompatible
schemas are not supported. Use a trusted, stable search_path and
consistent schema/primary routing for every caller. Do not use a read-replica pool.

By default publish positive-revision policies with `UpdatePolicies`, then call `Allow(ctx, ...Charge{Name, Cost})` or `AllowOnce`; no local binding or policy refresh is required. The explicit static alternative is `BindStatic` → `StaticLimiter`: construct encapsulated `StaticCharge` values with `q.StaticCharge(cost)` and call its `Allow` / `AllowOnce`. Low-level caller-policy admission uses `Rule` with `AllowStatic` / `AllowStaticOnce` / `WaitStatic`, and requires Revision 0. There is no default dynamic Wait.

## Default dynamic policies

The driver implements mandatory dynamic `pacelock.Driver`, optional
`pacelock.PolicyDriver` and `pacelock.StaticDriver` interfaces. The control plane publishes configuration; the data plane sends **only
names and costs** and uses the latest persisted configuration under the scope lock.
No local policy refresh, new binding or SDK worker is needed on admission nodes.
The caller owns polling its configuration source and publishing updates.

```go
// Control plane: publish an authoritative version from your configuration source.
err = limiter.UpdatePolicies(ctx, pacelock.Quota{
    Name: "requests",
    Policy: pacelock.Policy{Revision: 1, Rate: 60, Period: time.Minute, Burst: 10},
})
if err != nil { return err }
// Data plane: the same call automatically uses future published versions.
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: "requests", Cost: 1})
// Or retain a decision; the ID fingerprint includes names/costs, not policy versions.
receipt, err := limiter.AllowOnce(ctx, "operation-id", time.Minute,
    pacelock.Charge{Name: "requests", Cost: 1})
_ = decision
_ = receipt
```

Publish positive, monotonically increasing revisions (at most 9007199254740991).
Updates take the same scope lock as admission and validate the complete batch with
`ApplyPolicies` before writing. Only the separate policy table is read or written:
TAT and debt expiry remain untouched, including during pause/resume. Higher revisions
replace configuration; identical revisions with equal effective interval/burst are
read-only successes. Older revisions or same-version conflicts return
`ErrPolicyMismatch`. Debt can exceed a newly reduced capacity and recovers naturally;
changing configuration never resets it.

Managed admission fails closed with `ErrPolicyMissing` / `NotCharged` if any policy
is absent. Costs above the current burst return `ErrInvalid`, not retryable denial.
A managed `Rate: 0` pauses admission (positive Period/Burst still required):
`ErrPaused` / `NotCharged` is returned before the over-burst check, without a debit
or receipt write. Resume by publishing a higher positive-rate revision. There is
no managed Wait API; caller retry/config polling remains outside the SDK.

Existing retained receipts replay **before** current policy checks, unchanged by
pauses or updates. Managed retries with the same names/costs replay across revisions.
Static and managed requests sharing an ID conflict. A replayed historical allow is
not permission to execute upstream again. Static Rule/BindStatic requests reject managed
revision fields locally, and new static admissions to configured quotas return
`ErrPolicyMismatch`; old static receipts still replay before this bypass guard.

Update failures before commit roll back; commit errors are `Unknown`, meaning the
policy update may have committed, not that any quota was debited. Retry identical
revisions explicitly to resolve uncertainty; a newer update may supersede them.
There are no hidden retries. Cancellation before dispatch causes no mutation.

**Rollout order: migrate → upgrade every binary → activate managed policies.**
Run `Migrate` **before upgraded request-serving binaries handle any traffic, even
static-only traffic**: new static admission queries the policy table to prevent
bypass. Migration only adds the table, so old binaries can continue serving static
traffic while binaries are upgraded, provided no managed policies are activated.
Once every binary sharing the database is upgraded, publish managed configuration.
Old PostgreSQL code cannot coexist with active dynamic policies: it does not check
the separate configuration table and can bypass managed limits. Run Migrate during
controlled setup with no concurrent migration runners; DDL takes table locks.

## Transactions and failure contract

Each admission uses Read Committed, INSERT ON CONFLICT for a permanent domain guard,
then SELECT FOR UPDATE on that guard. This serializes even initially missing quota
and receipt rows. Backend time is sampled once with clock_timestamp after locking.
All quota rows and a retained allow/deny result commit together. PostgreSQL uses
the core Evaluate / EvaluateManaged transitions; Redis's equivalents run inside Lua.
Managed admission validates original debt fields, then applies current configuration
in memory. Debt rows store interval/burst for integrity checks, not policy authority.

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
- Expired debt is logically ignored, but managed policy identity persists forever.
  Schedule `driver.Prune(ctx, scope, limit)` externally to remove at most limit
  expired debt rows (static or managed) and limit expired receipts per call (1..10000).
  It takes the same domain lock and never touches the policy table.
  Account for persistent policy cardinality. Domain guard rows are
  deliberately retained; plan for domain cardinality. Do not independently delete
  guard rows or delete active state. Tune vacuum for frequently updated quota rows.
- This version owns its transaction and cannot join a business transaction. It
  does not provide automatic state transfer/failover from Redis, billing, queues
  or a throughput/SLO guarantee. Validate latency and hot domains in your deployment.

Use the root Compose fixture and `scripts/check.sh`. `tests/contracts` exercises
both drivers plus real PG rollback, lost-commit-reply recovery, lock cancellation,
post-lock time and cleanup. See [Driver contract](../../docs/driver.md).

[Runnable pgx example](../../examples/postgres/main.go): from the repository root,
run `(cd examples/postgres && go run .)` against the Compose PostgreSQL fixture.

Retention starts at the sampled backend decision time; SQL/commit latency consumes
part of that window. Choose retention to cover dispatch and recovery latency.

The core, both runtime drivers, the optional pgx example and integration-test
modules require Go 1.26+. Production applications should use a maintained Go
release with its latest patches.
