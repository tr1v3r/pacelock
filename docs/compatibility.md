# Compatibility and migration

## Dynamic-default API migration (breaking pre-v1)

- Default `Allow(ctx, ...Charge{Name, Cost})` and `AllowOnce(ctx, id, retention, ...Charge)`
  resolve backend policies. First publish `Quota{Name, Policy{Revision: 1, ...}}` through
  `UpdatePolicies`; request nodes do not refresh policies or carry revisions.
- Former dynamic `AllowManaged` / `AllowManagedOnce` become `Allow` / `AllowOnce`;
  `NamedCharge` becomes public name/cost `Charge`. The old names are removed.
- Former static `Allow` / `AllowOnce` become `AllowStatic` / `AllowStaticOnce`;
  `Wait` becomes `WaitStatic`. `Rule` remains the low-level caller-policy value.
- `Bind` becomes `BindStatic` and returns `StaticLimiter` (formerly `BoundLimiter`).
  Its methods remain `Allow` / `AllowOnce` / `Lookup`; `q.Charge(cost)` becomes
  `q.StaticCharge(cost)` returning encapsulated `StaticCharge`. Static revisions are 0.
- Dynamic `Driver.Admit(Admission)` is mandatory (`ManagedAdmission` → `Admission`).
  Static support moves to optional `StaticDriver.AdmitStatic(StaticAdmission)`;
  `PolicyDriver` is unchanged and `Lookup` is shared. Update custom drivers too.
- There is no default/dynamic Wait or automatic policy watcher. Pure `Evaluate` and
  `EvaluateManaged` remain available; the API rename itself does not reset debt.

For PostgreSQL: **Migrate → upgrade all binaries → activate configuration**. Run
`Migrate` before upgraded binaries serve even static traffic. For either backend,
upgrade every writer/maintenance process before publishing dynamic policies; old
writers can bypass configuration. Mixed activation and unconverted rollback are unsupported.


## Supported contract

The core targets Go 1.26+; independent drivers target Redis 7+ or PostgreSQL 16+. CI configuration tests Go 1.26/stable with
Redis 7.0, the current Redis 7 image and Redis 8. Cluster routing is tested with
three Redis 7 masters. PostgreSQL CI includes 16/17/18 and common Driver contracts. Live results and exact local versions are recorded in
[verification.md](verification.md); configured CI is not evidence of a completed
remote run. Managed Redis variants and Redis-compatible products require their
own compatibility validation.

The Go directive is a source-compatibility floor, not a security support promise
for old toolchains. Production binaries must use a security-maintained Go branch
with current patches. Scanning on an older local Go 1.26.8 identified standard
library advisories [GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809) and
[GO-2026-6607](https://pkg.go.dev/vuln/GO-2026-6607). Their combined fixes require
Go 1.26.10 or Go 1.27.3 on those branches; use later security patches when available.
Security CI uses stable Go. The minimum-version job checks compatibility only.

Use a concurrency-safe go-redis client and primary routing. `redisdriver.New` accepts
`redis.Scripter` for custom integrations, but such integrations must preserve
Redis execution, routing and error semantics. Client ownership remains with the
caller. No background goroutines, queues, implicit retry loop or package-global
quota configuration are introduced.

The public API is pre-v1. Construct a Redis or PostgreSQL `Driver` explicitly,
then pass it to `New` or `NewWithOptions`. The core has no backend dependency.
Typed-nil clients and nil contexts return `ErrInvalid`. Check errors with
`errors.Is/As` and accounting certainty with `OutcomeOf`, not string matching.

### Per-command retry safety (pre-v1)

Concrete `*redis.Client` and `*redis.ClusterClient` instances accept default,
positive and disabled (`-1`) `MaxRetries` budgets. Pacelock wraps its EVALSHA/EVAL
commands with go-redis v9.22.0's `NoRetry` marker, preventing ambiguous replay at
node, Cluster and ASK pipeline layers without disabling retries for other commands
on a shared client. Explicit `MaxRetries: -1` disables retries for other commands too.

Cluster supports default or positive bounded redirect budgets. `MaxRedirects: -1`
still disables redirects; only that opt-out loses transparent MOVED/ASK recovery.
NOSCRIPT still safely falls back to EVAL. Unsupported negative budgets remain
invalid. Both constructors inspect normalized options without connecting,
mutating settings or taking client ownership. Do not mutate client options later.
Custom transports, hooks, factories and proxies must preserve the no-replay
contract. See [operations](operations.md#client-retries-and-cluster-availability).
Preserve `Unknown` and do not blindly replay `Allow`.

This is a development-line safety change, not a stable-release announcement.
The README's explicit `@dev` installation selects that line; retain its resolved
pseudo-version for reproducible builds. Default `@latest` currently selects an
older API. A stable tag remains a separate, explicitly authorized release decision.

## Redis state

Keys use a scope hash tag and separate quota/receipt suffixes:

```text
pacelock:{domain_digest}:quota_digest
pacelock:{domain_digest}:attempt:sha256(admission_id)
```

The state formats are:

```text
TAT_microseconds:effective_interval_microseconds:Burst (static debt)
D3|TAT_microseconds (dynamic debt)
P3|effective_interval_microseconds:Burst:Revision (permanent dynamic configuration)
expires_at_microseconds|fingerprint|{"names":[...],"values":[allowed,retry_us,blocked_flags...]}
```

Debt logically expires when `tat <= now`, regardless of native TTL. Active static
quotas require matching effective interval and burst; dynamic policy identity never
expires. Complete-result expiry equals admission Redis time plus retention.
One MSET commits admitted quotas and the complete receipt, clearing native TTLs;
physical removal requires explicit bounded cleanup. Replays and lookups do not
change either deadline. Static fingerprints use `[retention_ms,step,burst,cost,name,...]`,
without an empty placeholder. Dynamic fingerprints bind ordered names/costs and
retention, not revisions, with a distinct managed-admission-v1 discriminator.
Removed Q2/E2/C1/P1 formats are not accepted.

Malformed receipts return `ErrState` / `Unknown`, never missing or permission to
admit again. Prune parses candidates by key role, validates the entire batch before
one DEL, removes only expired debts/receipts, and permanently retains valid policy
configuration. Malformed or wrong-role candidates fail without deleting anything.

These are internal formats, not application APIs. See the bilingual Redis guides
for cleanup cursor semantics, ACLs and operational limits.

## Policy changes

Default dynamic policy changes use `UpdatePolicies` with monotonically increasing
positive revisions (maximum 9007199254740991). Equivalent same-revision publications
are read-only successes; stale/conflicting publications return `ErrPolicyMismatch`.
Updates preserve absolute TAT without rescaling, reset or debt-expiry shortening.
`Rate=0` pauses; resume or roll back parameters at a higher revision. Retained
receipts replay before current configuration checks. Missing policy fails closed,
and static admission cannot bypass configured policies.

For the explicit Revision-0 static alternative, active policy mismatch rejects
before quota mutation. Coordinate static policy changes
across callers. Either wait for the old quota to replenish and expire while
coordinating traffic, or design an explicit debt-preserving migration outside this
SDK. Renaming a quota/domain creates fresh capacity; deleting keys silently resets
debt. Neither is a transparent policy update.

## Failure and replay safety

`Allow` has no attempt ID and remains unsafe to replay after an ambiguous failure.
`AllowOnce` returns the original decision while its record survives. A complete
record is useful evidence of that admission, but says nothing about whether the
upstream business request was executed. A malformed receipt, missing record, lost
replication or expired ID is not evidence that capacity was never charged.

Use separate IDs for admission transport recovery and for distinct business
attempts. Implement business execution coordination outside this limiter. This
SDK does not provide exactly-once execution, crash-proof quota accounting, refund,
output reservation or billing. Those are not implied by a successful release.

The core, both runtime drivers, the optional pgx example and the
integration-test modules require Go 1.26+. Production applications should
use a maintained Go release.
