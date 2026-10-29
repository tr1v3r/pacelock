# Compatibility and migration

## Dynamic-default API migration (breaking pre-v1)

- `New` / `NewWithOptions` construct dynamic `Limiter`; publish versioned `Quota`
  values with `UpdateQuotas` before name/cost admission.
- `NewStatic` / `NewStaticWithOptions` independently construct `StaticLimiter`,
  validating/copying fixed Revision-0 quotas without backend I/O.
- Both expose `Allow(ctx, ...Charge) (Decision, error)` and `Wait(ctx, ...Charge) error`.
  Only the dynamic limiter exposes `UpdateQuotas`. Static calls select any nonempty
  subset; omitted quotas are not checked or charged. Selected rules use binding order.
- Removed: `AllowOnce`, `Lookup`, `Receipt`, `LookupRequest`, admission
  `ID`/`Retention`/`Fingerprint`, and `Event.Replayed`. There are no compatibility
  aliases or retained-result recovery methods. Every admission can consume again.
- Custom drivers must return `(Decision, error)` from `Driver.Admit` and optional
  `StaticDriver.AdmitStatic`. `Admission` has only `Scope` + `Charges`;
  `StaticAdmission` has only `Scope` + `Rules`. Remove lookup implementations from
  the required interface and all receipt/replay processing.
- Earlier renames still apply: `AllowManaged` → `Allow`, `NamedCharge` → `Charge`,
  `UpdatePolicies` → `UpdateQuotas`, `PolicyDriver` → `QuotaDriver`,
  `PolicyUpdate` → `QuotaUpdate`. Replace `Bind` / `BindStatic` and rule-taking
  limiter methods with standalone static construction. `Rule`, `Policy`, `Validate`,
  `Evaluate`, `EvaluateManaged` and `ApplyPolicies` remain pure-helper vocabulary.
- Dynamic Wait is preserved: snapshot charges once, resolve the latest backend
  policy on every attempt, retry only confirmed capacity denials after RetryAfter.
  Every error stops, including Unknown and transport errors classified NotCharged.
  Cancellation between attempts is NotCharged; in-flight certainty and confirmed
  success are preserved. One event covers the whole call. No queue, worker, fairness,
  capacity reservation, refund or configuration watcher is added.

**Do not blindly retry Unknown.** A mutation may already have committed, and there
is no SDK recovery API to determine its result. Admission is not business execution
coordination, exactly-once delivery, billing or a refund ledger.

### Existing installations and legacy receipts

The API removal does not reset quota debt or automatically drop legacy receipt
storage. PostgreSQL `Migrate` preserves existing legacy admissions tables/rows;
new installations need no receipt table. Old Redis attempt keys may remain and
must not be assumed to disappear through current quota maintenance. Retire old
writers first, back up as required, and explicitly plan operator-owned cleanup
of obsolete receipt storage without deleting active debt or permanent policy fences.
Do not run old receipt-writing clients alongside new clients expecting deduplication.

For PostgreSQL: **Migrate → upgrade all binaries → activate configuration**. Run
Migrate before upgraded binaries serve even static traffic. Upgrade every writer
and maintenance process before publishing dynamic policies; old writers can bypass
configuration. Mixed activation and unconverted rollback are unsupported.

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
then pass it to `New` / `NewWithOptions` for dynamic admission or `NewStatic` /
`NewStaticWithOptions` for fixed policies. The core has no backend dependency.
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

Keys use scope hash tags so jointly admitted quotas share one Cluster slot.
Static debt uses `tat:interval:burst`, dynamic debt uses `D3|tat`, and permanent
configuration uses `P3|interval:burst:revision`. These are internal formats, not
application APIs; see the [Redis guide](../driver/redis/README.md).

Debt logically expires at `tat <= now`, independently of physical cleanup. A
single MSET commits admitted quota debt. Current admission does not read or write
legacy attempt records. Prune must preserve permanent configuration and revision
fences; follow the driver's current cleanup contract rather than old receipt-era tools.

## Policy changes

Default dynamic policy changes use `UpdateQuotas` with monotonically increasing
positive revisions (maximum 9007199254740991). Equivalent same-revision publications
are read-only successes; stale/conflicting publications return `ErrPolicyMismatch`.
Updates preserve absolute TAT without rescaling, reset or debt-expiry shortening.
`Rate=0` pauses; resume or roll back parameters at a higher revision. Missing policy fails closed,
and static admission cannot bypass configured policies.

For the explicit Revision-0 static alternative, active policy mismatch rejects
before quota mutation. Coordinate static policy changes
across callers. Either wait for the old quota to replenish and expire while
coordinating traffic, or design an explicit debt-preserving migration outside this
SDK. Renaming a quota/domain creates fresh capacity; deleting keys silently resets
debt. Neither is a transparent policy update.

## Failure and retry safety

`Allow` is unsafe to repeat after an ambiguous failure. `Wait` never retries an
error. A later NotCharged result does not resolve an earlier Unknown result.
Apply an explicit application failure policy without silently clearing or refunding
debt. The SDK provides no result recovery, exactly-once business execution, billing,
refunds, output reservation or worker/queue ownership.
