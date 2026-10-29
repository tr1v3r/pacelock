# Compatibility and current contract

## Current API

`New` / `NewWithOptions` construct the dynamic `Limiter`. Publish revisioned
`Quota` values with `UpdateQuotas`, then call `Allow` or `Wait` with named positive
`Charge` costs. `Driver.Admit` receives only `Scope` and `Charges`; optional
`QuotaDriver` publishes configuration. Pure math helpers do not admit requests.

Admission is non-idempotent, with no retained results or recovery API. `Wait` retries
only valid capacity denials; every error stops. **Do not blindly retry Unknown.**
There are no upgrade, migration, mixed-version or historical storage compatibility
guarantees. Historical verification documents are archives, not current contracts.

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
then pass it to `New` / `NewWithOptions` for dynamic admission. The core has no backend dependency.
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
The only accepted debt format is `D1|tat`; the only accepted policy format is
`P1|interval:burst:revision`, with exact field counts and validated decimal fields.
Marker `1` identifies the first dynamic format, not `Policy.Revision`.
Unknown, malformed or wrong-role stored formats fail closed before mutation.
These encodings are internal, not application APIs; see the
[Redis guide](../driver/redis/README.md).

Debt logically expires at `tat <= now`. Prune deletes only valid, fully recovered
D1 debt, never policy keys or unknown keys. Configuration and revision fences persist.
Applications must not reset, rename or delete active quota state to obtain capacity.

## Policy changes

Default dynamic policy changes use `UpdateQuotas` with monotonically increasing
positive revisions (maximum 9007199254740991). Equivalent same-revision publications
are read-only successes; stale/conflicting publications return `ErrPolicyMismatch`.
Updates preserve absolute TAT without rescaling, reset or debt-expiry shortening.
`Rate=0` pauses; resume or roll back parameters at a higher revision. Missing policy fails closed,
with `ErrPolicyMissing`.

Configuration can remain in code, while execution is backend-authoritative.
Every request uses the latest published policy. Explicitly handle stale publication
errors; restoring earlier parameters requires a higher revision. Renaming quotas
or deleting state creates fresh capacity, not a policy update.

## Failure and retry safety

`Allow` is unsafe to repeat after an ambiguous failure. `Wait` never retries an
error. A later NotCharged result does not resolve an earlier Unknown result.
Apply an explicit application failure policy without silently clearing or refunding
debt. The SDK provides no result recovery, exactly-once business execution, billing,
refunds, output reservation or worker/queue ownership.
