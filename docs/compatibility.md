# Compatibility and migration

## Supported contract

The module targets Go 1.24+ and Redis 7+. CI configuration tests Go 1.24/stable with
Redis 7.0, the current Redis 7 image and Redis 8. Cluster routing is tested with
three Redis 7 masters. Live results and exact local versions are recorded in
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

Use a concurrency-safe go-redis client and primary routing. `New` still accepts
`redis.Scripter` for custom integrations, but such integrations must preserve
Redis execution, routing and error semantics. Client ownership remains with the
caller. No background goroutines, queues, implicit retry loop or package-global
quota configuration are introduced.

The public API remains pre-v1. This change preserves the signatures of `New` and
`Allow`, the layouts of `Rule` and `Decision`, existing errors.Is behavior and the
v1 quota key/value encoding. `NewWithOptions` is separate to avoid changing the
function type of `New`. Typed-nil clients and nil contexts now return `ErrInvalid`.
Check errors with `errors.Is/As`, not string matching; wrapper text has changed.

### Per-command retry safety (pre-v1)

Concrete `*redis.Client` and `*redis.ClusterClient` instances now accept default,
positive and disabled (`-1`) `MaxRetries` budgets. Pacelock wraps its EVALSHA/EVAL
commands with go-redis v9.22.0's `NoRetry` marker, preventing ambiguous replay at
node, Cluster and ASK pipeline layers without disabling retries for other commands
on a shared client. Existing explicit `MaxRetries: -1` settings remain valid.

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

Existing quota keys retain their exact encoding:

```text
pacelock:v1:{domain_digest}:quota_digest
TAT_microseconds:effective_interval_microseconds:Burst
```

New idempotency records use the same slot and a disjoint suffix:

```text
pacelock:v1:{domain_digest}:attempt:sha256(admission_id)
P1|fingerprint
C1|fingerprint|{"names":[...],"values":[allowed,retry_us,blocked_flags...]}
```

`P1` means interrupted/pending; `C1` is a complete decision. The fingerprint binds
retention and the ordered effective rules. These formats are internal; do not
write or parse them in application code. Never log raw retained records as metrics.
A future format change requires a documented reader/migration strategy.

Old `Allow` and new methods share quota state. Old callers do not write attempt
records, so mixed rollout cannot deduplicate an admission initially made with
`Allow`. New `AllowOnce` callers must keep ID, parameter order, effective policy,
cost and retention identical on replay. Rolling back to the old code leaves
bounded record TTLs but removes idempotent protection from subsequent old calls.

## Policy changes

An active policy mismatch rejects before quota mutation. Coordinate policy changes
across callers. Either wait for the old quota to replenish and expire while
coordinating traffic, or design an explicit debt-preserving migration outside this
SDK. Renaming a quota/domain creates fresh capacity; deleting keys silently resets
debt. Neither is a transparent policy update.

## Failure and replay compatibility

`Allow` has no attempt ID and remains unsafe to replay after an ambiguous failure.
`AllowOnce` returns the original decision while its record survives. A complete
record is useful evidence of that admission, but says nothing about whether the
upstream business request was executed. A pending record, missing record, lost
replication or expired ID is not evidence that capacity was never charged.

Use separate IDs for admission transport recovery and for distinct business
attempts. Implement business execution coordination outside this limiter. This
SDK does not provide exactly-once execution, crash-proof quota accounting, refund,
output reservation or billing. Those are not implied by a successful release.
