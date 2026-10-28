# Redis driver

[简体中文](README.zh-CN.md)

Independent module: `github.com/tr1v3r/pacelock/driver/redis`. Requires Redis 7+.
Applications own the initialized go-redis Client/ClusterClient and close it.

```go
driver, err := redisdriver.New(client)
if err != nil { return err }
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

Import `redisdriver "github.com/tr1v3r/pacelock/driver/redis"` and
`"github.com/tr1v3r/pacelock"`. Construction performs no I/O or option mutation.
One domain is one Cluster slot. Every caller must use matching scope, names and
effective policies, and include every applicable quota in an admission.

## Single-command commit and recovery

Lua reads the receipt and all requested quotas, checks policies, calculates GCRA,
and encodes all values and the response before its only mutation: one `MSET`.
An allowance commits at most 32 small quota values plus one complete receipt;
a denial commits only the receipt. Plain `Allow` commits only admitted quotas.
It does not load or rewrite an aggregate domain object. New admissions never write
Pending. `Lookup` reads the authoritative primary and never refreshes retention.

A rejected MSET command (for example, a script-visible ACL/OOM rejection) is
`NotCharged`. Errors without that affirmative server response, including lost
responses, remain `Unknown`. A lost post-commit response can be recovered using
the complete retained result. This does not promise crash/failover durability or
upstream execution deduplication. Never blindly retry ordinary `Allow` or refund.

Define named `Quota` values such as `rpm` and `tokens`, then call
`limiter.Bind(rpm, tokens)` to validate and copy the fixed quota set locally.
Submit `bound.Allow(ctx, rpm.Charge(1), tokens.Charge(1500))`, or pass the same
`...Charge` values to `bound.AllowOnce(ctx, id, retention, charges...)`.
`q.Charge(cost)` captures a quota value snapshot and cost in private fields without
validation or I/O. `Quota.Name` / `Policy` remain public; later mutation affects
neither earlier charges nor bindings. Change policies by creating a new binding.
Every call must charge all bound quotas exactly once; order is normalized to binding
order. Each captured policy must be fully valid, with matching name, effective
microsecond-rounded interval and burst; equivalent valid static policy expressions
are accepted. Rule/Bind admission requires revision zero. Invalid, incomplete or mismatched charges fail locally with `ErrInvalid` /
`NotCharged` before admission, not backend `ErrPolicyMismatch`. Existing Rule APIs,
and the base Driver interface remain compatible; blocking admission still uses `limiter.Wait` with
rules, not a bound wait method. See [policy and cost contracts](../../README.md#fixed-policies-and-per-attempt-costs).

Concrete clients receive per-command NoRetry while retaining bounded MOVED/ASK
redirects. Ring is unsupported. Custom Scripters/hooks/factories/proxies must
preserve no-replay and primary-routing behavior. All admissions pass explicit debt
and policy keys (plus optional receipt). During Cluster resharding, mixed/missing
keys can yield `TRYAGAIN`; this remains a conservative `Unknown` transport error,
never an implicit replay. Retry/recovery decisions belong to the caller.

## Explicit managed policies

`Policy.Revision == 0` keeps static behavior. Opt in with positive, monotonically
increasing revisions and `limiter.UpdatePolicies(ctx, quotas...)` (or the driver's
`UpdatePolicies(ctx, pacelock.PolicyUpdate{Scope: scope, Quotas: quotas})`). Each batch
is atomic with admission, using one final MSET and the same NoRetry transport.
Updates preserve absolute TAT exactly, including debt above a reduced capacity.
Same revision plus the same effective interval/burst is idempotent; lower revisions
or same-revision conflicts return `ErrPolicyMismatch`. Roll back parameters only at
a higher revision. `NotCharged` describes quota debit, not an assertion that an
ambiguous update failed: transport/lost-reply failures remain `Unknown` and need
an explicit same-revision retry if desired.

The data plane passes names and costs only: `limiter.AllowManaged(ctx,
pacelock.NamedCharge{Name: "rpm", Cost: 1})`, or `AllowManagedOnce(ctx, id,
retention, charges...)`. The driver's optional `AdmitManaged` extension resolves
current backend policies in the admission Lua call; callers do not refresh bindings
or send revisions. Missing configuration returns `ErrPolicyMissing` / `NotCharged`.
Static Rule/Bind calls reject positive revisions locally; static admission against a
configured quota returns `ErrPolicyMismatch`, preventing bypass even after idle/prune.

Managed `Rate=0` returns `ErrPaused` / `NotCharged` without debit or receipt write,
even when cost exceeds the paused burst. Otherwise over-burst cost returns `ErrInvalid`.
Resume with a higher revision; debt recovers naturally. There is no `WaitManaged`.
Receipts replay before current policy checks, including across updates and pauses.
Their historical allowed result is **not permission to execute upstream again**.
Managed fingerprints bind ordered names/costs and retention, not policy revisions;
static fingerprints remain unchanged and static/managed requests with one ID conflict.

Configuration and debt are separate. The same-slot `policy:<name_hash>` key holds
permanent `P3|interval_us:burst:revision` (zero interval means paused). Updates read
and change only configuration, never debt or its expiry; same-revision equivalent
updates perform no write. Existing quota keys hold managed debt as `D3|tat_us`,
with Q2/legacy debts still readable. Prune reclaims expired debt but validates and
retains configuration forever. Memory therefore includes all configured names;
there is no automatic policy retirement/reset.

Deploy protocol-aware binaries to **every reader, writer and maintenance process
before activation**: older writers do not check configuration keys and can bypass
policy. Mixed-version activation and unconverted rollback are unsupported. Idle or
pruning does not make rollback safe. Callers own configuration polling/publication;
the SDK starts no worker. See the [runnable managed example](examples/managed/main.go).

## Logical expiry and bounded physical cleanup

Keys keep their existing `pacelock:v1:{scope_hash}:...` names/hash tag. Values use:

```text
quota:   Q2|tat_us:interval_us:burst (static) or D3|tat_us (managed debt)
policy:  P3|interval_us:burst:revision (separate same-slot key)
receipt: E2|expires_us|C1|fingerprint|decision_json
```

A static quota logically expires at its TAT, when its debt is fully replenished.
Managed quota debt also recovers at TAT, but its policy identity never expires.
A receipt expires at Redis admission time plus retention. Reads ignore logically
expired static state even if its key is still present. Replays/queries never extend deadlines.
These encodings are internal; applications must not construct them.

MSET clears native TTLs. Physical deletion is therefore an application-owned
operational requirement for bounded memory, separate from admission correctness.
Schedule repeated cleanup cycles for every used scope, including idle scopes;
constructors/admissions start no worker and do not run a full scan.

```go
scope := pacelock.Scope{Namespace: "llm-service", Domain: "provider-account-a"}
var cursor redisdriver.PruneCursor // Keep this across scheduled maintenance ticks.
// Example: at most eight steps during this tick; maintenanceCtx has a deadline.
for step := 0; step < 8; step++ {
    page, err := driver.Prune(maintenanceCtx, scope, cursor, 32)
    if err != nil { return err } // Ignore page; retry the previous cursor later.
    cursor = page.Cursor
    if cursor.Done() {
        cursor = redisdriver.PruneCursor{} // Begin a new cycle on a later tick.
        break
    }
}
```

- `Prune` requires `1 <= limit <= MaxPruneKeys` (64). Each call checks at most
  `limit` candidate values and performs at most one SCAN discovery page. COUNT is
  a hint, not a hard discovery/latency limit; overflow is retained in the cursor.
- Discovery routes to the scope's current primary, including Cluster. SCAN walks
  that node's keyspace, filtered by scope; many scopes can make repeated scans
  expensive. Measure and budget maintenance separately from admissions.
- A short Lua script re-reads current values, then deletes expired v2 candidates
  in one DEL. A stale candidate cannot authorize deleting updated active debt or
  a renewed record. Legacy values/Pending retain their original native TTL.
- `Examined`, `Deleted` and `Cursor` are valid only on success. Lost cleanup replies
  can be retried safely; counts are not an exactly-once audit ledger.
- Cursors are opaque, in-memory and scope-bound. Restart from zero after restart
  or topology changes. Concurrent modification makes SCAN best effort; repeat
  complete cycles. Cleanup failure leaves extra memory, not permission to erase
  live state. Monitor memory/cleanup lag and handle prolonged cleanup outages.
- Admission ACL: `+evalsha +eval +get +mset +time`, plus client handshake grants.
  Maintenance additionally needs `+scan +del`; a separate maintenance identity is
  supported. Cluster also needs `+cluster|slots`. See [security](../../SECURITY.md).

## Upgrade and rollback

Stop and drain old writers before enabling v2 writers; mixed old/new writers and
an unconverted rollback are unsupported. Keep the same keys/scope/policies: the
new reader accepts legacy quota strings and C1 receipts, preserving debt and
original receipt TTL. Successful new quota writes replace legacy values in place.
Legacy P1 remains `ErrOutcomeUnknown`, is never automatically refunded/deleted,
and is never synthesized by v2. Old scripts cannot understand v2 values.

Prepare cleanup and ACL changes before rollout. Do not rename/delete keys to get
fresh capacity. A rollback requires a quiesced, deadline/debt-preserving conversion
or expiry/drain of all retained state; no automatic rollback converter is supplied.
See [compatibility](../../docs/compatibility.md).

Use primary routing, TLS/authentication, controlled ACLs, noeviction and explicit
durability settings. [Operations](../../docs/operations.md), [core usage](../../README.md)
and [Driver contract](../../docs/driver.md) describe the remaining limits.
There is no automatic PostgreSQL failover or cross-backend state migration.

Run `REDIS_ADDR=localhost:16379 go run ./examples/llm` from this module.
Run `sh scripts/check.sh` at the root for all modules; Redis tests include model,
concurrency, logical expiry, migration, cleanup, ACL/OOM and lost-response faults.

Short local baseline/MSET and candidate-cleanup measurements are in the
[verification record](../../docs/validation/redis-mset-local.md); they are not production SLOs.
The [Lua scalar-vs-Q2 experiment](bench/lua-parse/README.md) includes a reproducible
harness and raw timings; its scalar prototype drops policy checks and is not a
contract-preserving replacement.
