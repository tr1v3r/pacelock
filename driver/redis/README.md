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
It does not load or rewrite an aggregate domain object. `Lookup` reads the authoritative primary and never refreshes retention.

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
microsecond-rounded interval and burst; equivalent valid policy expressions are
accepted. Invalid, incomplete or mismatched charges fail locally with `ErrInvalid` /
`NotCharged` before admission, not backend `ErrPolicyMismatch`. Rule APIs share the same
Driver and storage; blocking admission uses `limiter.Wait` with
rules, not a bound wait method. See [policy and cost contracts](../../README.md#fixed-policies-and-per-attempt-costs).

Concrete clients receive per-command NoRetry while retaining bounded MOVED/ASK
redirects. Ring is unsupported. Custom Scripters/hooks/factories/proxies must
preserve no-replay and primary-routing behavior.

## Logical expiry and bounded physical cleanup

Keys use `pacelock:{scope_hash}:...`, with an `attempt:` suffix for receipts. Values use:

```text
quota:   tat_us:interval_us:burst
receipt: expires_us|fingerprint|decision_json
```

The sole quota format is `tat:interval:burst`, without a version prefix. Every
quota logically expires when `tat <= now`, regardless of native TTL; its debt is
then fully replenished and a new policy may apply. Active quotas still require
matching interval and burst. A receipt expires at Redis admission time plus
retention. Reads ignore logically expired state even if its key is still present.
Replays/queries never extend deadlines. `decision_json` is the writer's canonical
`{"names":[...],"values":[...]}` form: fixed field order, no extra fields or
whitespace, with UTF-8 names and integer values. Invalid receipts are rejected
before expiry is applied. These encodings are internal; applications must not
construct them.

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
- A short Lua script re-reads current values by key role: `:attempt:` keys contain
  receipts; quota keys contain `tat:interval:burst`. It validates all candidates
  before deleting expired quotas and receipts in one DEL. Malformed or wrong-role
  values fail the batch without deletion. A stale candidate cannot authorize
  deleting updated active debt or a renewed record.
- `Examined`, `Deleted` and `Cursor` are valid only on success. Lost cleanup replies
  can be retried safely; counts are not an exactly-once audit ledger.
- Cursors are opaque, in-memory and scope-bound. Restart from zero after restart
  or topology changes. Concurrent modification makes SCAN best effort; repeat
  complete cycles. Cleanup failure leaves extra memory, not permission to erase
  live state. Monitor memory/cleanup lag and handle prolonged cleanup outages.
- Admission ACL: `+evalsha +eval +get +mset +time`, plus client handshake grants.
  Maintenance additionally needs `+scan +del`; a separate maintenance identity is
  supported. Cluster also needs `+cluster|slots`. See [security](../../SECURITY.md).

## Receipt handling

Receipts use logical expiry. Their fingerprint encodes
`[retention_ms,step,burst,cost,name,...]` in rule order. A malformed receipt returns
`ErrState` / `Unknown`; it is never treated as missing or as a new admission.
Preserve uncertainty and investigate rather than deleting state or refunding.
See [state contracts](../../docs/compatibility.md#redis-state).

Prepare cleanup and ACLs before deployment. Do not rename/delete keys to get fresh
capacity.

Use primary routing, TLS/authentication, controlled ACLs, noeviction and explicit
durability settings. [Operations](../../docs/operations.md), [core usage](../../README.md)
and [Driver contract](../../docs/driver.md) describe the remaining limits.
There is no automatic PostgreSQL failover or cross-backend state migration.

Run `REDIS_ADDR=localhost:16379 go run ./examples/llm` from this module.
Run `sh scripts/check.sh` at the root for all modules; Redis tests include model,
concurrency, logical expiry, state validation, receipt handling, cleanup, ACL/OOM
and lost-response faults.

Short local baseline/MSET and candidate-cleanup measurements are in the
[verification record](../../docs/validation/redis-mset-local.md); they are not production SLOs.
The historical [Lua encoding experiment](bench/lua-parse/README.md) includes a
reproducible harness and raw timings for the former quota encoding; its scalar
prototype drops policy checks and is not a contract-preserving replacement.
