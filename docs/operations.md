# Operating Pacelock

## Before production traffic

1. Define the quota contract: units, replenishment rate, instantaneous burst,
   provider window semantics and account/model/tenant rule coverage. GCRA does not
   exactly reproduce arbitrary fixed/sliding windows or provider token accounting.
2. Choose the atomicity domain. All jointly checked quotas and attempt records
   share one slot. Measure the hottest domain, not only aggregate Redis throughput.
3. Use private networking, TLS, least-privilege authentication, dedicated quota
   keys and `maxmemory-policy noeviction`. Configure Redis persistence/replication
   according to the application's tolerance for lost admissions.
4. Set `MaxRetries: -1`, and also `MaxRedirects: -1` for Cluster. Set
   `ContextTimeoutEnabled: true` and connection, read/write, pool and request
   deadlines to the application's latency budget. Constructors reject unsafe
   retry options on concrete go-redis clients; see the retry contract below.
   Audit middleware, hooks, custom clients/factories and proxies for command replay.
5. Decide the backend-failure policy explicitly. Returning an unavailable response
   is usually preferable when exceeding provider quotas is unacceptable. Any
   fail-open policy knowingly permits traffic outside the shared quota contract.
6. For `AllowOnce`, persist IDs before dispatch, select retention covering the
   recovery horizon and budget memory for retained IDs, including denials. A rough
   count is admission attempts per second × retention seconds; measure actual
   bytes per record including names and Redis overhead. Keep pending records.
7. Exercise the exact Redis version, ACL, TLS, failover topology and customer load.
   The included fixture validates three-master routing, not high availability.

No quota or idempotency policy is hidden in an environment variable inside the
SDK. Environment variables in this repository configure examples/tests only.

## Client retries and Cluster availability

Clients must be initialized with go-redis constructors; zero-value client structs
are unsupported. For these initialized concrete `*redis.Client` and
`*redis.ClusterClient` instances, `New` and `NewWithOptions` inspect options locally.
Unsafe settings return `ErrInvalid` and a nil limiter, without a network request
or changing caller-owned settings. Use a
dedicated client configured before construction; do not mutate its options later.
For go-redis v9.17.3, the checked values are the **normalized** options:

- Standalone: `Client.Options().MaxRetries == 0`, obtained by passing
  `MaxRetries: -1` to `redis.NewClient`. Omitting it enables ordinary retries and
  is rejected.
- Cluster: `ClusterClient.Options().MaxRedirects == 0` and `MaxRetries == -1`.
  Pass both `MaxRedirects: -1` and `MaxRetries: -1` explicitly. go-redis also
  normalizes an omitted Cluster `MaxRetries` to `-1`, which is accepted; an omitted
  `MaxRedirects` enables the outer loop and is rejected.

The Cluster outer loop retries some network failures as well as redirects.
Leaving it enabled can debit a non-idempotent `Allow` twice, or hide a first debit
behind a later quota denial. Disabling it is therefore required, even when the
application normally uses `AllowOnce`.

This also disables transparent `MOVED`/`ASK` retries. Normal routing uses the slot
map, but resharding/failover or a stale map can surface an error on the current
call. These backend errors remain `Unknown`, not quota denials or proof of no
debit. Plan for this availability tradeoff; do not restore retries as a workaround.
For an uncertain `AllowOnce`, recover the same retained ID under a fresh deadline;
for `Allow`, apply the application's explicit failure policy. Slot-map discovery
and refresh require the Cluster ACL grant documented in [security](../SECURITY.md).

Custom `redis.Scripter` implementations and wrappers, hooks, Cluster `NewClient`
factories and external proxies cannot be fully verified by constructor option
checks. Their owners must preserve the no-ambiguous-replay contract. A successful
constructor is not an end-to-end exactly-once guarantee. Pacelock itself only
falls back from `EVALSHA` to `EVAL` for a `NOSCRIPT` cache miss; it does not retry an
ambiguous transport failure.

## Metrics and diagnosis

Use `NewWithOptions(..., Options{Observer: ...})` to export operation latency and
allowed/denied/error/found/missing counts. Track replay ratio, unknown outcomes,
policy conflicts, pool timeouts, Redis CPU/memory, evictions, replication lag and
hot slots. Use a concurrency-safe callback; avoid network I/O in the callback.
Bound label cardinality; never label with tenant IDs, admission IDs, quota names
or raw errors. Wrap the observer in application code if panic recovery is needed.

`ErrInvalid` is a local input problem. `ErrPolicyMismatch` suggests inconsistent
rollout or uncoordinated policy migration. `ErrState` suggests corrupted/wrong-type
state, invalid replies or an unsupported clock jump: inspect, alert and preserve
state. Always check `OutcomeOf(err)`: `ErrState` alone does not establish that no
debit happened. The typed outcome distinguishes detected pre-write checks from
ambiguous post-dispatch/reply failures. `ErrBackend` preserves the original error chain.

## Incident recovery

| Observation | Safe interpretation | Next action |
|---|---|---|
| Normal denial | No quota debit for this call | Reject or schedule a genuinely new attempt; apply jitter externally |
| Pre-dispatch cancellation | This call did not debit | Retry only if business intent still exists |
| Timeout/lost reply to `Allow` | Debit unknown | Do not blindly repeat or refund; apply failure policy |
| Timeout/lost reply to `AllowOnce` | Admission may exist | Query the same ID with a fresh deadline within retention |
| Complete retained allow/deny | Original decision recovered | Coordinate execution with the business attempt record |
| Pending record | Script may have partially charged | Alert; preserve marker and uncertainty; do not automatically refund/reuse ID |
| Missing record | Not observed, possibly expired/lost/in-flight | Do not infer zero debit or exactly-once execution |
| Policy conflict | This invocation did not mutate quota | Coordinate configuration across callers |
| Redis failover/eviction/deletion | Quota and dedupe history may be lost | Assess overshoot; restore service under an explicit degraded-mode policy |

A retained rejected ID cannot become an allowed attempt through replay. A replayed
`RetryAfter` is historical; it should not be used to reset a waiting timer forever.
A new actual attempt requires a new ID and a new quota decision. Admission recovery
alone must never issue a second upstream model call.

## Durability and clock behavior

Redis script isolation prevents interleaving but does not roll back runtime
errors. The SDK prevalidates quota state and writes a pending marker before debit
for `AllowOnce`. If later writes fail, the marker prevents replay while retained;
it cannot determine how much was already written or restore a lost replica log.
Do not delete it to unblock a request.

Redis TIME is shared wall time, not a monotonic clock. Forward jumps can replenish
capacity early; backward jumps can delay recovery or reject state. Alert on host
clock instability and test operational recovery. `WAIT`, Redis persistence and
replication policies are deployment choices; this SDK does not promise that a
successful admission survives every crash or failover.

## Benchmarking

Run the commands in the README against disposable Redis on representative
hardware/network paths. Benchmark 1, 4 and 32 dimensions and separate allowed,
denied, new idempotent, replay and lookup calls. Include concurrent clients and
multiple domains in application load testing. Serial loopback benchmark averages
are not p95/p99 latency, production throughput, fairness or availability results.

Use dedicated synthetic names. Benchmarks clean up their own namespace and retain
new attempt records for a minute; plan sufficient memory. See [verification.md](verification.md)
for the captured local baseline and its limitations.
