# Operating the Redis driver

For PostgreSQL, see its [deployment guide](../drivers/postgres/README.md).
Core Driver contracts are documented [separately](drivers.md).

## Before production traffic

1. Define the quota contract: units, replenishment rate, instantaneous burst,
   provider window semantics and account/model/tenant rule coverage. GCRA does not
   exactly reproduce arbitrary fixed/sliding windows or provider token accounting.
2. Choose the atomicity domain. All jointly checked quotas and attempt records
   share one slot. Measure the hottest domain, not only aggregate Redis throughput.
3. Use private networking, TLS, least-privilege authentication, dedicated quota
   keys and `maxmemory-policy noeviction`. Configure Redis persistence/replication
   according to the application's tolerance for lost admissions.
4. Default, positive or disabled (`-1`) `MaxRetries` budgets are supported; Cluster
   may retain its default redirect budget. Set
   `ContextTimeoutEnabled: true` and connection, read/write, pool and request
   deadlines to the application's latency budget. SDK script commands carry
   `NoRetry`; see the retry contract below.
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
Unsupported negative budgets (`MaxRetries < -1` or `MaxRedirects < -1` at client
construction) return `ErrInvalid` and a nil limiter, without a network request
or changing caller-owned settings. Configure the client before construction;
do not mutate its options later. A shared client retains its retry policy for
commands outside Pacelock.
The SDK uses go-redis v9.22.0's per-command `NoRetry` contract. Concrete clients
are adapted without changing options: EVALSHA/EVAL carry the marker through node,
Cluster and ASK pipeline execution. NOSCRIPT still falls back safely to EVAL.
Default, positive and disabled MaxRetries budgets are all supported. A standalone
client normalizes input 0 to 3 and -1 to 0; Cluster normalizes input 0 to -1.
These client defaults do not change the per-command NoRetry guarantee. Cluster's
normalized MaxRedirects may be zero or positive: -1 at construction disables
redirects, while the default/positive budget permits bounded MOVED/ASK recovery.
Ambiguous EOF, timeout and lost-response errors remain Unknown and are never
converted into a second admission. ASKING and the script use the same pipeline
connection. Routing recovery does not guarantee failover persistence.

The dependency update adds go-redis's indirect atomic/sys dependencies; no new
SDK service or background worker is introduced. Custom hooks must not strip the
NoRetry marker or replay commands themselves. Old explicit no-redirect settings
remain supported, but are no longer required for known Cluster clients.
The upgraded client may discover COMMAND metadata on first use; provision its
read permission for efficient routing, and warm client metadata before strict
latency measurements. Discovery may have a client-owned background deadline.

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
