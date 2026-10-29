# Operating the Redis driver

For PostgreSQL, see its [deployment guide](../driver/postgres/README.md).
Core Driver contracts are documented [separately](driver.md).

## Before production traffic

Default admission is dynamic: publish trusted `Quota` policies with positive
revisions before sending `Charge{Name, Cost}` requests. Upgrade every writer and
maintenance process before activation; run PostgreSQL `Migrate` before upgraded
binaries handle traffic. Retire old static writers or stop their static path before
installing policies. `Limiter.Wait(ctx, charges...)` uses the latest
backend policies atomically on each attempt, retrying only ordinary capacity denials
after advisory `RetryAfter`; every error stops waiting. Updates do not proactively
wake sleepers, so a relaxed policy may not be used until the previous delay expires.
Set a context deadline. There is no SDK configuration watcher or background worker;
configuration publication/polling belongs to the application.

Keep configuration/revision fences permanent: `Prune` never deletes them. Updates
preserve absolute TAT, even above a reduced burst; never rename/delete state to
reset debt. Pause with `Rate=0`, resume at a higher revision. On uncertain
publication explicitly retry the same revision/policy; do not blindly retry Allow.

1. Define the quota contract: units, replenishment rate, instantaneous burst,
   provider window semantics and account/model/tenant rule coverage. GCRA does not
   exactly reproduce arbitrary fixed/sliding windows or provider token accounting.
2. Choose the atomicity domain. All jointly checked quotas
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
6. Redis quota state requires caller-scheduled physical cleanup, including idle
   scopes. Bound each maintenance tick, retain the cursor across ticks, repeat full
   cycles and monitor memory/cleanup lag. Logical expiry works if cleanup is delayed;
   physical memory will not drain by itself. See the Redis driver guide.
7. Exercise the exact Redis version, ACL, TLS, failover topology and customer load.
   The included fixture validates three-master routing, not high availability.

No quota policy is hidden in an environment variable inside the
SDK. Environment variables in this repository configure examples/tests only.

## Client retries and Cluster availability

Clients must be initialized with go-redis constructors; zero-value client structs
are unsupported. For these initialized concrete `*redis.Client` and
`*redis.ClusterClient` instances, `redisdriver.New` inspects options locally.
Unsupported negative budgets (`MaxRetries < -1` or `MaxRedirects < -1` at client
construction) return `ErrInvalid` and a nil driver, without a network request
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

No SDK service or background worker is introduced. Custom hooks must not strip
the NoRetry marker or replay commands themselves. Explicit no-redirect settings
are supported but not required for concrete Cluster clients.
The client may discover COMMAND metadata on first use; provision its
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
allowed/denied/error counts. Track unknown outcomes,
policy conflicts, pool timeouts, Redis CPU/memory, evictions, replication lag and
hot slots. Use a concurrency-safe callback; avoid network I/O in the callback.
Bound label cardinality; never label with tenant IDs, quota names
or raw errors. Wrap the observer in application code if panic recovery is needed.

`ErrInvalid` is an input problem (including costs above the current backend burst). `ErrPolicyMismatch` suggests inconsistent
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
| Policy conflict | This invocation did not mutate quota | Coordinate configuration across callers |
| Redis failover/eviction/deletion | Quota history may be lost | Assess overshoot; restore service under an explicit degraded-mode policy |

There is no result-recovery API after an uncertain mutation. Do not blindly retry
`Unknown`, reset debt, or infer that a later `NotCharged` outcome resolved it.
Legacy receipt storage is not automatically removed during upgrades; coordinate
explicit cleanup after retiring old writers. See [migration](compatibility.md).

## Durability and clock behavior

Redis script isolation prevents interleaving. The current driver validates and
encodes before one final MSET that commits all quota updates. An affirmative
commit rejection is NotCharged; ambiguous transport/server failures remain Unknown.
Single-command commit does not restore lost persistence/replica logs.

Redis TIME is shared wall time, not a monotonic clock. Forward jumps can replenish
capacity early; backward jumps can delay recovery or reject state. Alert on host
clock instability and test operational recovery. `WAIT`, Redis persistence and
replication policies are deployment choices; this SDK does not promise that a
successful admission survives every crash or failover.

## Benchmarking

Run the commands in the README against disposable Redis on representative
hardware/network paths. Benchmark 1, 4 and 32 dimensions and separate allowed,
denied calls and policy updates. Include concurrent clients and
multiple domains in application load testing. Serial loopback benchmark averages
are not p95/p99 latency, production throughput, fairness or availability results.

Use dedicated synthetic names. Plan sufficient memory and explicitly clean up expired debt. See [verification.md](verification.md)
for the captured local baseline and its limitations.
