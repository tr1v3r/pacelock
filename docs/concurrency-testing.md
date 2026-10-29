> Historical archive: the content below includes removed static, idempotent admission, receipt and lookup APIs. It is not current integration guidance or validation. Run historical commands only against their recorded source revision. Current dynamic-only admission exposes UpdateQuotas/Allow/Wait, with no uncertain-result recovery; do not blindly retry Unknown. See the [current API](../README.md).

# Concurrency test contracts


`TestRedisConcurrency` is part of the default `go test ./...` suite. With Redis
fixtures configured, the same 18 scenario groups run against standalone Redis and
a three-master Cluster. No opt-in stress flag or build tag is required. Changes
in this suite add tests only; they do not change admission or accounting behavior.

## Matrix and assertions

| Scenario | Workload | Contract checked |
|---|---|---|
| Burst boundary | 100 / 500 / 1000 simultaneous callers × 1 / 4 / 32 rules, four clients | Exactly 37 weighted admissions; correct blocking dimensions even with reversed rule order; denied calls leave every non-bottleneck quota's remaining capacity intact |
| Mixed costs | 500 callers, costs 1–7 | Accepted cost never exceeds 1000; exact remaining capacity can be consumed |
| Repeated workers | 64 workers × 40 sequential requests, mixed `Allow`/`AllowOnce`, one hot and eight spread domains | Each domain admits exactly its own capacity over repeated contention |
| Overlapping quotas | 400 callers, global quota plus four tenant quotas | Global and per-tenant bounds both hold; failed joint admissions do not consume tenant capacity |
| Namespace/domain isolation | 512 callers across 32 identities, identical quota name and attempt ID | Each identity admits exactly once; namespace-only and domain-only differences both isolate state |
| Same ID | 1000 callers each for allowed and denied outcomes | Exactly one original receipt, identical retained decisions, no duplicate debit |
| Mixed APIs and IDs | 100 `Allow` calls plus four copies of each of 100 IDs | 100 original receipts, exactly 73 total debits, consistent replays and lookups |
| Conflicting ID parameters | 500 callers with two different costs | Exactly one parameter set wins, the other returns `ErrConflict`/`NotCharged`; only the winning cost is debited |
| Policy initialization race | 200 callers with two policies and unique side quotas | First policy wins; losing policy reports `ErrPolicyMismatch`; errors and denials leave side quotas absent |
| Validation/cancellation/observer | 600 mixed valid, invalid, pre-canceled and expired-context operations | Exact valid debits, pre-dispatch errors are `NotCharged`, untouched keys remain absent, one correct observer event per call |
| Lookup/replay race | 300 readers/writers then 500 readers/replays over 100 IDs | Reads observe missing or complete results, never torn records; completed reads/replays do not change quota or refresh TTL |
| Corrupt state | 300 callers per malformed record, malformed quota and wrong Redis type | Correct error/outcome classification, no extra debit or new pre-write-error record |
| Lost responses | 400 dispatched calls per EOF, cancellation and deadline injection, then 400 recovery calls | Completed Redis work can return `Unknown`; SDK does not retry internally; lookup/replay recovers without a second debit |
| Small connection pool | 500 callers sharing one client with one connection per node | Exact admission count, bounded completion, client remains usable |
| Exhausted connection pool | Pin every connection, then 100 deadline-bound calls | Backend deadline errors conservatively report `Unknown`; independently verify no state was written; release restores service |
| Cold script cache | Flush all masters, then 300 mixed admission/read calls | Concurrent `NOSCRIPT` fallback succeeds and still charges once |
| Refill/record expiry | Three real-clock waves of 200 callers; 500 callers after a record expires | Accepted cost stays within burst plus elapsed replenishment; idle quota expires; expired ID permits exactly one new debit |
| Multiple processes | Four independent processes × 64 goroutines, both normal admission and one shared ID | Shared capacity and deduplication hold across process-local clients and SDK instances |

All burst cases use a synchronized start barrier. Each wave has a 20-second
safety deadline; subprocess groups have a 40-second bound. These timeouts detect
hangs and leaked waiters, not a performance SLO. Tests use bounded connection pools
and join workers before deleting their own namespaced fixture state. Subprocesses
run the same test executable and therefore retain race instrumentation.

Slow-refill cases use one minute per unit to keep their exact-capacity assertions
independent of scheduling within a wave. The fast-refill case instead measures
Redis server time and asserts `accepted <= burst + floor(elapsed / interval)`.
No exact p95/p99 latency or fairness among competing callers is asserted.

The response-loss/cancellation/deadline cases replace a successful **real Redis
command result** with a transport/context error. They test recovery after execution;
they do not simulate TCP loss or cancellation interrupting the Redis script.
Corrupt state cases seed explicit fixtures. The existing ACL integration
tests now exercise all-or-none MSET ACL/OOM rejection, errors before/after the
commit boundary, and recovery of complete records. Additional standalone/Cluster
cleanup tests cover strict candidate budgets, SCAN overflow, stale candidates and
200 concurrent cleanup/admission calls without resetting capacity. Malformed receipts
return `ErrState` / `Unknown` without a second debit.

## CI and local reproduction

Existing required CI check names stay unchanged. All six Go 1.26/stable × Redis
7.0/7/8 jobs run the full suite and additionally repeat contention three times.
The `cluster` job does the same with both standalone and Cluster configured.
Every run uses `-race`; repeat runs use `-shuffle=on`. Scenario order is also
shuffled explicitly within each backend, with the seed printed for reproduction.
No performance job needs to be selected manually.

CI sets `PACELOCK_REQUIRED_REDIS_BACKENDS=standalone` or `standalone,cluster`.
A missing expected address is a failure, rather than a silently skipped integration
suite. Outside CI, absent address variables skip the corresponding backend as usual.

Use **dedicated disposable Redis services**: cold-cache tests run `SCRIPT FLUSH`
on all masters, and the existing fault suite creates temporary ACL users. Never
point these tests at production or shared development Redis.

```sh
cd driver/redis/internal/engine
docker compose up -d --wait
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
  go test -race -count=1 -timeout=5m ./...

PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
  go test -race -shuffle=on -count=3 -timeout=5m -run '^TestRedisConcurrency$' -v .
docker compose down
```

To reproduce scenario order, set `PACELOCK_CONCURRENCY_SEED` to the seed printed by
the failing backend. This reproduces ordering, not operating-system scheduling.
A child-process failure includes that child's output in the parent test failure.

## Evidence boundary

This suite covers bounded high-contention correctness and recovery, including
multi-process calls and repeated workers. It does not certify production throughput,
latency percentiles, fairness, hours-long stability, multi-host network partitions,
Redis failover/replica data loss, live Cluster resharding, OOM, or clock jumps.
Those need deployment-specific load and fault testing. Redis Cluster fixtures here
have three masters and no replicas. Existing model/fuzz tests continue to cover
algorithm boundaries independently of these concurrent executions.

## Archived local verification — 2026-10-23

These results describe the earlier native-TTL protocol and its then-current tests,
not validation of the current state format or regression suite.

On macOS/arm64 with dedicated Docker Redis 7.4.11 and three Redis 7.4.11 masters:

- Go 1.24.13: full real-Redis/Cluster suite with `-race` passed (12.89 s);
  the final ACL concurrent-recovery addition was also checked separately.
- Go 1.27.3: final full real-Redis/Cluster suite with `-race` passed (12.704 s).
- Go 1.27.3: all 68 concurrency leaf cases, randomized scenario order and
  `-race -shuffle=on -count=3`, passed (30.327 s total).
- Normal tests without Redis, `gofmt`, `go vet`, CI YAML parsing and
  `git diff --check` passed. The required-fixture negative check failed as intended.

These are local test execution times, not SDK latency measurements. GitHub CI
separately verifies Linux, the Go-version matrix and Redis 7.0/7/8 on the PR head.
