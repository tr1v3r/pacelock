# Prune and contended Wait validation

Local observation, 2026-10-30: macOS arm64 / Apple M3 Pro, Go 1.27.3,
PostgreSQL 17 and Redis 7 disposable containers. These results are not SLOs.

## PostgreSQL

`TestPostgresPruneIndexedRecovery` inserts 20,000 active debts, one recovered
row, and one malformed early-expiry row with an active TAT. It logs both old/new
candidate queries and the complete new DELETE using EXPLAIN ANALYZE BUFFERS.
The complete DELETE is rolled back before checking the public Prune API.

One run (80,119 total table rows after repeated isolated test runs):

| Operation | Shared buffer hits | Execution time |
| --- | ---: | ---: |
| Old candidate filter/order | 1,638 | 3.591 ms |
| Indexed candidate filter/order | 4 | 0.013 ms |
| Complete indexed DELETE | 9 | 0.085 ms |

The old plan filtered 80,118 rows in a sequential scan; the new candidate plan
used `pacelock_quotas_recovered_idx`. The full DELETE used a nested loop with the
recovery index and primary key. Plans and timings depend on table statistics,
visibility and deployment. LIMIT is not a hard elapsed-time bound.

`TestPostgresPruneConcurrentAdmission` runs 20 pruning operations concurrently
with 20 admissions in one domain, checks exactly 20 recovered deletions, then
verifies that the consumed live quota remains exhausted.

The extra recovery index adds write/storage cost and prevents HOT updates when
its recovery expression changes. These observations measure cleanup, not an
admission throughput improvement. Run migration/index creation during controlled
setup; see the PostgreSQL guide for write-lock implications.

## Wait

Reproduce with:

```sh
go test -run '^$' -bench BenchmarkWaitContention -benchtime=1x -count=3 .
```

Each batch starts 100 callers against real `EvaluateManaged` math, burst 1,
1,000 units/s. An atomic in-memory driver isolates retry pressure from network
latency. Three samples:

| Wait mode | Attempts per 100 admissions | Batch completion |
| --- | --- | --- |
| Default exact RetryAfter | 5,050 / 5,050 / 5,044 | 111 / 116 / 113 ms |
| WaitBackoff=250ms | 784 / 800 / 805 | 355 / 410 / 417 ms |

The option reduced backend attempts by about 84%, at about 3–4 times completion
latency here. It does not eliminate worst-case contention or guarantee fairness.
Zero remains the default; choose a cap based on the latency budget. Pure window
bound/saturation tests, maximum-duration jitter checks, error-stop tests and
cancellation tests run independently of benchmark timings.

## Checks

`scripts/check.sh` passed for all six modules with `-race`, vet and module
verification, with standalone Redis, Redis Cluster and PostgreSQL required via
`PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster` and
`PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres`. Services were separate,
disposable review fixtures. No production-backend validation is claimed.
