# Quota.Charge local verification — 2026-10-28

Scope: local, uncommitted changes on `codex/bound-policy-costs`, based on
`705c86b5422a9f8c5671197db2edcd29007d8c55` (PR #20). These results are not CI
results, a release certification or production throughput measurements.

## Environment and correctness checks

- Go 1.27.1, darwin/arm64, Apple M3 Pro, 12 logical CPUs reported by Go.
- Disposable Compose project `pacelock-quota-charge-20261028`: `redis:7-alpine`
  standalone plus three-master Cluster, and `postgres:17-alpine`, over host loopback.
- The project's containers and network were removed after backend checks.
- No new dependency, Driver contract, storage layout or low-level Rule API change.

From the worktree root, both invocations passed across all five modules
(dependency verification, vet, and `-race -count=1` tests, including example builds):

```sh
sh scripts/check.sh
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster sh scripts/check.sh
```

The added tests cover quota/charge snapshots, complete-set validation, invalid
policies and costs, effective-policy equivalence including microsecond rounding,
local rejection before Driver dispatch, unchanged capacity and unconsumed retained
IDs after rejection, atomic denial, retained replay, shared debt, concurrent calls,
charge-order normalization and binding-order fingerprint conflicts.

All three migrated examples executed successfully against these fixtures:

```sh
REDIS_ADDR=localhost:16379 go -C driver/redis run ./examples/llm
REDIS_ADDR=localhost:16379 go -C driver/redis run ./examples/idempotency
DATABASE_URL='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
  go -C examples/postgres run .
```

The Redis examples admitted the request and demonstrated `Replayed:true` on retry;
the PostgreSQL example reported `allowed=true`.

Additional contribution checks passed:

```sh
go -C driver/redis/internal/engine test -run '^$' -fuzz FuzzInterval -fuzztime 20s .
go -C driver/redis/internal/engine test -run '^$' -fuzz FuzzRecord -fuzztime 20s .
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
```

- `FuzzInterval`: 2,576,384 executions; `FuzzRecord`: 2,747,759 executions.
- The core-module vulnerability scan reported **zero reachable vulnerabilities**.
  It also reported one advisory in imported packages and thirteen in required
  modules whose vulnerable code did not appear to be called. This is not a claim
  that the toolchain or every dependency is advisory-free, nor a nested-module scan.
- Formatting and `git diff --check` passed. Go-cache/fuzz-cache permission failures
  under the workspace sandbox were retried with explicit approval; only completed
  successful runs are counted above.

## Facade microbenchmark

The same command ran before and after the API adjustment, separately from tests
and fuzzing, with the toolchain and CPU above:

```sh
go test -run '^$' -bench '^BenchmarkBoundAdmission$' -benchmem -count=3 .
```

`BenchmarkBoundAdmission` uses an always-allow fake Driver and preconstructed
charges in reverse binding order. It measures ordinary `Allow`, not charge
construction, binding, retained admission or real backend latency. Before means
the PR's `Charge{Name, Cost}` implementation; after means the quota snapshot and
full effective-policy validation implementation. All timing samples are ns/op:

| Mode | Quotas | Before (three samples) | After (three samples) | B/op, both | allocs/op, both |
|---|---:|---|---|---:|---:|
| rules | 1 | 63.91 / 62.69 / 66.35 | 58.24 / 58.00 / 59.20 | 48 | 1 |
| charges | 1 | 87.72 / 84.82 / 88.30 | 90.16 / 90.04 / 89.85 | 96 | 2 |
| rules | 4 | 130.3 / 133.9 / 132.5 | 124.9 / 124.0 / 123.4 | 192 | 1 |
| charges | 4 | 216.1 / 222.5 / 217.8 | 215.1 / 213.1 / 213.6 | 384 | 2 |
| rules | 32 | 920.7 / 921.2 / 824.3 | 786.6 / 807.1 / 799.6 | 3624 | 4 |
| charges | 32 | 1249 / 1224 / 1311 | 1485 / 1420 / 1426 | 5416 | 5 |

Per-admission allocations are unchanged; the larger captured Charge values are
constructed outside the timed loop, so this does not measure their storage cost.
The median 32-quota bound call increased from 1249 to 1426 ns/op while the unchanged
low-level control also varied between runs. Extra policy validation has a cost;
these short local samples are not a statistically isolated optimization claim,
backend capacity estimate or latency SLA.
