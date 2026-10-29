# Redis MSET local verification — 2026-10-28

> Historical snapshot: diagrams, measurements and API names below describe the pinned source revision, not the current API. Current static use constructs a standalone `NewStatic` / `NewStaticWithOptions`, then passes public `Charge{Name, Cost}` values to `StaticLimiter.Allow` / `AllowOnce` / `Wait`. Default `New` returns dynamic `Limiter`. See the [current API](../../README.md#explicit-static-alternative-fixed-policies-and-costs); generated historical artifacts are intentionally unchanged.

> Archived snapshot: these runs used Q2 quotas, E2/C1/P1 receipts and the
> then-supported native-TTL readers. Those formats/readers are not current.
> Measurements and regression coverage below describe only that earlier tree;
> they do not validate today's key prefix, quota/receipt format or fingerprint.
> See [current state contracts](../compatibility.md#redis-state).

These are short local correctness/performance observations, not production SLOs.
Measured on macOS/arm64, Apple M3 Pro, Go 1.27.3, Redis 7.4.2 on host loopback,
without persistence or replicas. Baseline source: `0f70f5f`. Candidate: the MSET
feature tree in this PR, measured before its commit. Runs were sequential, with
no concurrent fuzzing or correctness suite during benchmarking.

Both protocols use 1/4/32 dimensions; each 200 ms benchmark ran three times.
Reported medians do not imply statistical significance. The comparison includes
Redis command execution and loopback, not just Lua CPU cost. Go allocation counts
for matching admission cases were unchanged. Candidate cleanup excludes SCAN and
expired-state setup; it measures only recheck/deletion, not total cycle cost.
No whole-domain aggregate key is written. Key/record memory still requires
application-scheduled cleanup, and per-scope discovery scans the node keyspace.

Commands (run in `driver/redis/internal/engine` with PACELOCK_REDIS_ADDR set):

```sh
go test -run '^$' -bench 'BenchmarkRedis/(allow|once)/rules=(1|4|32)$' -benchtime=200ms -count=3 -benchmem .
go test -run '^$' -bench BenchmarkPruneCandidates -benchtime=200ms -count=3 -benchmem .
```

| Admission | Quotas | v1 median µs/op | MSET median µs/op |
| --- | --- | --- | --- |
| allow | 1 | 141.88 | 125.84 |
| allow | 4 | 157.65 | 145.78 |
| allow | 32 | 253.10 | 239.16 |
| once | 1 | 183.32 | 140.94 |
| once | 4 | 159.05 | 154.08 |
| once | 32 | 278.17 | 257.81 |

| Candidate cleanup (no discovery) | Median µs/op |
| --- | --- |
| 1 keys | 133.42 |
| 32 keys | 189.19 |
| 64 keys | 256.94 |

Validation used disposable standalone Redis, a three-master Redis Cluster, and
PostgreSQL 18 (`postgres:18-alpine`). With both required-backend settings enabled,
`sh scripts/check.sh -coverprofile=coverage.out` passed all five modules, including
module verification, vet and real-backend race tests. The Redis engine reported
97.7% statement coverage before the final additional corruption assertions.
Both existing fuzz targets passed 20 seconds each. Go 1.27.3 govulncheck v1.9.0
with `-test` passed all five modules with no reachable vulnerabilities.

The archived run’s regression coverage included one-MSET model snapshots; before/after-commit and
encoding faults; actual ACL/OOM rejection; lost replies; logical expiry without
physical deletion; legacy debt/C1/P1; strict cleanup candidate budgets and overflow;
malformed/wrong-role state; stale candidates; lost cleanup replies; Cluster ACLs;
and 200 concurrent cleanup/admission calls preserving exact shared capacity.

## Raw baseline

```text
goos: darwin
goarch: arm64
pkg: github.com/tr1v3r/pacelock/driver/redis/internal/engine
cpu: Apple M3 Pro
BenchmarkRedis/allow/rules=1-12         	    1669	    157490 ns/op	     875 B/op	      19 allocs/op
BenchmarkRedis/allow/rules=1-12         	    1904	    123956 ns/op	     870 B/op	      19 allocs/op
BenchmarkRedis/allow/rules=1-12         	    1677	    141883 ns/op	     875 B/op	      19 allocs/op
BenchmarkRedis/once/rules=1-12          	    1604	    142646 ns/op	    1798 B/op	      52 allocs/op
BenchmarkRedis/once/rules=1-12          	    1426	    193571 ns/op	    1808 B/op	      52 allocs/op
BenchmarkRedis/once/rules=1-12          	    1694	    183322 ns/op	    1795 B/op	      52 allocs/op
BenchmarkRedis/allow/rules=4-12         	    1416	    157653 ns/op	    2307 B/op	      41 allocs/op
BenchmarkRedis/allow/rules=4-12         	    1573	    146805 ns/op	    2304 B/op	      41 allocs/op
BenchmarkRedis/allow/rules=4-12         	    1560	    166530 ns/op	    2304 B/op	      41 allocs/op
BenchmarkRedis/once/rules=4-12          	    1387	    159049 ns/op	    3715 B/op	      98 allocs/op
BenchmarkRedis/once/rules=4-12          	    1243	    181661 ns/op	    3717 B/op	      98 allocs/op
BenchmarkRedis/once/rules=4-12          	    1470	    157251 ns/op	    3714 B/op	      98 allocs/op
BenchmarkRedis/allow/rules=32-12        	     942	    253809 ns/op	   19304 B/op	     243 allocs/op
BenchmarkRedis/allow/rules=32-12        	     939	    248982 ns/op	   19307 B/op	     243 allocs/op
BenchmarkRedis/allow/rules=32-12        	     964	    253104 ns/op	   19302 B/op	     243 allocs/op
BenchmarkRedis/once/rules=32-12         	     835	    270131 ns/op	   25599 B/op	     505 allocs/op
BenchmarkRedis/once/rules=32-12         	     850	    290418 ns/op	   25605 B/op	     505 allocs/op
BenchmarkRedis/once/rules=32-12         	     877	    278167 ns/op	   25583 B/op	     505 allocs/op
PASS
ok  	github.com/tr1v3r/pacelock/driver/redis/internal/engine	7.721s
```

## Raw MSET admission

```text
goos: darwin
goarch: arm64
pkg: github.com/tr1v3r/pacelock/driver/redis/internal/engine
cpu: Apple M3 Pro
BenchmarkRedis/allow/rules=1-12         	    1736	    176137 ns/op	     874 B/op	      19 allocs/op
BenchmarkRedis/allow/rules=1-12         	    1862	    123200 ns/op	     871 B/op	      19 allocs/op
BenchmarkRedis/allow/rules=1-12         	    1975	    125843 ns/op	     869 B/op	      19 allocs/op
BenchmarkRedis/once/rules=1-12          	    1680	    140814 ns/op	    1796 B/op	      52 allocs/op
BenchmarkRedis/once/rules=1-12          	    1534	    140940 ns/op	    1804 B/op	      52 allocs/op
BenchmarkRedis/once/rules=1-12          	    1626	    143185 ns/op	    1797 B/op	      52 allocs/op
BenchmarkRedis/allow/rules=4-12         	    1573	    145781 ns/op	    2304 B/op	      41 allocs/op
BenchmarkRedis/allow/rules=4-12         	    1579	    139986 ns/op	    2304 B/op	      41 allocs/op
BenchmarkRedis/allow/rules=4-12         	    1447	    149098 ns/op	    2308 B/op	      41 allocs/op
BenchmarkRedis/once/rules=4-12          	    1515	    154083 ns/op	    3705 B/op	      98 allocs/op
BenchmarkRedis/once/rules=4-12          	    1488	    158499 ns/op	    3706 B/op	      98 allocs/op
BenchmarkRedis/once/rules=4-12          	    1512	    154033 ns/op	    3709 B/op	      98 allocs/op
BenchmarkRedis/allow/rules=32-12        	     979	    238040 ns/op	   19302 B/op	     243 allocs/op
BenchmarkRedis/allow/rules=32-12        	    1000	    239156 ns/op	   19300 B/op	     243 allocs/op
BenchmarkRedis/allow/rules=32-12        	     978	    244279 ns/op	   19302 B/op	     243 allocs/op
BenchmarkRedis/once/rules=32-12         	     924	    255540 ns/op	   25608 B/op	     505 allocs/op
BenchmarkRedis/once/rules=32-12         	     913	    259699 ns/op	   25596 B/op	     505 allocs/op
BenchmarkRedis/once/rules=32-12         	     920	    257809 ns/op	   25598 B/op	     505 allocs/op
```

## Raw final candidate cleanup

```text
goos: darwin
goarch: arm64
pkg: github.com/tr1v3r/pacelock/driver/redis/internal/engine
cpu: Apple M3 Pro
BenchmarkPruneCandidates/keys=1-12         	    1797	    132052 ns/op	         1.000 candidates/op	     280 B/op	       8 allocs/op
BenchmarkPruneCandidates/keys=1-12         	    1737	    133421 ns/op	         1.000 candidates/op	     280 B/op	       8 allocs/op
BenchmarkPruneCandidates/keys=1-12         	    1698	    157633 ns/op	         1.000 candidates/op	     280 B/op	       8 allocs/op
BenchmarkPruneCandidates/keys=32-12        	    1346	    187234 ns/op	        32.00 candidates/op	    1289 B/op	      39 allocs/op
BenchmarkPruneCandidates/keys=32-12        	    1243	    189190 ns/op	        32.00 candidates/op	    1289 B/op	      39 allocs/op
BenchmarkPruneCandidates/keys=32-12        	    1104	    198596 ns/op	        32.00 candidates/op	    1289 B/op	      39 allocs/op
BenchmarkPruneCandidates/keys=64-12        	     865	    271629 ns/op	        64.00 candidates/op	    2378 B/op	      71 allocs/op
BenchmarkPruneCandidates/keys=64-12        	    1063	    228932 ns/op	        64.00 candidates/op	    2378 B/op	      71 allocs/op
BenchmarkPruneCandidates/keys=64-12        	    1054	    256941 ns/op	        64.00 candidates/op	    2378 B/op	      71 allocs/op
PASS
ok  	github.com/tr1v3r/pacelock/driver/redis/internal/engine	6.093s
```
