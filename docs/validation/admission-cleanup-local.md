# Dynamic facade allocation cleanup (2026-10-29)

The cleanup removes the synthetic `[]Rule` previously allocated solely to check
returned quota names. Decisions are checked directly against dispatched charges.
This is a facade-only microbenchmark with an always-allow fake driver, **not** a
Redis/PostgreSQL throughput or latency measurement.

Environment: Go 1.27.1, darwin/arm64, Apple M3 Pro, default GOMAXPROCS=12.
Baseline: `c996ddaa3c95220aaeccb80ddf6b718d97188c85`. After: this cleanup's core
sources, identified by Git blob IDs:

```text
managed.go   527b54b2399ba9fee56805abf6ab9369a6c37369
pacelock.go  b8462a60722c474d030a4eb1c9826f0d0ce6a246
gcra.go      2988e14a1dcc3eab4bae63ace0c8e065f27ac1b4
```

Run the following independently in the baseline and cleanup checkouts:

```sh
go test -run '^$' -bench '^BenchmarkDynamicAdmission$' \
  -benchmem -benchtime=200ms -count=3 .
```

All three samples are included below. B/op and allocs/op were identical across
all three samples of each case.

| Quotas | Baseline ns/op samples | Cleanup ns/op samples | B/op before → after | allocs/op before → after |
| --- | --- | --- | --- | --- |
| 1 | 54.15, 53.39, 57.26 | 49.03, 51.86, 46.93 | 88 → 24 | 2 → 1 |
| 4 | 136.2, 136.3, 135.2 | 100.9, 100.8, 102.9 | 320 → 96 | 2 → 1 |
| 32 | 1739, 1768, 1759 | 1611, 1609, 1637 | 6152 → 4104 | 9 → 8 |

**One allocation per Allow call is removed** in this benchmark; the dispatched
charge snapshot remains. The timing runs are short, sequential before/after
samples on a shared host, without CPU affinity or statistical confidence
analysis. They do not establish an end-to-end performance guarantee or measure
denial, Wait, policy publication or internal Redis observation cleanup.

中文结论：本次仅验证门面层清理，每次 Allow 少一次临时 Rule 切片分配，保留请求快照。
不应将这些纳秒级样本解释为 Redis/PostgreSQL 端到端性能提升。
