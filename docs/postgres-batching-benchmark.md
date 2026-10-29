# PostgreSQL batching benchmark / PostgreSQL 批量 SQL 基准

Measured 2026-10-30 on Apple M3 Pro, darwin/arm64, Go 1.27.1,
Docker Engine 29.8.2, PostgreSQL 17.11 (`postgres:17-alpine`), localhost TCP
through Docker. One database connection; sequential requests, no contention.
Baseline driver: commit `05f20ad`; candidate: batched driver rebased onto `4bd9a76`
(the intervening commit only renames QuotaUpdate to QuotaBatch).

2026-10-30 本机测量；单连接、串行、无竞争，通过 Docker 本机 TCP 访问 PG。
基线为 `05f20ad` 的驱动，候选为接入 `4bd9a76` 类型重命名后的批量实现。

## Results / 结果

Median of three runs, each `-benchtime=1s`, without `-race`.
Timing includes admission transaction/lock/time sampling, but excludes migration,
policy publication and warm-up. Denied requests exhaust only the final quota.

三次运行的中位数，每次 1 秒，不开启 race。包含准入事务、锁和取时，排除迁移、
配置发布和预热。拒绝场景仅最后一个额度耗尽。

| Quotas / 额度数 | Outcome / 结果 | Before ms/op | After ms/op | Speedup / 加速比 | Before → after allocs/op |
|---|---|---:|---:|---:|---:|
| 1 | Allowed / 允许 | 2.168 | 1.859 | 1.17× | 147 → 174 |
| 1 | Denied / 拒绝 | 1.725 | 1.367 | 1.26× | 134 → 150 |
| 4 | Allowed / 允许 | 2.955 | 1.852 | 1.60× | 372 → 271 |
| 4 | Denied / 拒绝 | 2.539 | 1.462 | 1.74× | 317 → 210 |
| 32 | Allowed / 允许 | 13.196 | 2.158 | 6.12× | 2496 → 1104 |
| 32 | Denied / 拒绝 | 9.462 | 1.776 | 5.33× | 2049 → 738 |

At 32 quotas, allowed statements fall from 99 to 6, denied from 67 to 5,
excluding BEGIN/COMMIT. This is a round-trip optimization, not constant database
work. Allocation count falls for multi-quota calls, but bytes allocated for a
32-quota allowed call rise from about 132 KB to 166 KB; single-quota allocations
also rise. Larger SQL/argument buffers trade memory for fewer round trips.
No throughput, contention or production latency guarantee is inferred from these
noisy local samples (some individual runs varied substantially).

32 个额度时，允许分支 SQL 从 99 条降为 6 条，拒绝从 67 条降为 5 条（不含事务边界）。
减少的是往返，不是数据库处理的数据量。多额度分配次数下降，但 32 额度允许分支的分配
字节从约 132 KB 增为 166 KB，单额度分配次数也增加：较大的 SQL 与参数缓冲区是代价。
本机样本有明显抖动，不能据此承诺吞吐、竞争表现或生产延迟。

## Reproduce / 复现

Start disposable fixtures from the repository root, then:

从仓库根启动一次性测试容器后执行：

```sh
cd tests/contracts
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
  go test -run '^$' -bench BenchmarkPostgresAdmission -benchtime=1s -count=3
```

To measure the baseline with the same benchmark, use a separate checkout of
`05f20ad` and an alternate `-modfile` replacing only the PostgreSQL driver with
that checkout's driver. The baseline core must remain compatible with its
`QuotaUpdate` type; the measurement was made before rebasing the candidate onto
`QuotaBatch`. Do not mix those API versions. Benchmarks create unique namespaces;
remove their disposable database after measurement.

基线使用同一个 benchmark，通过独立 `-modfile` 将 PG 驱动替换为 `05f20ad` 的独立副本。
基线 core 必须保留兼容的 `QuotaUpdate` 类型；本次在候选接入 `QuotaBatch` 重命名前完成
基线测量，不能混用这两个 API 版本。基准创建独立 namespace；结束后销毁测试数据库。
