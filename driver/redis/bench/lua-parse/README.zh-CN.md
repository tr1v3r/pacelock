# Redis Lua 单数字与 Q2 对照实验

> 历史快照：本文图示、测量和 API 名称对应文中固定源码版本，早于 dynamic-default 改名；原静态 Allow/AllowOnce/Wait/Bind 现对应 AllowStatic/AllowStaticOnce/WaitStatic/BindStatic，原 Quota.Charge 现为 Quota.StaticCharge。不是当前默认动态 API 用法。

这是隔离实验，不是生产协议迁移提案。单数字原型去掉跨客户端策略冲突检测及旧格式兼容，
**不与 Pacelock 当前 Q2 协议契约等价**。

在 Redis driver 模块目录中，连接**独立、可销毁的 Redis**：

```sh
go run ./bench/lua-parse -addr 127.0.0.1:PORT > results.json
go run ./bench/lua-parse -addr 127.0.0.1:PORT -iterations 200000 > results-200k.json
```

必须显式传入地址。不要连接生产 Redis，也不要并行运行：程序会覆盖和删除固定的
`bench:{lua-parse}:*` 键，并依赖独占 EVALSHA 统计。需要 SCRIPT LOAD、EVALSHA、
INFO、TIME、GET、MSET 和 DEL 权限。启动 Redis 时使用 `--save '' --appendonly no`
以匹配本次条件，不要修改已有服务的持久化配置。

## 方法

- 使用 go-redis，直接读取当前 checkout 的 `protocol.lua` 和 `gcra.lua`。
- 预热 3 轮不计入，随后 15 轮，每轮按固定种子随机交错各方案顺序。
- 使用 Redis `cmdstat_evalsha.usec` 差值计时，不含客户端及网络延迟。
- 微基准每次脚本执行 50,000 次，轮换 64 个合法、债务活跃的 TAT；每次结果计入
  整数校验和并验证。包含样本构造、循环/索引、函数调用及校验和开销；样本构造
  被循环次数摊薄，共同循环基线单独报告，不作扣减。
- `scalar_parse` 仅 `tonumber`；`q2_parse` 包含 Q2 前缀识别、截取、模式匹配及
  三次数值转换。
- reader 组额外包含边界校验和状态表分配；Q2 还检查历史 interval/burst 是否
  匹配当前策略。单数字原型对异常输入的校验不完全等价。
- 完整脚本组执行真实 Allow Lua，分别操作 1、4、32 个已有活跃额度。单数字版本
  替换读取/编码、移除策略比较，保留 GET、GCRA、MSET 和响应构造。
  这组测的是**状态表示、校验和编码的综合差异**，不能全归因于解析。
- 完整脚本每个样本 pipeline 1,000 次调用，检查每条均允许及 EVALSHA 调用数精确
  匹配。样本状态刷新不计入执行时间。
- 完整脚本使用 interval=1us、burst=1,000,000,000；微基准使用 interval=100,000us、
  burst=10，不是完全相同的负载。
- 报告各轮平均耗时的中位数，不是请求延迟 P50/P99。包含 Lua GC、服务端调度、
  内存分配和虚拟机噪声。仅覆盖合法、活跃、热数据和始终获准路径。
- 另以每次 200,000 次循环复测，验证规模变化后的稳定性。

## 计时陷阱

初测在脚本内两次调用 TIME，得到相同的缓存时间，导致微基准耗时全为 0。
该无效结果不纳入提交。保留的有效数据为 [50,000 次循环](results-redis8-valid.json)
和 [200,000 次循环](results-redis8-200k.json)，均使用 commandstats 计时。

未修改生产实现。

## 本机结果（2026-10-28）

被测生产源码版本：`2820790`（2026-10-28 checkout）。
Apple M3 Pro / arm64，OrbStack Linux 容器，Redis 8.10.2，关闭 RDB/AOF。
下列数字来自 50,000 次循环的有效结果，各轮平均耗时取中位数：

| 微基准 | 单数字 | Q2 | Q2 额外耗时 |
|---|---:|---:|---:|
| 解析 | 851.80 ns | 1323.50 ns | 471.70 ns（+55.4%） |
| 读取及校验 | 1031.16 ns | 1540.14 ns | 508.98 ns（+49.4%） |

共同循环基线 35.18 ns。200,000 次复测：解析 887.40 / 1321.85 ns，读取校验
1053.91 / 1544.74 ns；额外成本仍约 0.43–0.51us/额度。

| 完整 Allow 服务端执行 | 单数字 | Q2 | Q2 额外耗时 |
|---|---:|---:|---:|
| 1 个额度 | 5.319 us | 6.234 us | 0.915 us（+17.2%） |
| 4 个额度 | 13.676 us | 16.846 us | 3.170 us（+23.2%） |
| 32 个额度 | 83.663 us | 109.052 us | 25.389 us（+30.3%） |

结论：纯解析的相对差异明显，绝对增量约半微秒；在多额度、CPU 受限的服务端路径
中值得关注，但不能把这些百分比当作端到端延迟或线上吞吐变化，更不能把单数字
原型当作保持原契约的可替换实现。
