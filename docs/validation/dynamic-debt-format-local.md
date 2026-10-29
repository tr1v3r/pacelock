# 动态债务格式：D3 与纯数字的本地对比

> Historical measurement / 历史实验：2026-10-29，Asia/Singapore。
> 基线为 `bcc67d523b67269e42faaacfa0767bf72020b996`，即移除幂等回执后、移除静态入口前。
> 这里测的是动态准入，不是静态三段式。后续动态单路径、cost-only Lua 协议的性能需要重新测量。
> 当前首个动态格式已重新编号为 D1/P1；下文 D3/P3、样本、摘要与固定基线复现脚本均保留历史原貌，不能当作新标记实现的测量。当前兼容规则见[迁移说明](../compatibility.md#redis-state)。

**结论：这次实验中，将 `D3|TAT` 换成纯数字 TAT，完整动态准入的 Redis 服务端耗时下降约 2%～4%。没有足够理由仅为性能改变存储格式。**

English summary: on this historical baseline, a numeric-only debt prototype reduced full dynamic-admission server time by about 2–4%. This is not an end-to-end throughput/SLO result, a current-version benchmark, or a production-ready migration proposal.

- [原始 JSON（每组 21 轮采样及 Redis INFO）](dynamic-debt-format-local.json)
- [复现脚本](../../scripts/validation/dynamic-debt-format.py)

## 环境与方法

- 原始 `INFO server` 记录：Redis 7.4.11，standalone；Linux `7.2.9-orbstack-00391-g2b298d05fbd4`，aarch64，64 位。
- 本地容器实验，不是 Cluster。原始输出未保存完整 CPU 配额、持久化配置、allocator 或镜像 digest，不能据此宣称这些环境因素受控。
- 下方复现命令使用 `redis:7-alpine`；tag 会变化，应核对实际版本。该命令关闭 RDB/AOF，使用 noeviction；它不是原始容器配置的完整快照。
- Python 标准库 RESP 客户端，宿主机 loopback 连接容器。统计 `INFO commandstats` 的 `EVALSHA usec` 差值，**不是客户端墙钟时间，也不包含完整网络往返**。
- 3 轮预热不计入结果，21 轮测量，固定随机种子 `20261029` 打乱各变体的顺序。
- 微基准每次脚本循环 100,000 次，轮换 64 个预先生成的值；结果含循环/函数调用开销，未减去空循环基线。
- 完整脚本每个样本流水线执行 2,000 次；分别覆盖 1/4/32 个配额的允许与拒绝。配额状态与 P3 策略在计时区间外写入，每轮重置。
- 成功场景：每单位 1µs、Burst=1,000,000,000，初始债务为后端当前时间加 60 秒。拒绝场景：每单位 1 小时、Burst=1，初始债务为当前时间加 1 小时。每个 quota 的 Cost=1。
- 两种格式的 key 使用相同长度；策略 key 和准入计算不变。所有返回值都有允许/拒绝及维度数量校验。

源文件 SHA-256：

```text
protocol.lua 15411a9c1f8a84dc09eeb9d85d1cf50ea28cebca7a357cc34393593111f889b0
gcra.lua     51196c48d98c882deeb1552eb4dd461af33e3aad3c9897dd0d6bd57105528a5a
```

## 结果：各组中位数

### 微基准

| 操作 | ns/次 |
| --- | ---: |
| 循环/函数调用基线 | 35.89 |
| D3 前缀识别 + TAT 提取/数字转换 | 853.36 |
| 纯数字转换 | 678.68 |
| D3 完整债务读取/校验 | 993.24 |
| 纯数字完整债务读取/校验 | 855.29 |

完整读取/校验节省约 **138ns（13.9%）**。纯数字仍检查非负、整数、精确数值边界和债务时间上界。

### 完整动态准入

单位：Redis 服务端 µs/请求。差值和比例均由未舍入的组中位数计算。

| 结果 | 配额数 | D3 | 纯数字 | 节省 | 耗时下降 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 允许 | 1 | 6.300 | 6.177 | 0.123 | 1.95% |
| 允许 | 4 | 16.939 | 16.326 | 0.613 | 3.62% |
| 允许 | 32 | 110.697 | 107.200 | 3.496 | 3.16% |
| 拒绝 | 1 | 5.811 | 5.647 | 0.163 | 2.81% |
| 拒绝 | 4 | 15.993 | 15.492 | 0.501 | 3.13% |
| 拒绝 | 32 | 107.704 | 104.712 | 2.992 | 2.78% |

差异包含读取、解析和成功时的写入编码/存储，不只是 `string.match` 的成本。完整请求还需要 GET 策略、GET 债务、执行 GCRA、构造结果以及成功时 MSET，所以微基准的百分比不能直接套到整体请求。

## QPS 如何换算

按单个 Redis 主节点主线程估算，公式为：

```text
节省的主线程时间比例 = QPS × 每请求节省的 µs / 1,000,000
```

以允许路径计算：

| 每次配额数 | QPS 示例 | 节省的主线程占用约 |
| --- | ---: | ---: |
| 1 | 100,000 | 1.2 个百分点 |
| 4 | 50,000 | 3.1 个百分点 |
| 32 | 5,000 | 1.7 个百分点 |

这不是已实测达到的持续吞吐量，也不是端到端 CPU 使用率。客户端并发数、网络延迟、其他 Redis 命令、节点饱和程度都影响真实收益。远离饱和时通常不明显；接近饱和时几个百分点可能影响排队/尾延迟，但本实验没有测 p99，也不存在可直接推广的“达到某 QPS 就明显”的门槛。

## 限制与安全边界

1. 这是单机本地微基准，无置信区间或跨机器重复验证，不能成为容量承诺。
2. 纯数字是实验原型，不是已支持的协议。它保留正常值的数值约束，但 `tonumber` 接受的字符串语法比 D3 的十进制正则更宽，损坏状态的接受集合并不完全等价。
3. 原型不兼容读取现有 D3 数据；未测双格式迁移分支，也未提供生产迁移方案。不能直接拿它替换生产 reader。
4. 不涉及更改 P3 策略格式、版本栅栏或业务侧重试语义；不证明可以删除类型标记或策略校验。
5. 原始 JSON 是当时输出，保持不变。归档复现脚本仅增加 CLI、安全确认、固定 Git 基线/摘要校验和输出元数据；没有将后续脚本验证产生的数据混入原采样。

## 复现

仅在独立、可丢弃容器执行。脚本会覆盖固定 `bench:{d3}:` key，不能指向共享 Redis。源码由 Git 中的上述提交读取，不需要切换当前工作树；浅克隆须先取得该提交。`--disposable` 是必选确认，输出文件必须不存在。

```sh
docker run -d --name pacelock-dynamic-format-bench \
  -p 127.0.0.1:18379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no --maxmemory-policy noeviction
# 确认容器已启动且 Redis PING 返回 PONG，再测量。
docker exec pacelock-dynamic-format-bench redis-cli PING
python3 scripts/validation/dynamic-debt-format.py --disposable \
  --output /tmp/pacelock-dynamic-format-new.json
# 成功或失败后均清理本次专用容器，不要删除其他 Redis 实例。
docker rm -f -v pacelock-dynamic-format-bench
```

默认参数与原实验一致：`--rounds 21 --iterations 100000 --batch 2000`。脚本没有第三方 Python 依赖；缩小参数只能验证复现流程，不能复用原结论作为新测量。
