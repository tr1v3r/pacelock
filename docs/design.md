# Pacelock 设计说明

当前实现已知正数消耗的多维加权 GCRA 原子准入，不实现计费、退款、业务执行、队列、worker 或并发许可所有权。此次 pre-v1 破坏性变更移除幂等准入与结果恢复，详见[迁移说明](compatibility.md)。

## 公开契约与职责

```go
New(driver Driver, namespace, domain string) (*Limiter, error)
NewWithOptions(driver Driver, namespace, domain string, options Options) (*Limiter, error)
(*Limiter).UpdateQuotas(ctx context.Context, quotas ...Quota) error
(*Limiter).Allow(ctx context.Context, charges ...Charge) (Decision, error)
(*Limiter).Wait(ctx context.Context, charges ...Charge) error
```

核心仅依赖标准库，负责输入及结果校验、等待与同步观测。Driver 负责服务端时间、并发同步、状态持久化和错误扣减分类。应用管理客户端、deadline、额度命名及适用集合；构造不联网、不关闭客户端。Redis 复用 go-redis 的连接池、认证、TLS、Cluster 路由及脚本缓存；PostgreSQL 使用 database/sql。

`Admission` 只包含 Scope 和 Charges；`Driver.Admit(ctx, Admission)` 返回 `(Decision, error)`。
可选 `QuotaDriver` 发布动态配置；不支持的可选能力返回 `ErrUnsupported` / `NotCharged`。
`Rule`、`Validate`、`Evaluate`、`EvaluateManaged` 和 `ApplyPolicies` 保持为离线纯计算工具。

## 后端权威策略

默认 `New` 构造动态 Limiter。控制面以 `UpdateQuotas` 发布正数且单调递增的 Revision，最大 9007199254740991；数据面只发送 `Charge{Name, Cost}`。同版本同有效策略只读成功，旧版本或同版本冲突返回 `ErrPolicyMismatch`。更新不读取、重置、重标定或缩短旧债务；缩小 Burst 后超过容量的债务继续自然恢复。`Rate=0` 暂停，准入返回 `ErrPaused` / `NotCharged`；缺失配置返回 `ErrPolicyMissing`，不默认无限额度。

配置可以写在代码中，但必须先显式发布。后端权威执行支持新旧 pod 滚动共存；旧配置发布被拒绝时须明确处理或上报，不能吞掉所有错误。回滚以更高版本发布旧参数。每次选中额度的 `Decision.Limited` 按请求 Charge 顺序返回；多次调用合起来不具备原子性。

安装策略前必须退役旧静态写入者或停止其静态路径。PostgreSQL 先显式 Migrate，再让新版进程处理流量。迁移保留旧债务与历史回执，不自动清空状态。

## GCRA 与原子边界

```text
I         = ceil(Period / Rate)，向上取整到微秒
B         = Burst，总瞬时容量而非额外容量
C         = Cost，本次已知正数消耗
candidate = max(TAT, now) + C * I
retry     = max(0, candidate - B * I - now)
allowed   = 所有选中额度的 retry 均为 0
成功      → 一起保存全部 candidate
拒绝      → 不扣减任何额度，RetryAfter 为最大 retry
```

Period 和 Burst × I 最多 24 小时。GCRA 是速率/突发包络，不是精确固定或滑动窗口计数器。已知输入 token 不控制未知输出 token；字符、字节、SSE chunk 不是 token。

Redis 在同槽 Lua 中读取 TIME、配置和债务，校验并完成编码后以一次 MSET 提交全部通过的额度。兼容读取历史债务格式 `tat:interval:burst`，动态债务使用 `D3|tat`，永久配置使用 `P3|interval:burst:revision`。债务在 `tat <= now` 逻辑过期；MSET 不配置原生 TTL，应用调度有界 Prune 清理，永久配置和版本栅栏不删除。详见[Redis 指南](../driver/redis/README.zh-CN.md)。

PostgreSQL 锁定 domain 行后采样 clock_timestamp，在短事务内读取与写入全部额度；确认回滚为 NotCharged，提交丢响应为 Unknown。它不加入业务事务。后端之间不自动迁移债务，不提供联合事务或自动故障切换。

同一 namespace/domain 的相关额度共用原子边界。共享全局额度可能形成热分片；任意分片不能保留跨槽原子性。调用方必须完整选择适用额度，库不能推断漏掉的规则。

## 阻塞等待

Wait 使用调用方 goroutine 内的私有循环，先立即准入，仅在有效的普通容量拒绝后按 RetryAfter 休眠。动态 Wait 在派发前复制一次 Charges，每次尝试原子读取最新后端策略。

任何错误立即结束，包括暂停、缺失配置、新 Burst 小于 Cost、传输错误（即使 NotCharged）、Unknown、不支持能力和无效 Driver 结果。两次尝试之间取消为 NotCharged；后端调用期间保留扣减确定性，确认成功不会被并发取消覆盖。休眠不持有锁、连接或事务，不提供 FIFO、公平性、容量预留或后台 worker。策略更新不主动唤醒休眠者，请用 context deadline 限制等待。

## 失败、观测和升级

错误保留 errors.Is/As 与底层链，Outcome 只有 NotCharged 或 Unknown。前者只说明本次调用确定未扣减，不能消除先前调用的不确定性。任何错误都应忽略 Decision。**Unknown 可能已经扣减，不要盲目重试；不确定写入后没有恢复接口。**业务失败不会退还容量。

具体 go-redis 客户端的 SDK 脚本带逐命令 NoRetry，阻止不确定重放，保留有界 MOVED/ASK 路由以及 NOSCRIPT 安全回退。自定义客户端、hooks、工厂和代理须保持不重放契约。Redis 隔离和一次提交不能保证所有崩溃/故障切换后的持久性；TIME 是墙钟，前跳可能提前恢复容量，后跳可能延迟或拒绝。

Observer 同步且并发调用，不创建后台任务。操作为 allow、wait；Wait 每个完整调用仅一次事件。事件含数量、耗时、结果、等待时间和错误，不含重放字段；回调须快速并发安全，panic 向外传播。不要将原始名称或错误当作指标标签。

已移除 AllowOnce、Lookup、Receipt、LookupRequest、准入 ID/Retention/Fingerprint 和 Event.Replayed。升级不自动删除旧 Redis attempt key 或 PostgreSQL admissions 表/行；停止旧写入者后由运维显式规划备份和清理，不触碰活跃债务与永久配置。历史验证和图示不是当前接口承诺。

## 验证与参考

共享契约覆盖原子拒绝、加权跨客户端竞争、策略冲突、取消、动态策略变更、Wait、Unknown、PostgreSQL 回滚及提交丢响应。Driver 测试另覆盖真实 Redis/Cluster、状态异常、清理与故障边界。历史测量见[验证记录](verification.md)，当前契约见[Driver 说明](driver.md)。吞吐、持久性、时钟故障和目标环境 SLO 必须独立验证。

- [Redis Lua scripting](https://redis.io/docs/latest/develop/programmability/eval-intro/)
- [go-redis](https://github.com/redis/go-redis)
