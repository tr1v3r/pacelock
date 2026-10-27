# Redis Driver

[English](README.md)

独立模块：`github.com/tr1v3r/pacelock/drivers/redis`，要求 Redis 7+。
使用 go-redis v9，应用负责初始化、持有并关闭 Client/ClusterClient。

```go
driver, err := redisdriver.New(client)
if err != nil { return err }
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
```

导入 `redisdriver "github.com/tr1v3r/pacelock/drivers/redis"` 和
`"github.com/tr1v3r/pacelock"`。构造函数不联网、不修改客户端配置。
一个 domain 对应一个 Cluster 槽。所有实例需使用相同作用域、额度名称和有效策略，
每次请求需完整包含所有适用额度。

通过 `limiter.Bind(quotas...)` 在本地复制固定策略，再调用 `bound.Allow(ctx, charges...)`
或 `bound.AllowOnce(ctx, id, retention, charges...)`。每次必须为全部已绑定额度各提供一次消耗，
Charge 会按绑定顺序整理。详见[策略与消耗契约](../../README.zh-CN.md#固定策略与单次消耗)。

v1 Lua、键哈希及准入记录协议保持不变。具体客户端通过逐命令 NoRetry 禁止不确定重放，
保留有界 MOVED/ASK 重定向；不支持 Ring。自定义 Scripter、hooks、节点工厂和代理需自行
遵守不重放与主节点路由契约。脚本运行时错误可能部分写入：检查 OutcomeOf，恢复仍保留的
准入 ID，不盲目退款。

使用主节点、TLS/认证、受控 ACL、noeviction 和明确的持久化配置。
详见[运维说明](../../docs/operations.md)、[核心用法](../../README.zh-CN.md)
与 [Driver 契约](../../docs/drivers.md)。不提供自动 PG 故障切换或状态迁移。

在本模块运行示例：`REDIS_ADDR=localhost:16379 go run ./examples/llm`。
根目录执行 `sh scripts/check.sh` 检查全部模块；`internal/engine` 保留并发、Cluster、ACL、
模型对照和传输测试。
