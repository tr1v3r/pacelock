# Pacelock

[![CI](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/pacelock/actions/workflows/ci.yml)
[English](README.md)

**面向 LLM API 的 Go 分布式限流库。**

Pacelock 使用 Redis 和加权 GCRA，让多个服务实例共同遵守一份额度。一次准入可以同时检查请求数和 token 数：全部通过才扣减，任一维度不足则拒绝，所有维度都不扣减。

当前是基础实现，API 尚处于实验阶段，不是完整的 LLM 网关。要求 Go 1.24+、Redis 7+。CI 验证 Redis 7；键设计满足 Redis Cluster 同槽要求，但暂未覆盖 Cluster 集成测试。

## 已实现

- 加权 GCRA：消耗一次请求或已知数量的 token。
- 一次调用最多 32 个具名额度的原子准入。
- 跨进程共享状态、Redis 服务端时间、空闲状态自动过期。
- 返回 `Allowed`、建议等待时间 `RetryAfter` 和所有阻塞维度名称。
- 校验超过突发容量的消耗，以及活跃状态的策略冲突。

当前没有队列、阻塞式 `Wait`、并发限制、分词、预留、退款、流式用量校正或幂等重放。[Airlock](https://github.com/tr1v3r/airlock) 处理并发准入，Pacelock 处理随时间恢复的消耗额度；组合使用不代表两者操作原子化。

## 安装

```sh
go get github.com/tr1v3r/pacelock
```

## 使用

```go
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    MaxRetries: -1,
    ContextTimeoutEnabled: true,
})
defer client.Close()

limiter, err := pacelock.New(client, "llm-service", "provider-account-a")
if err != nil {
    return err
}
decision, err := limiter.Allow(ctx,
    pacelock.Rule{
        Name: "model-a:rpm", Rate: 60, Period: time.Minute,
        Burst: 10, Cost: 1,
    },
    pacelock.Rule{
        Name: "model-a:input-tpm", Rate: 60000, Period: time.Minute,
        Burst: 12000, Cost: 1500,
    },
)
if err != nil {
    return err // 后端异常；可能无法确定是否已经扣减。
}
if !decision.Allowed {
    // 使用 RetryAfter 和 Limited 拒绝请求，或安排稍后重试。
    // RetryAfter 不会为下一次调用预留容量。
}
```

[完整可运行示例](examples/llm/main.go)：

```sh
REDIS_ADDR=localhost:6379 go run ./examples/llm
```

共同执行限流的实例必须使用相同的 namespace、domain、额度名称和有效策略。如果同时受供应商和租户额度约束，每次调用都要带上这些规则；库无法推断漏传的规则。不同 namespace/domain 相互独立。示例只扣除已知输入 token，不控制输出 TPM，也不会自动匹配供应商的计费或限流口径。

## 语义与运行边界

- `Rate / Period` 是恢复速率；`Burst` 是总瞬时容量，不是额外容量。Cost 必须为正，且不超过 Burst。
- GCRA 约束的是速率与突发包络，不是精确的固定分钟或滑动分钟计数器。参数需要符合供应商实际额度规则。
- 发射间隔向上取整到整微秒；Period 和突发容量完全恢复所需时间均不超过 24 小时。极高速率可能因取整而显著降低有效速率。
- 策略身份为有效发射间隔与 Burst。状态尚存在时不一致会返回 `ErrPolicyMismatch`；这用于发现部署冲突，不是永久配置存储。额度完全恢复后状态过期。
- 一个 domain 的键都落在 Redis Cluster 的同一槽。按原子判定需求划分 domain；全部放在一个 domain 会集中到一个分片。
- 禁用客户端自动重试（`MaxRetries: -1`）。Pacelock 只在 `NOSCRIPT` 时从 `EVALSHA` 回退到 `EVAL`。中间件、hooks、代理和集群路由可能另外引入重放；准入操作**不具备幂等性**。
- 派发前取消不会扣减。派发后丢失响应、超时或取消可能导致扣减结果未知，不应盲目重试或退款。任何错误下都应忽略 Decision，由业务明确决定故障策略。
- 使用独立的额度键与 Redis `noeviction`。淘汰、人工删除、时钟跳变、故障切换丢数据都可能削弱限制。脚本隔离不等于崩溃持久性或运行时错误回滚，本库也不是计费账本。

## 设计与后续方向

[设计说明](docs/design.md) 包含 GCRA 公式、Redis 状态、原子性边界、错误语义与 LLM 扩展设想：

1. 请求/token 联合额度：已实现已知 Cost 的准入。
2. 输出 token 预留与实际用量校正：设计提案。
3. 幂等准入与显式 attempt 身份：设计提案。

## 验证

```sh
go test ./...
go vet ./...
# 未设置该变量时，真实 Redis 集成测试会跳过。
PACELOCK_REDIS_ADDR=localhost:6379 go test -race -count=1 ./...
```

CI 使用 Redis 7，运行 Go 1.24 和 stable Go 的集成测试。覆盖多维拒绝、跨客户端加权竞争、策略冲突、损坏状态、过期恢复、命名空间隔离和派发前取消。

MIT 许可证。
