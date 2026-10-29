# Redis Driver

[English](README.md)

独立模块：`github.com/tr1v3r/pacelock/driver/redis`，要求 Redis 7+。
应用负责初始化、持有并关闭 go-redis Client/ClusterClient。

```go
driver, err := redisdriver.New(client)
if err != nil { return err }
limiter, err := pacelock.New(driver, "llm-service", "provider-account-a")
if err != nil { return err }
if err := limiter.UpdateQuotas(ctx, pacelock.Quota{Name: "rpm", Policy: pacelock.Policy{
    Revision: 1, Rate: 60, Period: time.Minute, Burst: 10,
}}); err != nil { return err }
decision, err := limiter.Allow(ctx, pacelock.Charge{Name: "rpm", Cost: 1})
// 错误时忽略 decision，并检查 pacelock.OutcomeOf(err)。
```

导入 `redisdriver "github.com/tr1v3r/pacelock/driver/redis"` 和
`"github.com/tr1v3r/pacelock"`。构造不联网、不修改客户端配置。
一个 domain 对应一个 Cluster 槽。所有调用者需使用相同作用域和名称，动态策略以后端配置为准；
每次准入必须包含全部适用额度。

先用 `UpdateQuotas` 发布正 revision 的策略，再用 `Allow(ctx, charges...)` 或
`Wait(ctx, charges...)` 准入，无需本地绑定或策略刷新。这是唯一运行时准入路径。
驱动实现 `Admit(ctx, Admission) (Decision, error)`，请求只包含名称与消耗。
`Rule`、`Validate`、`Evaluate` 仅用于纯数学计算，不是运行时策略通道。
每次调用只检查传入的唯一额度名称；必须包含全部适用额度。空调用、重复名称与无效消耗
在本地拒绝，后端未配置名称返回 `ErrPolicyMissing`。多次调用合起来不具备原子性。

## 单命令准入

Lua 读取本次涉及的额度与策略，计算 GCRA，提前编码全部值及响应，最后仅执行一次
`MSET`。允许时提交最多 32 个独立的小额度值，拒绝时不写入。不会读取或重写整个 domain 对象。

脚本收到明确的 MSET 命令拒绝（例如 ACL/OOM）时返回 `NotCharged`。没有明确拒绝响应
的错误，包括响应丢失，仍为 `Unknown`。不保留准入结果，也不提供恢复查询；不要盲目
重试结果不确定的准入或退还可能已扣减的额度。Redis 崩溃、故障切换持久性及上游执行去重
不属于限流器契约。

具体客户端继续通过逐命令 NoRetry 禁止不确定重放，并保留有界 MOVED/ASK 重定向。
不支持 Ring；自定义 Scripter、hooks、工厂和代理须保证不重放及主节点路由。
全部准入显式传入债务键、策略键。Cluster 迁槽期间，部分键存在或缺失可能导致
`TRYAGAIN`，保守归类为 `Unknown` 传输错误，绝不隐式重放；是否重试或恢复由调用者决定。

## 后端托管动态策略

全部准入均由后端托管策略。通过正数、单调递增的 revision 和
`limiter.UpdateQuotas(ctx, quotas...)` 发布配置，也可调用驱动的
`UpdateQuotas(ctx, pacelock.QuotaBatch{Scope: scope, Quotas: quotas})`。
每批更新与准入原子串行，最后仅执行一次 MSET，复用 NoRetry 传输。
额度配置变更不会补充容量、重置或重标定债务；更新精确保留原始 TAT，即使债务超过缩小后的容量也不重置。
同 revision、同有效 interval/burst 可幂等重试；较低 revision 或同 revision 不同策略
返回 `ErrPolicyMismatch`。回滚参数必须发布更高 revision。`NotCharged` 描述额度扣减，
不表示不确定更新一定失败；传输错误或响应丢失仍为 `Unknown`，需要时由调用者显式重试同版本。

数据面只传名称和消耗：`limiter.Allow(ctx, pacelock.Charge{Name: "rpm", Cost: 1})`，
驱动的必需 `Admit(Admission)` 接口在同一次
Lua 准入中读取后端最新策略，调用方无需刷新 binding 或传递 revision。配置缺失返回
`ErrPolicyMissing` / `NotCharged`。不存在调用方策略覆盖通道，空闲或清理后也不能绕过策略。

托管 `Rate=0` 返回 `ErrPaused` / `NotCharged`，不扣减；即使消耗超过暂停额度的
burst 也优先返回暂停。非暂停时超 burst 消耗返回 `ErrInvalid`。用更高 revision 恢复，债务
随时间自然恢复。

`Limiter.Wait(ctx, charges...)` 在派发前只复制一次 Charge，每次普通尝试原子读取后端
最新策略，不快照策略。仅在有效容量拒绝后按建议的 `RetryAfter` 重试；任何错误都停止，
包括暂停、策略缺失、新 Burst 小于 Cost、传输错误（即使为 `NotCharged`）、`Unknown`、
不支持的能力及无效 Driver 结果。两次尝试间取消为 `NotCharged`；调用期间保留不确定性，
已确认成功不因并发取消变为失败。整次调用只产生一次 `wait` 观测。更新不会主动
唤醒休眠者，旧 RetryAfter 可能延迟使用放宽后的策略。不增加队列、worker、后台 goroutine、
公平性或退款。请设置 context deadline，详见[阻塞等待准入](../../README.zh-CN.md#阻塞等待准入)。

配置与债务分离：同槽 `policy:<name_hash>` 键永久保存 `P1|interval_us:burst:revision`，
interval 为零表示暂停。更新只读写配置，不读写债务或过期时间；同 revision 的等效更新不写入。
额度键保存 `D1|tat_us` 债务。只接受这两种精确格式；未知、损坏或角色错误的格式在变更前拒绝。
Prune 仅删除合法且完全恢复的 D1 债务，绝不删除策略或未知键。内存包含全部已配置名称，
不提供自动策略退役。标记 `1` 与正数单调递增的 `Policy.Revision` 无关。
不保证升级、迁移或历史存储兼容。配置轮询与发布由应用负责，SDK 不启动 worker。
参见[可运行托管示例](examples/managed/main.go)。

## 逻辑过期与有界物理清理

键前缀为 `pacelock:{scope_hash}:`。值格式为：

```text
额度：D1|tat_us
策略：P1|interval_us:burst:revision（独立同槽键）
```

债务在 TAT 时逻辑过期，表示容量已全部恢复；策略身份永不过期。
发布策略不会重置债务。每次准入都要求策略键存在。
Lua 协议为 `KEYS = [N 个债务键, N 个策略键]`、`ARGV = [N 个消耗]`，
没有模式选择器，也不接受调用方传入 interval/burst。
字段数量与十进制语法必须精确匹配；未知、损坏或角色错误的格式在变更前报错。编码属于内部协议，应用不得自行构造。

MSET 会清除原生 TTL，因此应用必须安排物理清理以控制内存。准入正确性依赖逻辑过期，
不依赖清理是否及时；但长期不清理会积累内存。对每个使用过的作用域（包括已空闲的作用域）
调度重复的完整清理周期。构造及准入不启动后台任务，也不扫描整个 domain。

```go
scope := pacelock.Scope{Namespace: "llm-service", Domain: "provider-account-a"}
var cursor redisdriver.PruneCursor // 在多次调度之间保留。
// 示例：本轮最多八步；maintenanceCtx 由调用方设置截止时间。
for step := 0; step < 8; step++ {
    page, err := driver.Prune(maintenanceCtx, scope, cursor, 32)
    if err != nil { return err } // 忽略 page，稍后用原 cursor 重试。
    cursor = page.Cursor
    if cursor.Done() {
        cursor = redisdriver.PruneCursor{} // 后续调度再开始新周期。
        break
    }
}
```

- `Prune` 要求 `1 <= limit <= MaxPruneKeys`（64）。每次最多检查 limit 个候选值，
  最多发现一页 SCAN。COUNT 仅为提示，不是扫描工作量或延迟硬上限；多出的键保存在游标中。
- 发现阶段按作用域路由到当前主节点，支持 Cluster。SCAN 实际遍历所在节点的键空间，
  再按作用域筛选；大量作用域重复扫描可能昂贵，需单独测量和安排维护预算。
- 短 Lua 重新读取额度与策略候选值，全部校验通过后一次 DEL 仅删除合法且完全恢复的 D1 债务，绝不删除策略或未知键。
  未知格式、格式损坏或角色不符时整批报错、不删除。旧扫描结果不能授权删除已被并发更新的活跃债务。
- 仅成功时使用 `Examined`、`Deleted`、`Cursor`。丢失清理响应后可安全重试；计数不构成
  恰好一次的审计账本。
- 游标不可透明解析或序列化，只在内存中保留并绑定作用域。重启或拓扑变化后从零开始。
  并发变化下 SCAN 不提供快照，需重复完整周期。清理失败会多占内存，不能据此删除活跃状态；
  应监控内存、清理滞后，并处理长期维护故障。
- 准入 ACL：`+evalsha +eval +get +mset +time`，另加客户端握手权限。
  清理额外需要 `+scan +del`，可使用独立维护身份；Cluster 还需 `+cluster|slots`。
  详见[安全策略](../../SECURITY.md)。

## 部署安全

部署前安排清理并配置 ACL，不要通过改名或删键获得新容量。未知键不属于清理候选，
不得使用宽泛前缀删除作为额度维护。

使用主节点、TLS/认证、受控 ACL、noeviction 和明确的持久化配置。
[运维说明](../../docs/operations.md)、[核心用法](../../README.zh-CN.md)及
[Driver 契约](../../docs/driver.md)说明其他限制。不提供自动 PostgreSQL 故障切换或状态迁移。

在本模块执行 `REDIS_ADDR=localhost:16379 go run ./examples/llm`。
根目录执行 `sh scripts/check.sh` 检查全部模块；Redis 测试覆盖模型对照、并发、逻辑过期、
状态校验、清理、ACL/OOM 和响应丢失故障。

旧协议与 MSET、候选清理的短时本地实测见[验证记录](../../docs/validation/redis-mset-local.md)，不代表生产 SLO。
历史 [Lua 编码对照实验](bench/lua-parse/README.zh-CN.md)保留当时额度编码的原始耗时（程序已移除）；
其中单数字原型移除了策略校验，不是保持原契约的替换方案。
