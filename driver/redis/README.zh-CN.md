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

默认先用 `UpdateQuotas` 发布正 revision 的策略，再用 `Allow(ctx, ...Charge{Name, Cost})` 或 `AllowOnce` 准入。无需本地绑定或策略刷新。显式静态替代路径是 `BindStatic` → `StaticLimiter`，通过 `q.StaticCharge(cost)` 构造封装的 `StaticCharge`，然后调用绑定对象的 `Allow` / `AllowOnce`。底层静态路径使用 `Rule` 与 `AllowStatic` / `AllowStaticOnce` / `WaitStatic`，Revision 必须为 0；没有默认动态 Wait。

每次静态绑定调用接受已绑定额度的任意非空子集，
每个选中额度仅提供一次消耗，且 `0 < cost <= Burst`；未传入额度既不检查也不扣减。
选中 StaticCharge 按绑定顺序整理，幂等指纹只包含选中额度的名称、整理后的顺序、有效策略、消耗及保留期限；
同一仍保留的 ID 改变子集返回 `ErrConflict`，调整传入 StaticCharge 顺序不改变指纹。多次独立调用合起来不具备原子性。
捕获的完整策略必须合法，且名称、向上取整到微秒的有效间隔和 Burst
须与绑定一致；合法且有效策略相同的不同写法可接受。空调用、重复、未知、无效或不匹配的 StaticCharge 在准入前
本地返回 `ErrInvalid` / `NotCharged`，不是后端 `ErrPolicyMismatch`。Rule API 与绑定接口共用 Driver 和存储，等待仍通过 `limiter.WaitStatic` 传入规则，不提供绑定等待方法。
详见[策略与消耗契约](../../README.zh-CN.md#固定策略与单次消耗)。

## 单命令提交与恢复

Lua 读取准入记录及本次涉及的额度，校验策略、计算 GCRA，提前编码全部值及响应，
最后仅执行一次 `MSET`。允许时提交最多 32 个独立的小额度值和一条完整结果；拒绝时
只提交结果；普通 `Allow` 只提交获准扣减的额度。不会读取或重写整个 domain 对象。
`Lookup` 查询权威主节点，不延长保留期限。

脚本收到明确的 MSET 命令拒绝（例如 ACL/OOM）时返回 `NotCharged`。没有明确拒绝响应
的错误，包括响应丢失，仍为 `Unknown`。提交后丢响应可通过保留的完整结果恢复。
这不保证崩溃或故障切换不丢数据，也不为上游执行去重。不要盲目重试普通 `Allow` 或退款。

具体客户端继续通过逐命令 NoRetry 禁止不确定重放，并保留有界 MOVED/ASK 重定向。
不支持 Ring；自定义 Scripter、hooks、工厂和代理须保证不重放及主节点路由。
全部准入显式传入债务键、策略键及可选结果键。Cluster 迁槽期间，部分键存在或缺失可能导致
`TRYAGAIN`，保守归类为 `Unknown` 传输错误，绝不隐式重放；是否重试或恢复由调用者决定。

## 默认动态策略

动态准入是默认路径。通过正数、单调递增的 revision 和
`limiter.UpdateQuotas(ctx, quotas...)` 发布配置，也可调用驱动的
`UpdateQuotas(ctx, pacelock.QuotaUpdate{Scope: scope, Quotas: quotas})`。
每批更新与准入原子串行，最后仅执行一次 MSET，复用 NoRetry 传输。
额度配置变更不会补充容量、重置或重标定债务；更新精确保留原始 TAT，即使债务超过缩小后的容量也不重置。
同 revision、同有效 interval/burst 可幂等重试；较低 revision 或同 revision 不同策略
返回 `ErrPolicyMismatch`。回滚参数必须发布更高 revision。`NotCharged` 描述额度扣减，
不表示不确定更新一定失败；传输错误或响应丢失仍为 `Unknown`，需要时由调用者显式重试同版本。

数据面只传名称和消耗：`limiter.Allow(ctx, pacelock.Charge{Name: "rpm", Cost: 1})`，
或 `AllowOnce(ctx, id, retention, charges...)`。驱动的必需 `Admit(Admission)` 接口在同一次
Lua 准入中读取后端最新策略，调用方无需刷新 binding 或传递 revision。配置缺失返回
`ErrPolicyMissing` / `NotCharged`。静态 Rule/BindStatic 接口在本地拒绝正 revision；对已配置额度
使用静态准入返回 `ErrPolicyMismatch`，即使空闲或清理后也不能绕过策略。

托管 `Rate=0` 返回 `ErrPaused` / `NotCharged`，不扣减、不写结果；即使消耗超过暂停额度的
burst 也优先返回暂停。非暂停时超 burst 消耗返回 `ErrInvalid`。用更高 revision 恢复，债务
随时间自然恢复。不提供动态 Wait；仅提供 `WaitStatic`。结果先于当前策略校验重放，跨更新、暂停也不变；
历史允许结果**不代表可以再次执行上游请求**。托管 fingerprint 绑定有序名称、消耗和保留期，
不包含策略版本。静态 fingerprint 编码有效规则，同 ID 的静态与托管请求冲突。

配置与债务分离：同槽 `policy:<name_hash>` 键永久保存 `P3|interval_us:burst:revision`，
interval 为零表示暂停。更新只读写配置，不读写债务或其过期时间；相同 revision 的等效更新
完全不写入。原额度键保存 `D3|tat_us` 托管债务，仍可读取 普通静态债务。Prune 回收过期债务，
但校验并永久保留配置；内存包含所有曾配置名称，不提供自动策略退役或重置。

激活前必须让**全部读取、写入及维护进程**升级为理解新协议的版本：旧写入方不检查配置键，
可能绕过策略。不支持混版本激活或未经转换回滚，空闲或清理也不会使回滚安全。
配置轮询和发布由应用负责，SDK 不启动工作线程。参见[可运行托管示例](examples/managed/main.go)。

## 逻辑过期与有界物理清理

键前缀为 `pacelock:{scope_hash}:`，结果记录使用 `attempt:` 后缀。值格式为：

```text
额度：tat_us:interval_us:burst（静态）或 D3|tat_us（托管债务）
策略：P3|interval_us:burst:revision（独立同槽键）
结果：expires_us|fingerprint|decision_json
```

静态额度在 TAT 时逻辑过期，表示债务已全部恢复；托管额度的债务也在 TAT 时恢复，但策略身份永不过期。
结果在 Redis 准入时间加 retention 后过期。即使键仍存在，读取也会忽略逻辑过期的静态状态。重放和查询不延长截止时间。

结果仅接受写入器规范 JSON：固定字段顺序、整数数值及 UTF-8 名称；先校验再判断过期。拒绝已移除的 Q2/E2/C1/P1 格式。
这些编码属于内部协议，应用不得自行构造。

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
- 短 Lua 按键角色重新读取候选值：`:attempt:` 键解析结果记录，额度键解析
  `tat:interval:burst`。全部候选校验通过后，一次 DEL 删除已过期额度和结果记录。
  格式损坏或角色不符时整批报错、不删除。旧扫描结果不能授权删除已被并发更新的活跃
  债务或新记录。
- 仅成功时使用 `Examined`、`Deleted`、`Cursor`。丢失清理响应后可安全重试；计数不构成
  恰好一次的审计账本。
- 游标不可透明解析或序列化，只在内存中保留并绑定作用域。重启或拓扑变化后从零开始。
  并发变化下 SCAN 不提供快照，需重复完整周期。清理失败会多占内存，不能据此删除活跃状态；
  应监控内存、清理滞后，并处理长期维护故障。
- 准入 ACL：`+evalsha +eval +get +mset +time`，另加客户端握手权限。
  清理额外需要 `+scan +del`，可使用独立维护身份；Cluster 还需 `+cluster|slots`。
  详见[安全策略](../../SECURITY.md)。

## 结果记录处理

结果记录使用逻辑过期，指纹按规则顺序编码为
`[retention_ms,step,burst,cost,name,...]`。损坏记录返回 `ErrState` / `Unknown`，
不会视为缺失或授权重新准入；应保留不确定性并排查，不自动删记录或退款。
详见[状态契约](../../docs/compatibility.md#redis-state)。

部署前安排清理并配置 ACL。不要通过改名或删键获得新容量。

使用主节点、TLS/认证、受控 ACL、noeviction 和明确的持久化配置。
[运维说明](../../docs/operations.md)、[核心用法](../../README.zh-CN.md)及
[Driver 契约](../../docs/driver.md)说明其他限制。不提供自动 PostgreSQL 故障切换或状态迁移。

在本模块执行 `REDIS_ADDR=localhost:16379 go run ./examples/llm`。
根目录执行 `sh scripts/check.sh` 检查全部模块；Redis 测试覆盖模型对照、并发、逻辑过期、
状态校验、结果记录处理、清理、ACL/OOM 和响应丢失故障。

旧协议与 MSET、候选清理的短时本地实测见[验证记录](../../docs/validation/redis-mset-local.md)，不代表生产 SLO。
历史 [Lua 编码对照实验](bench/lua-parse/README.zh-CN.md)提供当时额度编码的可复现程序和原始耗时；
其中单数字原型移除了策略校验，不是保持原契约的替换方案。
