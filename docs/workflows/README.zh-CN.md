# 准入流程图

[English](README.md)

三张带源码依据的交互式流程图，分别追踪一次准入尝试。图中标签和说明卡片使用简体中文，Go API 与协议名称保持原文。

| 流程图 | 展开的操作 | 可编辑图源 |
| --- | --- | --- |
| [公共准入流程](admission.html) | Bind → Charge → 本地校验 → Driver → 决策、重放或错误 | [JSON](admission.json) |
| [PostgreSQL](postgres.html) | scope guard 行锁 → 后端时间 → 保留结果 → GCRA → quota/receipt 写入 → Commit | [JSON](postgres.json) |
| [Redis](redis.html) | 同槽 key → Lua → 后端时间 → 保留结果 → GCRA → 最后一次 MSET → 返回值解码 | [JSON](redis.json) |

## 如何查看

克隆仓库或下载 HTML 文件后，用浏览器打开。GitHub 文件页显示的是 HTML 源码，不会运行交互式查看器。查看已提交的 HTML 不需要启动后端或执行构建；查看器支持缩放、明暗主题、源码引用和导出。

JSON 是可编辑图源。每个 HTML 均内置查看器，读者无需安装 Archify。

## 范围与扣减语义

- 所有额度一起放行并扣减；任一额度不足则全部不扣。保留拒绝结果仍可能写入 receipt，PostgreSQL 还可能提交首次插入的 scope guard 行。“未扣额”不等于“零存储写入”。
- 有效且匹配的保留记录直接返回原放行/拒绝，不再扣减、不重算等待时间、不延长保留期。重放放行不授权再次执行业务。
- `NotCharged` 只描述本次调用；`Unknown` 不能排除此前已提交，不能据此盲目重试普通 `Allow`。
- 图中只展开一次准入，不包含业务执行、计费、退款、队列或后台维护 worker。说明卡片单独解释 `Wait`、`Lookup`、迁移和显式清理旁路。

## 源码快照与维护

三张图均引用
[`6a8d58fc558fb8896ac9b8919c62f1bd7f3f8109`](https://github.com/tr1v3r/pacelock/tree/6a8d58fc558fb8896ac9b8919c62f1bd7f3f8109)。
后续文件即使迁移，源码链接也固定指向该版本。这些是经过核对的快照，不会随实现自动更新。API、扣减、存储或恢复契约变化时，需要重新追踪实现并更新 JSON 引用，不能只替换版本哈希。

Redis 图的标签及说明卡片已更新为当前无版本前缀的额度/结果格式、无 Pending 状态及按键角色清理。固定源码链接早于此次清理；存储和错误契约以[当前 Redis 指南](../../driver/redis/README.zh-CN.md)为准。

已提交 HTML 由 Archify 3.2.0 生成。在安装该版本后，从仓库根目录重新生成；先将 `ARCHIFY_CLI` 设为安装目录中的 `bin/archify.mjs` 路径：

```sh
review=".archify/workflow-review-$(date +%Y%m%d-%H%M%S)"
for name in admission postgres redis; do
  node "${ARCHIFY_CLI:?Set ARCHIFY_CLI to the installed Archify CLI}" finalize workflow \
    "docs/workflows/$name.json" "docs/workflows/$name.html" \
    --repo-root . --quality showcase --json --out-dir "$review/$name" || exit 1
done
```

`finalize` 会验证图源与源码引用、生成 HTML、检查产物并执行浏览器自动校验；浏览器门禁要求本机可用 Chrome。修改候选图后使用新的 review 目录；不要提交机器本地的验收记录、截图或日志。HTML 是生成产物，应编辑 JSON，而不是直接修改 HTML 中内置的查看器代码。
