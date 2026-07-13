# ETF 全天候模拟盘持续运行设计

## 目的

本文定义 Stage O 已有 artifact 和 CLI 之上的最小持续运行层，使 ETF 全天候策略能够按固定节奏积累真实时间的 forward evidence。

运行层只负责编排、检查、告警和生成待人工处理的输出。它不修改 `shadow-run-design.md` 已冻结的 seed、decision、fill、observation 合同，不连接券商，不自动确认计划，不自动生成模拟成交，也不根据运行结果调参。

## 当前基础

当前仓库已经具备：

- Stage M 历史覆盖与固定成本稳健性报告。
- Stage N `DRAFT` 调仓计划。
- Stage O shadow account seed、人工 decision、人工 fill、daily observation、post-mortem 和 performance report。
- `update-local-shadow` 本地研究账户更新入口。
- Tushare 国债曲线不可用时的 AKShare `bond_china_yield` fallback 补齐路径；相关 pipeline 变更落地后仍需重跑历史 lakehouse 并确认覆盖结果。

因此下一步不再新增策略阶段，而是把已有命令组成可重复、可恢复、可审计的日常流程。

## 设计原则

1. **人工决策边界不变**：系统只生成 `DRAFT`，`CONFIRMED` / `CANCELLED` 必须由人工记录。
2. **人工成交边界不变**：fill 必须来自人工可核验输入，调度器不得推断或补造成交。
3. **artifact 是事实来源**：任务状态不能替代 frozen parquet 和 review 文件。
4. **失败关闭**：关键输入不完整时阻断下游写入，不以前值、未来值或部分向量继续运行。
5. **幂等重跑**：相同业务日期重复触发只能得到“已完成”或相同结果，不得覆盖历史 artifact。
6. **研究与运行分离**：运行期间不改变 strategy version、参数、sleeve universe 或成本场景。

## 运行角色

V1 只定义两个角色：

| 角色 | 职责 |
| --- | --- |
| `scheduler` | 触发只读检查和已有 CLI；记录运行结果；发送告警 |
| `operator` | 审核月度计划；记录 decision、fill 和异常备注；处理阻断 |

V1 不增加审批系统或权限平台。`operator` 仍通过 repo-visible 输入完成操作，输入中不得包含 token、账户凭据或券商信息。

## 每日流程

每日流程在 SSE 交易日收盘数据稳定后触发。具体 UTC 时间由部署环境配置，不写入策略合同。

```text
交易日确认
  -> 同步 ETF、日历、宏观和利率数据
  -> 数据质量与 freshness 检查
  -> 构建缺失的 shadow observation
  -> 检查 observation 连续性和 baseline 对齐
  -> 更新只读 performance report
  -> 记录 run summary；异常时告警
```

每日运行必须满足：

- 只为 canonical trade calendar 中的交易日生成 observation。
- 5 个 sleeve 的 `close` 必须完整、唯一、有限且大于零。
- 当日新增 fill 只能应用一次，且 `fill_at` 不晚于 observation 的价格时点。
- baseline 缺失不阻断账户估值，但相对指标保持为空并产生 warning。
- 任一关键校验失败时，不写 partial observation。
- 无人工 fill 时也必须生成 observation，以形成连续 forward 净值。

## 月度流程

月度流程只在 frozen rebalance calendar 的调仓日执行：

```text
完成当期数据同步
  -> 构建 strategy context
  -> 构建 risk budget
  -> 构建 target weight
  -> 更新 strategy / baseline kernel
  -> 生成 Stage M robustness report
  -> 生成 Stage N DRAFT rebalance plan
  -> 通知 operator 审核
  -> operator 记录 decision
  -> operator 按可核验价格记录 paper fill
  -> 后续每日 observation 和周期 post-mortem
```

月度运行约束：

- 一个 `calendar_name + rebalance_date + strategy_name + strategy_version` 只允许一套 frozen 上游 artifact。
- `partial` 可以进入研究型订单草案，但摘要必须展示降级原因和 confidence cap；`unavailable` 必须阻断。
- robustness report 继续保留 `research-only` caveat，不能成为自动确认依据。
- 已存在同一幂等键的 Stage N plan 时不得重建或覆盖。
- 调度器只通知待审核状态，不得超时自动确认或自动取消。

## 国债曲线补数

国债曲线补数是数据完整性任务，不是持续运行的硬前置条件。处理顺序为：

1. 重跑目标历史区间的 rates 同步，优先 Tushare `yc_cb`，权限不足时使用 AKShare `bond_china_yield` fallback。
2. 验证 `rates.gov_curve_points` 的 1Y、10Y 和交易日覆盖，不只检查任务成功状态。
3. 重建受影响区间的 strategy context、risk budget、target weight 和研究报告。
4. 对比重建前后的状态分布和权重差异，保留审计记录。

fallback 失败时，流程保持现有 `partial / degraded_research` 语义，不允许把缺失收益率填为零，也不阻塞独立的 market-only shadow 观察。

## 调度状态

调度器只需维护最小运行记录：

| 字段 | 含义 |
| --- | --- |
| `job_name` | 稳定任务名 |
| `business_date` | 业务日期 |
| `started_at` / `finished_at` | 运行时间 |
| `status` | `SUCCEEDED`、`BLOCKED` 或 `FAILED` |
| `input_versions` | 关键 artifact schema / strategy version 摘要 |
| `output_paths` | 本次生成的 repo-visible 输出 |
| `diagnostic_codes` | 结构化阻断或失败代码 |

运行记录用于运维审计，不参与策略计算。V1 不需要通用 DAG 引擎；使用 TradePilot 现有 scheduler 触发一个薄编排入口即可。

## 告警设计

只有需要人工处理的情况才告警，避免把正常降级变成噪音。

### 阻断告警

- 交易日历缺失或冲突。
- 5-sleeve 收盘价缺失、重复、非正或时点非法。
- 上游 target weight 为 `unavailable`。
- 同业务键 artifact 内容冲突。
- fill 超计划、现金不足、持仓不足或时间非法。
- 应生成但未生成 observation，且一次幂等重试后仍失败。

### 警告告警

- 国债曲线 fallback 失败，macro/rates context 降为 `partial`。
- baseline 当日缺失，导致相对指标为空。
- 月度计划已生成但超过一个交易日仍无人工 decision。
- confirmed plan 在预设观察窗口内仍未完全成交。
- 实际权重偏离目标超过报告既有阈值。

告警内容必须包含业务日期、账户、strategy version、diagnostic code 和 artifact 路径；不得包含 credential。

## 恢复与补跑

- 数据同步失败：修复数据源后按原业务日期重跑；下游未写入前不得跳过校验。
- observation 缺口：按交易日顺序从最早缺口补跑，不能先生成后续日期再回填历史状态。
- baseline 缺口：允许账户 observation 继续；baseline 修复后通过只读报告重算相对序列，不改账户事实。
- 错误 decision 或 fill：V1 不支持覆盖或冲销，停止流程并人工审计；不得直接编辑已发布 parquet。
- strategy version 变化：创建独立运行序列，不与既有 forward 曲线拼接。

## 实现切分

实现按三步推进，每一步都可独立验收：

1. **运行摘要**：新增薄编排入口，串联已有 CLI 并输出结构化 run summary。
2. **定时触发与告警**：接入现有 scheduler，只对阻断和需人工处理状态告警。
3. **连续运行**：冻结一个 paper account 和 strategy version，开始积累至少 3 个、目标 6 个调仓周期。

V1 不建设前端写操作、工作流引擎、消息队列、broker adapter 或自动重试平台。

## 验证要求

- 相同业务日期重复触发不新增重复 artifact。
- 非交易日运行得到可识别的 no-op，不生成 observation。
- 日线不完整时流程阻断且无 partial 写入。
- baseline 缺失时 observation 成功、相对指标为空且 warning 可见。
- 月度任务不会自动写 decision 或 fill。
- observation 缺口按日期顺序补齐，结果与逐日运行一致。
- 任务失败后可从 frozen 输入恢复，不需要修改历史 artifact。
- run summary 能定位全部输入、输出和 diagnostic code。

## 完成标准

- 每日与月度流程可以通过一个稳定入口重复执行。
- 连续性、数据时点、幂等性和人工决策边界均有自动检查。
- 失败不会产生 partial artifact，恢复路径经过测试。
- operator 能从告警直接定位需要处理的业务日期和 artifact。
- 至少一个完整调仓周期形成 plan、decision、fill、daily observation、post-mortem 和 performance report 的闭环。
