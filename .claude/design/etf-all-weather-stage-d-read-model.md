---
title: "ETF All-Weather Stage D Read Model"
status: draft
mode: "design"
created: 2026-05-05
updated: 2026-05-05
modules: ["backend", "frontend"]
---

# ETF All-Weather Stage D Read Model

## Overview

Stage C 已经把 ETF 全天候 v1 的本地数据基座落地：

- `reference.trading_calendar.full_history`
- `reference.rebalance_calendar.monthly_post_20`
- `reference.etf_aw_sleeves.frozen_v1`
- `market.etf_daily`
- `market.etf_adj_factor`
- `derived.etf_aw_sleeve_daily`

Stage D 的职责不是继续扩张数据源，也不是立刻实现完整策略引擎。Stage D 的正确起点是：

> 在 Stage C 数据基座之上，构建一个月度调仓读模型，让 workflow 和 dashboard 能稳定消费 ETF 全天候 v1 的可解释上下文。

这个阶段只做一条最小纵切：从已落盘的 sleeve daily panel 和 rebalance calendar 生成每个调仓日的 sleeve-level snapshot，并暴露给后端 workflow / API。后续的 regime scoring、risk budget、optimizer、backtest 可以基于这个 snapshot 继续演进。

## Relationship With Stage C

Stage D 直接依赖 Stage C 的输出，而不重新抓取 source data。

Stage C 已完成的能力：

- SH/SZ 共同开市日基础
- 每月 20 日后第一个共同开市日调仓规则
- ETF 全天候 v1 frozen sleeve universe
- 复权可用语义的 ETF sleeve daily panel
- market data 单位口径：`volume` 为手，`amount` 为千元人民币，未在 derived 层转换

Stage D 继续沿用这些边界：

- 不改变 frozen sleeve universe
- 不改变 post-20 monthly rebalance clock
- 不改变 adjustment-aware return convention
- 不在 derived builder 内发起 source fetch
- 不把 Stage D 做成通用 DAG runner

Stage D 对 Stage C 的新增要求是：Stage C 数据在生成 snapshot 前必须具备明确的新鲜度和覆盖状态。如果数据缺失、窗口不足或上游数据落后，Stage D 应返回可解释的 `missing` / `partial` / `stale` 状态，而不是静默生成半成品策略上下文。

## Stage D Goal

- [ ] 生成 ETF 全天候 v1 月度调仓 snapshot read model
- [ ] 每个 snapshot 以 `(calendar_name, rebalance_date, sleeve_code)` 为业务键
- [ ] 对每个 sleeve 提供调仓日可用价格、复权因子、复权收盘价、短中期收益、波动率和数据质量状态
- [ ] 提供 workflow / dashboard 可消费的后端查询入口
- [ ] 明确 missing data 与 stale data 的状态表达
- [ ] 为下一阶段 regime scoring / risk budget / backtest 提供稳定输入 contract

## Stage D Non-Goals

本阶段明确不做：

- 宏观 slow fields 接入
- rates / curve datasets 接入
- 通用 strategy engine
- 完整 backtest engine
- 组合优化器或 ERC 实现
- 动态权重推荐
- 实盘交易指令生成
- 多策略、多 universe、多 profile 通用框架
- scheduler / DAG runner
- 重新选择 ETF sleeve
- 重新定义调仓日规则

## Immediate Scope

### Inputs

Stage D 只读取已有 canonical / derived 数据：

- `canonical_rebalance_calendar`
- `canonical_sleeves`
- `data/lakehouse/derived/derived.etf_aw_sleeve_daily/`
- `etl_source_watermarks`

### Output

推荐新增一个窄范围 derived read model：

- `derived.etf_aw_rebalance_snapshot`

### Business Key

- `(calendar_name, rebalance_date, sleeve_code)`

### Consumer

初版消费方固定为：

- backend workflow context
- dashboard insight / context fallback

Stage D 不直接服务 portfolio CRUD，也不把 Portfolio 页面改成策略页面。

## Snapshot Contract

### Required Fields

每行代表一个调仓日上的一个 ETF sleeve。

建议字段：

| Field | Meaning |
| --- | --- |
| `calendar_name` | 调仓日历名称，初版固定 `etf_aw_v1_monthly_post_20` |
| `calendar_month` | 调仓月份，格式 `YYYY-MM` |
| `rebalance_date` | 调仓日 |
| `effective_date` | 策略生效日，初版等于 `rebalance_date` |
| `sleeve_code` | ETF sleeve code |
| `sleeve_role` | `equity_large` / `equity_small` / `bond` / `gold` / `cash` |
| `close` | 调仓日原始收盘价 |
| `adj_factor` | 调仓日复权因子 |
| `adj_close` | 调仓日复权收盘价 |
| `return_1m` | 约 1 个月复权收益 |
| `return_3m` | 约 3 个月复权收益 |
| `return_6m` | 约 6 个月复权收益 |
| `volatility_3m` | 约 3 个月日收益年化波动 |
| `max_drawdown_6m` | 约 6 个月复权最大回撤 |
| `data_status` | `complete` / `partial` / `missing` / `stale` |
| `quality_notes` | JSON text，记录缺口、窗口长度和计算口径 |
| `source_max_trade_date` | 该 sleeve 可用的最新 trade_date |
| `ingested_at` | snapshot 生成时间 |

### Initial Feature Semantics

Stage D 的 feature 只做 market confirmation，不做宏观状态判断。

初版计算规则：

- `return_1m`：调仓日前最近约 21 个交易观测的 `adj_close` 收益
- `return_3m`：调仓日前最近约 63 个交易观测的 `adj_close` 收益
- `return_6m`：调仓日前最近约 126 个交易观测的 `adj_close` 收益
- `volatility_3m`：最近约 63 个 `adj_pct_chg` 的年化标准差
- `max_drawdown_6m`：最近约 126 个 `adj_close` 的窗口最大回撤

这里使用“交易观测数量”而不是自然日天数，因为 Stage C 的 derived panel 采用 adjustment-available rows 语义。缺少复权因子的观测不会进入 panel。

### Missing Data Rules

必须显式表达缺口。

- 每个 `rebalance_date` 必须输出完整 5 个 frozen sleeve rows；缺失不能表现为少一行。
- 如果某个 sleeve 缺少调仓日可用数据，仍必须输出该 `(calendar_name, rebalance_date, sleeve_code)` row，并将 `data_status` 标记为 `missing`。
- 如果某个 sleeve 在 `rebalance_date` 没有可用行，但此前存在最近可用交易日，初版不做 forward-fill，标记 `missing`。
- 如果某个计算窗口长度不足，但调仓日行存在，标记 `partial`。
- 如果 `etl_source_watermarks` 显示 `market.etf_daily` 或 `market.etf_adj_factor` 明显落后于调仓日，标记 `stale`。
- 只有调仓日行存在、所需窗口满足最低长度、上游 watermark 不落后时，标记 `complete`。

这个约束是为了避免后续 workflow、dashboard、regime scoring 或 risk budget 误把“少一行”当成 universe 变化。ETF 全天候 v1 的 universe 是 frozen 的，缺失是数据质量状态，不是资产池变化。

最低窗口建议：

- `return_1m` 至少 15 个观测
- `return_3m` 至少 45 个观测
- `return_6m` 至少 90 个观测
- `volatility_3m` 至少 45 个观测
- `max_drawdown_6m` 至少 90 个观测

## Recommended Design

### 1. Add a Narrow Snapshot Builder

在 `tradepilot/etl/service.py` 或独立小模块中实现窄范围 builder。

推荐 profile name：

- `derived.etf_aw_rebalance_snapshot.build`

边界：

- 只支持 ETF 全天候 v1 snapshot
- 只读取 Stage C outputs
- 不发起 source fetch
- 不引入通用 strategy profile runner

### 2. Validate Before Write

Stage D 必须把数据验证作为 snapshot builder 的内置步骤。

这里的验证重点不是再次证明 Tushare 源数据真实，而是确认读模型本身满足策略和 workflow 消费所需的最小 contract：

- 业务键唯一
- 调仓日合法
- sleeve universe 合法
- 每个调仓日都有固定 5 个 sleeve rows；缺失数据必须用 `data_status = "missing"` 表达，不能通过少行表达
- 缺失、窗口不足和上游 stale 状态被显式标记
- price / adjustment / return / volatility / drawdown 字段可解释

验证失败时，builder 不应静默写入 `complete` snapshot。对于可表达的数据质量问题，应生成带 `data_status` 和 `quality_notes` 的 snapshot rows；对于结构性错误，例如重复业务键、非法 sleeve、非法调仓日，应返回 failed result。

### 3. Store Snapshot As Derived Parquet

初版继续写 lakehouse derived zone：

- `data/lakehouse/derived/derived.etf_aw_rebalance_snapshot/<year>/<month>/`

业务键覆盖规则：

- 对同一 `(calendar_name, rebalance_date, sleeve_code)` 执行幂等覆盖
- 重跑同一窗口不能产生重复业务键
- 同一分区内按 `calendar_name, rebalance_date, sleeve_code, ingested_at` 排序

是否同步写 DuckDB 表可以延后。初版 API 可以通过服务层读取 derived parquet，减少 schema churn。

### 4. Add a Read Service

新增一个窄范围 read service，而不是把 parquet 查询散落在 API 或 workflow 中。

推荐模块：

- `tradepilot/etl/read_models.py`

推荐函数：

- `get_latest_etf_aw_snapshot(as_of_date)`
- `list_etf_aw_snapshots(start, end)`

builder 仍由 ETL service / bootstrap profile 负责；read service 只负责读取和组装 workflow / API contract。如果后续需要 API，可以再在 `tradepilot/api/workflow.py` 或新增窄路由中调用 read service。Stage D 不应把查询逻辑直接写进 React。

### 5. Workflow Context Integration

Workflow 初版只需要消费最近一个可用 snapshot。

`build_context_payload()` 使用的是时间对齐语义：按 workflow run 的 `workflow_date` 查询该日期及以前最新的 rebalance snapshot。它不表示该 snapshot 由当前 workflow run 直接产出，也不使用 workflow run id 进行绑定。

建议输出形态：

```json
{
  "schema_version": "etf_aw_snapshot_v1",
  "calendar_name": "etf_aw_v1_monthly_post_20",
  "rebalance_date": "2026-04-20",
  "data_status": "complete",
  "sleeves": [
    {
      "sleeve_code": "510300.SH",
      "sleeve_role": "equity_large",
      "return_1m": 0.0123,
      "return_3m": -0.0345,
      "volatility_3m": 0.1832,
      "max_drawdown_6m": -0.0811
    }
  ]
}
```

Workflow 在 Stage D 中只展示和传递 context，不生成权重推荐。

### 6. Dashboard Integration

Dashboard 初版只展示“ETF 全天候上下文”：

- 最新调仓日
- 5 个 sleeve 的 1m / 3m / 6m returns
- 3m volatility
- 6m max drawdown
- data_status

不展示目标仓位，不展示买卖建议，不展示伪策略结论。

## Validation And Success Criteria

### Why Stage D Must Validate

Stage D 需要数据验证。

Stage D 是从“已落地数据”到“策略 / workflow 可消费上下文”的转换层。如果这里不验证，后续 regime scoring、risk budget、dashboard insight 会把缺口、重复行或过期数据误读成真实市场状态。

Stage D 的验证职责是读模型 contract 验证，不是 source truth 验证：

- Stage C 负责 source fetch、raw landing、normalization、基础 validation 和 lineage。
- Stage D 负责确认 snapshot 是否完整、幂等、可解释、可被消费。

因此 Stage D validation 必须进入 acceptance criteria，而不是只作为人工检查。

### Builder Validation

Stage D builder 必须验证：

1. snapshot 非空
2. 每个调仓日输出 5 个 frozen sleeve rows；缺失数据必须以 `missing` row 表达，不能少行
3. 不存在重复 `(calendar_name, rebalance_date, sleeve_code)`
4. 所有 snapshot 的 `rebalance_date` 来自 `canonical_rebalance_calendar`
5. 所有 `sleeve_code` 来自 active frozen sleeves
6. `adj_factor` 存在且为正，除非该 row 明确为 `missing`
7. `adj_close` 为正，除非该 row 明确为 `missing`
8. return / volatility / drawdown 字段为有限数或显式空值
9. 显式空值必须对应 `partial` / `missing` / `stale`，不能出现在 `complete` row
10. `quality_notes` 必须是可解析 JSON text
11. `source_max_trade_date` 不能晚于生成时可见的上游 watermark
12. `data_status` 只使用允许枚举

### Status Precedence

同一行可能同时满足多个异常条件，初版按以下优先级确定 `data_status`：

1. `stale`：上游 watermark 或 sleeve panel 最新交易日落后于 snapshot 目标调仓日
2. `missing`：输入已经覆盖目标调仓日，但调仓日 sleeve row 不存在，或核心 price / adjustment 字段缺失
3. `partial`：目标日 row 存在且上游不 stale，但计算窗口不足
4. `complete`：调仓日 row 存在、窗口长度满足要求、上游 watermark 不落后

这个优先级用于避免 `missing` 掩盖“上游数据还没有更新到 rebalance_date”的根因。`stale` 行应在 `quality_notes` 中写入 `stale_sources` 或 `source_lag`，便于 workflow 和前端解释。

### Freshness Validation

Stage D 必须读取上游 watermark：

- `market.etf_daily`
- `market.etf_adj_factor`
- `reference.trading_calendar`

如果 snapshot 目标调仓日超过上游 available range，不能返回 `complete`。

### Acceptance Criteria

Stage D 第一切片完成标准：

1. `derived.etf_aw_rebalance_snapshot.build` 可以针对指定日期窗口执行。
2. 对已完成的 Stage C 数据，能生成月度 sleeve-level snapshot。
3. 重跑同一窗口不会产生重复业务键。
4. 缺失调仓日行或窗口长度不足时，snapshot 明确标记 `missing` / `partial`。
5. 上游数据不新鲜时，snapshot 明确标记 `stale`。
6. 缺失 sleeve 不会导致 snapshot 少行；每个调仓日仍返回 frozen universe 的 5 行。
7. 后端可以读取最近一个 snapshot 并返回稳定 JSON contract。
8. Dashboard 或 workflow 可以消费该 contract，而不直接读 parquet。
9. Stage D validation 是自动化测试覆盖的一部分，不依赖人工检查。
10. Stage B / Stage C 现有测试继续通过。

## Testing Strategy

新增测试建议放在：

- `tests/etl/test_stage_d.py`

Required tests：

1. snapshot builder 从 fixture sleeve daily panel 生成 5 个 sleeve rows
2. builder 只使用 `canonical_rebalance_calendar` 中的调仓日
3. return / volatility / max drawdown 计算结果可预测
4. 调仓日缺少某个 sleeve 行时标记 `missing`
5. 调仓日缺少某个 sleeve 行时仍输出 5 个 frozen sleeve rows，不能少行
6. 历史窗口不足时标记 `partial`
7. watermark 落后时标记 `stale`
8. 重复执行不会产生重复业务键
9. read service 能返回 latest snapshot JSON contract

Frontend 如果接 dashboard：

- 至少跑 `cd webapp && yarn build`
- 若只改后端 API，则不强制改 frontend

## Recommended Implementation Sequence

1. 在 `tradepilot/etl/datasets.py` 注册 `derived.etf_aw_rebalance_snapshot`
2. 在 `tradepilot/etl/service.py` 增加窄 profile dispatcher
3. 实现 snapshot frame builder，先只用已有 derived daily panel
4. 实现 derived parquet 幂等覆盖写入
5. 新增 `tests/etl/test_stage_d.py`
6. 新增 read service，返回 latest snapshot contract
7. 接入 workflow context 或新增最小 API
8. 如果 API 已稳定，再接 dashboard 展示

## Deferred After Stage D

Stage D 完成后，后续阶段再考虑：

- Stage E：规则型 regime scoring 和 confidence score
- Stage F：risk budget 与简单 inverse-vol / ERC weight engine
- Stage G：monthly backtest 和 baseline comparison pack
- Stage H：shadow run dashboard 与人工复盘闭环
- macro / rates / curve 数据接入

## Final Judgment

Stage D 不应该直接开始写“全天候策略引擎”。

当前最有价值、风险最低的下一步是：

> 把 Stage C 已完成的数据基座变成可查询、可解释、可验证的月度调仓 snapshot read model。

只有 snapshot contract 稳定后，后续 regime scoring、risk budget、optimizer 和 dashboard insight 才有可靠输入。
