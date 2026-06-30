# ETF 全天候当前设计文档

## 定位

`.claude/allweather/` 定位为 **ETF 全天候设计资料库**。

它的职责是保存外部项目 deep-read、公式说明、阶段性判断和可迁移工程经验。它不是 TradePilot 的运行时代码、不是当前实现的验收清单，也不直接决定后续开发顺序。

正式实现以本仓库已经落地的 ETL、read model、workflow、Dashboard 合同为准。资料库只在以下场景作为参考输入：

- 判断哪些外部模块值得吸收。
- 解释风险平价、动态风险预算、实盘约束等设计来源。
- 约束复杂模型的引入顺序，避免过早把 CNN、Transformer、期权对冲接入主链。

## 当前结论

TradePilot ETF 全天候不是 ETF 涨跌预测器，而是低频风险配置系统。

当前主线应保持为：

```text
冻结 sleeve universe
-> adjustment-aware ETF 日频面板
-> 月度 rebalance snapshot
-> 市场状态/宏观利率上下文
-> 策略上下文
-> 后续风险预算与组合建议
-> Dashboard / workflow 展示
```

`.claude/allweather/` 资料库补充的核心判断是：

```text
状态识别 -> 动态风险预算 -> 风险平价权重 -> 实盘约束
```

这条链应作为下一阶段策略计算层的设计方向，但不能覆盖当前已经完成的数据和上下文基座。

## 已有进度

### Stage B：真实数据接入切片

已完成 Tushare 真实数据接入验证，覆盖：

- `reference.instruments`
- `reference.trading_calendar`
- `market.etf_daily`
- `market.index_daily`
- raw Parquet 落地
- normalized Parquet 分区重写与去重
- dependency preflight、validation gating、watermark advancement

关键结果记录在 `docs/stage-b-ingestion-real-data-test-report.md`。

### Stage C：ETF 全天候 v1 数据基座

已完成本地回补：

- `reference.trading_calendar.full_history`
- `reference.rebalance_calendar.monthly_post_20`
- `reference.etf_aw_sleeves.frozen_v1`
- `market.etf_daily`
- `market.etf_adj_factor`
- `derived.etf_aw_sleeve_daily`

当前 v1 frozen sleeves：

| Role | Code | 用途 |
| --- | --- | --- |
| `equity_large` | `510300.SH` | 大盘权益 |
| `equity_small` | `159845.SZ` | 小盘权益 |
| `bond` | `511010.SH` | 债券防御 |
| `gold` | `518850.SH` | 黄金/压力对冲 |
| `cash` | `159001.SZ` | 现金/中性缓冲 |

`derived.etf_aw_sleeve_daily` 使用 adjustment-aware 语义。ETF daily `volume` 继承 Tushare `fund_daily` 的 `vol` 单位：手；`amount` 单位：千元人民币，不在 derived 层做单位转换。

关键结果记录在 `docs/stage-c-data-backfill-report.md`。

### Stage D：月度调仓快照

已实现 `derived.etf_aw_rebalance_snapshot.build` 和 read model：

- 每个 rebalance date 生成 5 个 sleeve 行。
- 输出 1M/3M/6M 收益、3M 波动、6M 回撤、数据状态和诊断信息。
- read model 输出 `etf_aw_snapshot_v1` / `etf_aw_snapshot_contract_v1`。

该层是 Dashboard 当前 ETF 全天候面板的主要数据来源。

### Stage E：市场侧状态评分

已实现 `derived.etf_aw_regime_score.build`：

- 只使用 rebalance snapshot 中的市场表现指标。
- 输出 `market_regime_label`、`market_score`、`confidence_score`、`scoring_status`。
- 支持 `risk_on`、`hedge_bid`、`defensive`、`mixed`、`insufficient_data` 等市场状态。
- 因为仍是 market-only scoring，置信度有上限，不能被解释为完整宏观 regime。

这是当前“状态识别”的第一版，不是最终的动态风险预算引擎。

### Stage F：宏观/利率上下文

已实现宏观和利率数据上下文的 read model 能力，涉及：

- `macro.slow_fields`
- `rates.daily_rates`
- `rates.lpr`
- `rates.gov_curve_points`
- `get_latest_etf_aw_macro_rates_context`
- `list_etf_aw_macro_rates_contexts`

该层把 PMI、SHIBOR、LPR、国债收益率曲线等字段汇总成 point-in-time 上下文，并保留缺失字段、陈旧字段、来源 caveat、修订 caveat。

### Stage G：策略上下文

已实现：

- `derived.etf_aw_market_features.build`
- `derived.etf_aw_strategy_context.build`
- `get_latest_etf_aw_market_features`
- `get_latest_etf_aw_strategy_context`

策略上下文目前只做上下文汇总和 readiness 判断，不输出：

- `target_weight`
- `trade_action`
- `order_instruction`

这是正确边界。当前系统已经能把市场快照、市场状态和宏观/利率上下文拼成 research-ready 或 degraded context，但还没有进入正式组合权重和交易建议层。

### Workflow / Dashboard

后端 workflow 已暴露 ETF 全天候上下文：

- `get_latest_etf_aw_context`
- `get_latest_etf_aw_regime_context`
- `get_latest_etf_aw_strategy_context`

前端 Dashboard 已展示 ETF 全天候 snapshot 表，字段包括 sleeve、代码、1M/3M/6M 收益、3M 波动、6M 回撤、状态和诊断。

当前前端主要展示 snapshot；strategy context 后续可作为 insight-first 面板的输入。

## 资料库吸收边界

### 直接吸收

来自 `.claude/allweather/allweather-hedging-deep-read/`：

- 风险平价求解器思想。
- VaR/CVaR/MRC/RC 风险指标。
- 清晰 schema 和 pipeline 分层。

这些应进入后续 `risk_budget -> target_weight` 计算层。

### 改造后吸收

来自 `.claude/allweather/chip-analysis-deep-read/`：

- 整数手数约束。
- 交易成本过滤。
- 换手限制。
- 现金缓冲。
- 风控过滤器链。
- MWU 多专家融合思想。

这些只应在权重建议稳定后进入执行约束层，不应提前污染当前上下文层。

来自 `.claude/allweather/rv-transformer-cta-deep-read/`：

- `Mock/Paper/Live` 执行接口思想。
- 概率门控。
- 测试结构。

这些适合未来 shadow / live pilot 阶段，不属于当前 research context 的必需项。

### 暂缓吸收

以下内容暂不进入 v1 主链：

- CNN 直接做配置决策。
- Transformer / L2 订单簿模型。
- 个股级期权对冲。
- “预测偏差 -> 逆向交易”实验。
- 在合成数据上优化的参数。

原因是当前系统的主要缺口不是模型复杂度，而是可解释状态识别、动态风险预算、组合权重验证和实盘约束。

## 目标架构

### 已落地层

```text
Tushare / project ETL
-> raw / normalized lakehouse
-> canonical DuckDB metadata
-> derived.etf_aw_sleeve_daily
-> derived.etf_aw_rebalance_snapshot
-> derived.etf_aw_regime_score
-> derived.etf_aw_market_features
-> derived.etf_aw_strategy_context
-> workflow context
-> Dashboard snapshot panel
```

### 下一阶段新增层

```text
strategy_context
-> regime/budget mapper
-> covariance estimator
-> budgeted risk parity or inverse-vol engine
-> target sleeve weights
-> rebalance threshold and cost filter
-> explainability table
-> shadow recommendation record
```

新增层必须保留两个硬边界：

1. `strategy_context` 是输入上下文，不应包含目标权重或交易动作。
2. `target_weight` 和 `trade_action` 必须来自后续明确命名的数据集，不能混入现有 Stage G 合同。

## V1 策略计算设计

### 输入

第一版策略计算只依赖已经落地或明确可落地的数据：

- `derived.etf_aw_rebalance_snapshot`
- `derived.etf_aw_regime_score`
- `derived.etf_aw_strategy_context`
- `derived.etf_aw_sleeve_daily`

宏观/利率字段可用于上下文和置信度校正，但在未完成充分验证前，不应单独触发大幅仓位切换。

### 状态到预算

V1 先使用规则映射，不使用机器学习分类器。

建议输出：

- `budget_status`
- `budget_basis`
- `base_risk_budget`
- `tilted_risk_budget`
- `confidence_score`
- `budget_notes`

市场状态初始映射应克制：

| Market regime | 预算倾向 |
| --- | --- |
| `risk_on` | 适度提高权益风险预算，降低现金/防御预算 |
| `hedge_bid` | 提高黄金和现金/防御预算，压低权益预算 |
| `defensive` | 提高债券和现金预算，压低权益预算 |
| `mixed` | 接近中性预算 |
| `insufficient_data` | 回到保守中性预算 |

置信度只控制偏移幅度，不直接决定极端仓位。

### 协方差估计

V1 使用 `derived.etf_aw_sleeve_daily` 的 adjusted return：

- 默认窗口：3M 或 6M，需在设计里固定。
- 最小样本数不足时降级为 inverse-vol 或中性权重。
- 协方差矩阵必须处理缺失、停牌和现金 sleeve 的低波动问题。

资料库里的 CNN vol/corr/tail 只能作为未来增强，不进入 V1。

### 权重引擎

优先级：

1. budgeted inverse-vol approximation
2. simplified ERC / risk parity
3. later: learnable ERC

V1 输出必须包含 explainability：

- 输入预算。
- 输入波动率/协方差摘要。
- 原始目标权重。
- 约束后目标权重。
- 降级原因。

### 实盘约束

权重稳定前只做纸面约束设计，不生成真实订单。

后续执行约束包括：

- 单 sleeve 上限。
- 最小交易金额。
- ETF 最小交易单位。
- 现金缓冲。
- 换手上限。
- 交易成本过滤。
- 折溢价/流动性异常过滤。

这些约束来自 `.claude/allweather/` 对 chip-analysis 的工程纪律总结，但 TradePilot 需要重新实现，不能复制外部项目的交易系统假设。

## 数据合同建议

下一阶段建议新增独立 derived 数据集，而不是扩展 Stage G：

### `derived.etf_aw_risk_budget`

粒度：

```text
calendar_name + rebalance_date + strategy_name + sleeve_role
```

核心字段：

- `schema_version`
- `calendar_name`
- `rebalance_date`
- `strategy_name`
- `sleeve_role`
- `base_budget`
- `tilted_budget`
- `confidence_score`
- `budget_status`
- `budget_basis`
- `quality_notes_json`
- `source_strategy_context_rebalance_date`
- `ingested_at`

### `derived.etf_aw_target_weight`

粒度：

```text
calendar_name + rebalance_date + strategy_name + sleeve_code
```

核心字段：

- `schema_version`
- `contract_version`
- `calendar_name`
- `rebalance_date`
- `effective_date`
- `strategy_name`
- `sleeve_code`
- `sleeve_role`
- `raw_target_weight`
- `constrained_target_weight`
- `current_weight`
- `target_weight_status`
- `optimizer_name`
- `optimizer_basis`
- `turnover_estimate`
- `quality_notes_json`
- `source_risk_budget_rebalance_date`
- `source_snapshot_rebalance_date`
- `ingested_at`

交易建议应再单独建模，例如 `derived.etf_aw_rebalance_plan`，避免把权重和订单混在一起。

## 阶段设计文档

后续阶段需要单独设计文档。原因是 Stage B-G 已经形成稳定上下文合同，后续每一层都会引入新的业务语义和验证责任；如果继续把所有细节塞进总设计文档，会把状态识别、风险预算、权重优化、交易约束和回测纪律混在一起。

总设计文档只保留方向、边界和阶段顺序。每个后续阶段在实现前先冻结一份独立设计文档。

### 必需文档

1. `risk-budget-design.md`

   范围：

   - `derived.etf_aw_risk_budget` schema。
   - read model contract。
   - `strategy_context -> sleeve risk budget` 映射规则。
   - base budget 与 tilted budget。
   - confidence 只控制偏移幅度的规则。
   - `complete`、`partial`、`stale`、`missing`、`unavailable` 降级行为。
   - 单元测试和 fixture 边界。

   非范围：

   - 目标权重。
   - 交易建议。
   - 订单或执行约束。

2. `target-weight-design.md`

   范围：

   - `derived.etf_aw_target_weight` schema。
   - budgeted inverse-vol MVP。
   - simplified ERC 是否值得引入的判断标准。
   - 协方差窗口、最小样本数、缺失数据处理。
   - cash sleeve 低波动处理。
   - raw target weight、constrained target weight、降级原因和 explainability 字段。
   - 与等权、静态 inverse-vol、静态风险平价的 baseline 对比要求。

   非范围：

   - 自动下单。
   - 当前持仓驱动的交易计划。
   - 实盘 broker / QMT / XtQuant 接口。

### 延后文档

3. `rebalance-plan-design.md`

   只有在 `target-weight-design.md` 对应实现稳定后再写。

   范围：

   - 当前持仓输入。
   - 目标权重到 paper rebalance plan。
   - 换手估算。
   - 成本过滤。
   - 最小交易金额和 ETF 最小交易单位。
   - 现金缓冲。
   - 不自动下单的人工确认边界。

4. `shadow-run-design.md`

   只有在 rebalance plan 能稳定生成后再写。

   范围：

   - 月度 freeze 流程。
   - forward observation。
   - post-mortem 模板。
   - Dashboard / workflow insight 展示边界。

## 验证标准

任何策略计算层进入 Dashboard 或 workflow insight 前，至少满足：

- 不改变 Stage B-G 已有合同。
- 能在无外部网络的测试 fixture 中构造 deterministic 输入。
- 对 `complete`、`partial`、`stale`、`missing`、`unavailable` 均有降级行为。
- 输出不能包含未来数据。
- 输出可解释表能逐月说明状态、预算、权重和降级原因。
- 与等权、静态 inverse-vol、静态风险平价做 baseline 对比。
- 成本和换手至少以估算形式进入评估。

## 近期执行顺序

1. 冻结本文档为当前设计入口。
2. 更新旧 `progress-status.md`，避免文档状态继续停留在 “schema not done”。已完成。
3. 新增并冻结 `risk-budget-design.md`。
4. 实现 `derived.etf_aw_risk_budget` schema、read model 和规则式 mapper。
5. 新增并冻结 `target-weight-design.md`。
6. 实现 `derived.etf_aw_target_weight` 和 budgeted inverse-vol MVP。
7. 增加 monthly explainability table 和 baseline comparison。
8. 再评估是否引入 simplified ERC。
9. 目标权重稳定后，再新增 `rebalance-plan-design.md`。

## 非目标

当前阶段不做：

- 自动实盘下单。
- 个股选择。
- 高频交易。
- 全机器学习状态分类。
- 期权对冲。
- CTA / 海外 / 商品 sleeve 扩展。
- 对 `.claude/allweather/` 外部项目代码的直接搬运。

## 设计原则

1. 先让每月决策可解释，再追求优化器复杂度。
2. 先保证 point-in-time 数据纪律，再谈回测收益。
3. 先输出上下文和建议，再输出交易动作。
4. 资料库提供判断材料，仓库代码和测试提供完成标准。
5. ETF 全天候 v1 的成功信号是稳定、可审计、低频、可降级，不是模型新颖。
