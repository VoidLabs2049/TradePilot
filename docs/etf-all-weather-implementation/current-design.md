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
-> 风险预算 artifact
-> 目标权重 artifact
-> 回测内核和后续评估
-> Dashboard / workflow 展示
```

`.claude/allweather/` 资料库补充的核心判断是：

```text
状态识别 -> 动态风险预算 -> 风险平价权重 -> 实盘约束
```

这条链应作为下一阶段策略计算层的设计方向，但不能覆盖当前已经完成的数据和上下文基座。

## 最新开发规划吸收

当前 ETF 全天候实现应按“后端基础框架先行，前端体验后置”的节奏推进。已新增的 ETF risk budget 页面只作为只读观察面板，用于检查 frozen artifact 是否可读、公式是否直观；target weight、baseline 和 gross backtest kernel 已完成，但稳健性评估、订单草案和模拟盘观察尚未完成。

MVP 的日常工作流是：

```text
每日数据获取
-> 更新 ETF / 指数 / 宏观与利率上下文
-> 月度调仓日生成风险预算和组合权重
-> 回测内核和评估报表验证历史表现
-> 用户根据结果进行人工交易判断
```

因此近期工程重点不是新增复杂模型，而是把后端命令行能力补齐：

- 数据获取和本地落库可重复运行。
- 风险预算、目标权重和回测可通过命令行脚本独立触发。
- 回测结果能产出净值、回撤、换手、指标和诊断报表。
- 前端页面先消费稳定 read model；不要在权重合同未稳定前提前做复杂交互。现有 risk budget 页面必须保持只读，不加入参数调整、回测触发、目标权重推导或交易动作。

模型策略上，V1 继续使用规则式状态映射、风险预算和 budgeted inverse-vol。机器学习、期货、期权、港股、美股和更完整宏观数据属于后续扩展方向，只能在当前股票/ETF 数据链路、回测纪律和风险预算估计稳定后逐步纳入。

## Frozen Artifact 主线

ETF 全天候后续策略层必须采用 frozen artifact 流程：

```text
strategy_context
-> derived.etf_aw_risk_budget
-> health check
-> derived.etf_aw_target_weight
-> health check
-> backtest kernel
-> robustness evaluation report
-> paper rebalance plan
-> simulated fill / forward observation
```

回测只能消费已经写出的风险预算和目标权重 artifact。回测内核和评估层不得在运行过程中重新估计 regime、重新生成 risk budget、重新优化 target weight 或动态搜索参数。

如果需要比较不同预算规则、协方差窗口或优化器参数，必须先生成不同 `strategy_name` / `strategy_version` 的独立 artifact，再分别进入同一个回测内核。这样每条净值曲线都能追溯到固定输入，避免把研究选参隐藏在回测循环里。

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

### Stage I：风险预算

已实现 `derived.etf_aw_risk_budget` 的 V1 最小版本：

- 注册 `derived.etf_aw_risk_budget` 数据集和 `derived.etf_aw_risk_budget.build` bootstrap profile。
- 从 `derived.etf_aw_strategy_context` 与 `derived.etf_aw_regime_score` 生成每个 rebalance date、每个 sleeve role 的风险预算。
- 固定中性预算、regime delta、confidence clamp、低置信度/缺失/陈旧/不可用降级和 point-in-time 来源检查。
- 写出前运行健康检查，`FAIL` finding 会阻断写出。
- read model 暴露 `get_latest_etf_aw_risk_budget` / `list_etf_aw_risk_budgets`。
- API 暴露 `/api/workflow/etf-aw/risk-budget/latest`。

该层仍然不输出：

- `target_weight`
- `raw_target_weight`
- `constrained_target_weight`
- `trade_action`
- `order_instruction`

已新增的 `/etf-aw` 页面只读取风险预算 artifact，用于展示 base budget、delta budget、tilted budget、状态和质量诊断。它不是投资组合权重页面，也不应承担参数配置、回测运行或交易建议职责。

### Workflow / Dashboard

后端 workflow 已暴露 ETF 全天候上下文：

- `get_latest_etf_aw_context`
- `get_latest_etf_aw_regime_context`
- `get_latest_etf_aw_strategy_context`

前端 Dashboard 已展示 ETF 全天候 snapshot 表，字段包括 sleeve、代码、1M/3M/6M 收益、3M 波动、6M 回撤、状态和诊断。

当前前端主要展示 snapshot 和 risk budget 只读观察结果；strategy context 后续可作为 insight-first 面板的输入。下一步不得继续扩展前端，除非后端 target weight 和 frozen backtest 合同已经稳定。

后续前端清理方向：

- 保留 `/etf-aw` 的只读定位，但清理视觉密度，让页面更像工作台而不是调试面板。
- 将 snapshot、risk budget、未来 target weight 和 backtest report 按稳定后端合同分区展示，避免一个页面堆叠临时字段。
- 统一状态、诊断、百分比和 caveat 展示组件，减少重复表格和长 JSON tooltip。
- 删除或隐藏只服务开发排查的字段，把详细诊断下钻到 drawer 或折叠区。
- 在 target weight 和 backtest 合同稳定前，不新增参数编辑、策略切换、回测触发或交易动作入口。

### Stage H：前置回测内核

已实现 `derived.etf_aw_backtest_kernel.build`：

- 消费已经写出的月度目标权重 artifact。
- 使用 `derived.etf_aw_sleeve_daily` 的 adjustment-aware 日频收益。
- 输出 daily NAV、月度 turnover 和最小指标。
- 缺少 `derived.etf_aw_target_weight` 时明确失败，不再静默回退等权。

该层仍是开发期验收夹具，不承载完整策略评估叙事。baseline 对比、成本假设、参数扰动和 Dashboard 净值展示属于后置 evaluation 层。

### Stage I：风险预算 artifact

已实现 `derived.etf_aw_risk_budget.build` 和 read model：

- 从 `derived.etf_aw_strategy_context` 生成 sleeve 级风险预算。
- 保留 `base_risk_budget`、`tilted_risk_budget`、`risk_budget`、`confidence_score`、`budget_status` 和质量说明。
- V1 使用规则式 regime/budget mapper，不使用机器学习分类器。
- 输出 frozen artifact，供 target weight 层消费。

已定位历史区间 `unavailable` 的主因：早期月份 market regime 已可用，但 `macro_rates_context_status = unavailable`，Stage G 旧规则把整个 strategy context 硬降为 `unavailable`，risk budget 随之降级。现已把 market-only 且市场上下文完整的场景调整为 `partial / degraded_research`，risk budget 对应输出 `partial`，并用较低 confidence cap 约束主动 tilt。

本地已补回 `macro.slow_fields`、`rates.daily_rates`、`rates.lpr` 的 2025-01 到 2026-05 历史数据；`rates.gov_curve_points` 因 Tushare `yc_cb` 接口权限不足仍只能覆盖 2026-04 以后。当前 risk budget 状态为 75 行 `partial`、10 行 `complete`。人工检查记录见 `docs/etf-all-weather-implementation/risk-budget-manual-check-2026-07-06.md`。

### Stage J：目标权重 artifact

已实现 `derived.etf_aw_target_weight.build` 和 read model：

- 消费 frozen risk budget、sleeve daily 和 rebalance calendar。
- 使用 `budgeted_inverse_vol` V1 优化器生成纸面目标权重。
- 固定 63 日收益窗口、42 最小观测、vol floor、cash/non-cash cap、no-trade band 和换手估计。
- 输出 raw target weight、constrained target weight、target weight、状态、来源日期和 explainability notes。
- 已通过前置 backtest kernel 消费验证。

人工检查记录见 `docs/etf-all-weather-implementation/target-weight-manual-check-2026-07-06.md`。当前 85 行目标权重中 10 行为 `complete`，75 行为 `partial`，0 行为 `unavailable`。其中 `partial` 主要来自上游 market-only risk budget 降级。

`2025-03-20` 曾有 5 行 `unavailable`。复核后确认根因不是波动率窗口不足，也不是状态传递仍为 `unavailable`，而是 risk budget 的 5 个 `tilted_budget` 四舍五入后合计为 `1.0000010000000001`，略超 target weight 上游校验的 `1e-6` 容忍线，导致整组预算被拒绝。现已把 risk budget rounding drift 固定压到最后一个 sleeve，重建后该期 target weight 为 `partial`。

### Stage K：月度解释表

已实现 `derived.etf_aw_monthly_explainability.build`：

- 消费 frozen `derived.etf_aw_strategy_context`、`derived.etf_aw_risk_budget`、`derived.etf_aw_target_weight` 和 `derived.etf_aw_backtest_kernel`。
- 每个 `calendar_name + rebalance_date + strategy_name + strategy_version` 输出一行解释。
- `strategy_version` 采用 target weight 版本；同时显式输出 `source_context_strategy_version`、`source_risk_budget_strategy_version` 和 `source_target_weight_strategy_version`，避免隐藏不同 artifact 版本组合。
- `rebalance_date` 是业务日期和 `year_month` 分区来源。
- 输出市场状态、macro/rates 是否缺失、risk budget 原因、target weight 原因、vol floor / cap / no-trade band 触发、目标权重换手、backtest turnover 和 diagnostics。
- backtest 是强依赖：解释表只在同一 rebalance cycle 有 backtest turnover 或 diagnostic 证据时写出。
- 不输出 `trade_action`、`order_instruction`、`rebalance_plan` 或任何真实交易计划字段。

该层是进入 Dashboard / workflow insight 前的解释层，不是 baseline evaluation，也不是 rebalance plan。

### Stage L：静态 inverse-vol 回测基线

已实现 `derived.etf_aw_baseline_weight.build` 和双权重源回测：

- baseline 使用最近 63 个交易日收益估计波动率，至少要求 42 个有效观测，并沿用 `0.005` volatility floor。
- 每个 rebalance date 必须生成 5 个完整、非负且权重和为 1 的 frozen sleeve 权重；不写出 partial baseline。
- `derived.etf_aw_backtest_kernel` 可分别消费 `target_weight` 和 `baseline`，并保留 `weight_source_type` 与 `source_weight_dataset`。
- `backtest-report` 可输出两条策略的指标、换手、diagnostics 和差值。
- 旧 backtest kernel 分区缺少权重来源字段时，会按原有语义补为 `target_weight` / `derived.etf_aw_target_weight` 后再 upsert，不需要删除历史分区。

默认 lakehouse 已完成 2025-01 到 2026-05 重建：baseline weight 为 85 行，策略和 baseline kernel 各为 349 行，artifact 与 kernel 健康检查均通过。当前策略累计收益为 `21.491%`，静态 inverse-vol baseline 为 `21.434%`；策略年化波动和月均换手略高，Sharpe 略低。该结果只覆盖 17 个调仓周期且尚未计入交易成本，应作为工程基线和增量诊断，不作为长期策略优劣结论。

该阶段仍不新增 backtest read model、API 或前端多策略对比图。

### Stage M：回测稳健性评估（设计完成，待实现）

`backtest-robustness-evaluation-design.md` 已完成，其实现范围为：

- 先审计当前策略与 baseline 的真实 point-in-time 可比区间、调仓周期数、状态分布和缺失原因。
- 保持 gross backtest kernel 不变，在 evaluation/report 层增加固定成本敏感性网格。
- 明确 previous-target turnover、初始建仓成本不可观测和短样本 caveat。
- 用扩展样本和成本后结果决定是否值得设计 simplified ERC。

该阶段仍只建设后端 CLI 和 repo-visible 报告，不新增 read model、API、前端或交易动作。详细合同见 `backtest-robustness-evaluation-design.md`。

### Stage N：模拟盘订单草案（设计完成，待实现）

Stage N 消费已冻结的目标权重、当前持仓、现金、账户总资产和最新可用价格，生成 `derived.etf_aw_rebalance_plan`。V1 固定 A 股 ETF 每手 100 份和 `cash_buffer_ratio = 0.01`，只输出 `DRAFT` 计划，不连接券商 API，不自动提交真实订单。

订单计算必须满足：

- 最新目标权重包含完整 5-sleeve，且权重和偏离 `1.0` 不超过 `1e-6`。
- BUY 和 SELL 均按交易单位向下取整；SELL 不得超过 `available_quantity`。
- 买入后保留固定现金缓冲；不足一手时输出 `HOLD` 和 `below_lot_size` warning。
- symbol、价格、持仓映射、可用现金或可用持仓缺失时阻断计划。
- 同一计划日期、策略版本和来源调仓日期不得重复生成非 cancelled 计划。

该阶段不负责策略研究、回测、真实佣金估算、盘口/滑点建模或自动成交。

### Stage O：模拟盘观察（设计完成，待实现）

Stage O 记录模拟成交状态、成交价和数量、持仓、现金、组合净值、目标与实际权重偏离、当日/累计收益、相对 baseline 收益及人工备注。先使用 repo-visible 记录完成可追溯闭环，竞品式绩效与归因界面后置。

详细合同见 `shadow-run-design.md`。V1 保持 Stage N plan 不可变，通过 append-only decision、paper fill 和 daily observation 派生状态，并生成只读 HTML / JSON performance report；不支持真实下单、自动成交、外部现金流或公司行动。

## 后续总流程

以下 Stage M-O 是近期交付标签，不代表已经实现。每一阶段必须满足退出条件后才能进入下一阶段；如果回测结论不足，应明确保留 research-only caveat，不能在评估循环中临时调参。

### 研发与交付主线

```text
Stage L 已完成：baseline artifact + 双 kernel + comparison report
  -> Stage M1：point-in-time 历史覆盖审计
  -> Stage M2：固定成本敏感性评估
  -> Stage N：paper rebalance plan + DRAFT order artifact
  -> 人工确认 / simulated fill
  -> Stage O：daily forward observation / post-mortem
  -> 远期：小资金 live pilot（单独设计、单独授权）
```

Stage M 后可并行形成研究决策：数据覆盖不足时补 point-in-time 数据；成本后优势不稳定时保留 V1 的 research-only 定位并复核上游规则；只有多区间、多成本结果一致时，才设计 simplified ERC candidate。candidate 必须使用独立 `strategy_version` 和同一 kernel / evaluation 合同，不占用 Stage N/O 的模拟盘交付编号。

### 阶段输入、输出和退出条件

| 阶段 | 核心输入 | repo-visible 输出 | 退出条件 |
| --- | --- | --- | --- |
| Stage M | frozen strategy/baseline kernel、turnover、上游状态 | coverage + cost robustness Markdown/JSON report | gross 可复现、共同日期完整、成本场景可比、caveat 完整 |
| Stage N | frozen target weight、当前持仓、现金、价格和交易单位 | `derived.etf_aw_rebalance_plan`、JSON/Markdown order draft | 权重、现金、持仓、价格和手数约束通过；只生成 `DRAFT` |
| Stage O | frozen plan、模拟成交记录、后续市场数据 | daily observation、forward performance、post-mortem | 不自动下单；计划、模拟成交、净值和偏差可追溯 |
| 可选研究分支 | Stage M 报告、独立 candidate artifact | 新 `strategy_version` 与同口径对比报告 | 不覆盖 V1，不隐藏参数搜索，不阻塞 Stage N/O 工程验证 |

### 决策门约束

1. Stage M 数据覆盖不足时，不能靠 inner join、未来数据回填或放宽 frozen vector 合同拉长样本。
2. 当前策略成本后优势不稳定时，Stage M 必须保留 research-only caveat；Stage N/O 仅用于验证工程闭环和积累 forward evidence，不得宣称策略已被证明有效。
3. simplified ERC candidate 必须使用独立 `strategy_version`，不得覆盖 V1 target weight，也不得阻塞近期模拟盘工程验证。
4. API 或前端不能触发 artifact 生成、参数搜索、回测运行或订单提交。
5. Stage N 的 rebalance plan 是 paper order draft，真实交易必须由用户人工判断；V1 不存在券商下单路径。
6. Stage O 只记录 simulated fill 和 forward result。任何 live pilot 都需要单独的数据、风控、执行和授权设计。

### 未来日常运行流程

Stage N/O 尚未完成前，日常流程停在只读评估和人工判断：

```text
每日数据同步
-> 数据质量与 watermark 检查
-> 更新 sleeve daily、宏观/利率数据和适用的只读上下文
-> 非调仓日：只更新上下文和只读观察结果
-> 月度调仓日：冻结 risk budget
-> risk budget health check
-> 冻结 target weight
-> target weight health check
-> 更新 baseline（研究对比用途）
-> 分别运行 strategy / baseline kernel
-> Stage M robustness report
-> 用户查看权重、历史表现、成本敏感性和 diagnostics
-> 当前阶段由用户自行进行人工交易判断
```

Stage N/O 完成后，月度链路才允许继续到：

```text
已批准且通过健康检查的 frozen target weight
-> 读取当前持仓
-> 生成 DRAFT paper rebalance plan
-> 手数、现金缓冲和可用持仓检查
-> 人工确认或拒绝模拟成交
-> 写入 simulated fill record
-> forward observation
-> post-mortem
-> 下一调仓周期
```

这两条运行链都禁止自动下单。Dashboard 的职责是展示稳定 artifact、质量状态和 caveat，不承担策略计算或执行。

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
-> derived.etf_aw_risk_budget
-> derived.etf_aw_target_weight

derived.etf_aw_sleeve_daily -> derived.etf_aw_baseline_weight

target_weight artifact ---\
                           -> derived.etf_aw_backtest_kernel -> backtest comparison report
baseline_weight artifact --/

strategy_context + risk_budget + target_weight + target kernel
-> derived.etf_aw_monthly_explainability
-> workflow context
-> Dashboard snapshot panel
```

### 后续目标层

```text
coverage / cost robustness evaluation
-> conditional strategy candidate comparison
-> paper rebalance plan
-> simulated fill / forward observation / post-mortem
-> later: evaluation read contract / API / read-only Dashboard
```

具体阶段、决策门和回退方向以“后续总流程”为准。

后续层必须保留以下硬边界：

1. `strategy_context` 是输入上下文，不应包含目标权重或交易动作。
2. `target_weight` 和 `trade_action` 必须来自后续明确命名的数据集，不能混入现有 Stage G 合同。
3. 回测需要拆成前置内核和后置评估层；前置内核只作为 `risk_budget` / `target_weight` 的开发期验收夹具，不承载完整策略评估叙事。

## V1 策略计算设计

### 输入

第一版策略计算只依赖已经落地或明确可落地的数据：

- `derived.etf_aw_rebalance_snapshot`
- `derived.etf_aw_regime_score`
- `derived.etf_aw_strategy_context`
- `derived.etf_aw_sleeve_daily`

宏观/利率字段可用于上下文和置信度校正，但在未完成充分验证前，不应单独触发大幅仓位切换。

### 回测内核验收夹具

已冻结并实现一个小型回测内核设计。

该内核的输入只包括：

- 给定的月度权重序列。
- `derived.etf_aw_sleeve_daily` 的 adjustment-aware 日频收益。
- `reference.rebalance_calendar.monthly_post_20` 的调仓日历。

该内核输出：

- 净值曲线。
- 年化收益、年化波动、Sharpe、最大回撤。
- 月度换手。
- 可复现实验诊断。

该内核必须是纯函数验收夹具，不包含：

- regime 评分。
- risk budget 生成。
- target weight 生成。
- 参数搜索。
- Dashboard 展示。
- 自动交易建议。

完整的 baseline 对标、成本假设、参数扰动和前端净值展示仍留在后置回测评估层。前置内核只解决一个问题：让后续预算映射、权重稳定性和换手行为有客观、可复现的开发期判据。

### 状态到预算

V1 已使用规则映射，不使用机器学习分类器。

建议输出：

- `budget_status`
- `budget_basis`
- `base_risk_budget`
- `tilted_risk_budget`
- `confidence_score`
- `budget_notes`

市场状态初始映射应克制：

| Market regime | Base budget | Tilt direction |
| --- | --- | --- |
| `risk_on` | 中性预算 | 适度提高权益风险预算，降低现金/防御预算 |
| `hedge_bid` | 中性预算 | 提高黄金和现金/防御预算，压低权益预算 |
| `defensive` | 中性预算 | 提高债券和现金预算，压低权益预算 |
| `mixed` | 中性预算 | 接近中性预算 |
| `insufficient_data` | 保守中性预算 | 不做主动 tilt |

`risk-budget-design.md` 必须把定性方向落成数值向量。初始规则固定为：

```text
tilted_budget = normalize(base_budget + confidence_score * delta_budget)
```

其中：

- `base_budget` 是每个 sleeve 的中性风险预算，合计为 1。
- `delta_budget` 是按 market regime 固定的预算偏移向量，合计为 0。
- `confidence_score` 只控制偏移幅度，不直接决定极端仓位。
- 降级状态必须回落到中性或保守中性预算，不能用缺失宏观字段放大偏移。

### 协方差估计

V1 使用 `derived.etf_aw_sleeve_daily` 的 adjusted return：

- 默认窗口、最小观测数和缺失比例阈值必须在 `target-weight-design.md` 里固定。
- 样本不足、协方差奇异、现金 sleeve 零波动、单 sleeve 数据缺失需要独立降级用例。
- V1 必须定义 vol floor 和协方差收缩规则，避免低波动 sleeve 权重发散。
- 降级需区分整体降级和单 sleeve 降级，并写入 explainability 字段。

资料库里的 CNN vol/corr/tail 只能作为未来增强，不进入 V1。

### 权重引擎

优先级：

1. budgeted inverse-vol approximation。已实现为 V1。
2. simplified ERC / risk parity。待 V1 数据质量稳定后再评估。
3. later: learnable ERC。延后。

V1 输出必须包含 explainability：

- 输入预算。
- 输入波动率/协方差摘要。
- 原始目标权重。
- 约束后目标权重。
- 降级原因。

权重写出前需要固定数值精度，建议保留 6 位小数。no-trade band 阈值必须明显大于浮点容差，避免上月与本月尾差触发伪调仓。

### 实盘约束

权重稳定前只做纸面约束设计，不生成真实订单。

后续执行约束包括：

- 单 sleeve 上限。
- no-trade band。
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

已完成 V1 最小实现。后续只允许补齐健康检查和 CLI 入口，不应继续扩大其职责。

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

1. `etf-aw-cli-design.md`

   范围：

   - ETF 全天候后端命令行主线。
   - `sync-data`、`build-risk-budget`、`health-check`、`build-target-weight`、`backtest-kernel`、`backtest-report` 命令边界。
   - 每个命令的输入、输出、失败条件和 frozen artifact 规则。
   - CLI 层最小测试要求。

   非范围：

   - 前端页面。
   - 自动交易。
   - 新策略参数搜索。

2. `backtest-kernel-design.md`

   范围：

   - 给定权重序列到净值曲线的纯函数接口。
   - 日频收益、月度调仓日历和权重生效日规则。
   - Sharpe、最大回撤、换手等最小指标。
   - 等权权重 fixture 先跑通。
   - 单元测试和 deterministic fixture 边界。

   非范围：

   - 策略权重生成。
   - baseline comparison pack。
   - 成本模型。
   - 参数扰动。
   - Dashboard 展示。

3. `risk-budget-design.md`

   范围：

   - `derived.etf_aw_risk_budget` schema。
   - read model contract。
   - `strategy_context -> sleeve risk budget` 映射规则。
   - base budget 与 tilted budget。
   - regime `delta_budget` 数值向量和 normalize 规则。
   - confidence 只控制偏移幅度的规则。
   - 样本不足、宏观字段缺失、置信度不足时的保守回落规则。
   - `complete`、`partial`、`stale`、`missing`、`unavailable` 降级行为。
   - 单元测试和 fixture 边界。

   非范围：

   - 目标权重。
   - 交易建议。
   - 订单或执行约束。

4. `target-weight-design.md`

   范围：

   - `derived.etf_aw_target_weight` schema。
   - budgeted inverse-vol MVP。
   - simplified ERC 是否值得引入的判断标准。
   - 协方差窗口、最小样本数、缺失数据处理。
   - vol floor、协方差收缩、奇异矩阵和 cash sleeve 低波动处理。
   - 权重数值精度、no-trade band 和阈值大于浮点容差的规则。
   - raw target weight、constrained target weight、降级原因和 explainability 字段。
   - target weight 健康检查清单，至少覆盖权重合计、非负权重、单 sleeve 上限、缺失 sleeve、重复 business key、异常换手、低波动 sleeve 权重发散、no-trade band 尾差、来源 risk budget 未通过健康检查等场景。
   - 使用前置回测内核检查权重稳定性、换手和基础指标。

   非范围：

   - 自动下单。
   - 当前持仓驱动的交易计划。
   - 实盘 broker / QMT / XtQuant 接口。

   初始健康检查要求：

   - FAIL：每个 rebalance date 不是 5 个 sleeve。
   - FAIL：`raw_target_weight` 或 `constrained_target_weight` 合计不等于 `1`，容忍浮点误差不超过 `1e-6`。
   - FAIL：任一权重为负数、非数值或超过 V1 单 sleeve 上限。
   - FAIL：来源 `derived.etf_aw_risk_budget` 缺失、未通过健康检查或 business key 不唯一。
   - FAIL：协方差样本不足、奇异矩阵、cash sleeve 低波动未触发降级却输出主动权重。
   - WARN：月度换手超过阈值，但仍在 V1 允许范围内。
   - WARN：no-trade band 内的尾差触发了伪调仓。
   - WARN：单 sleeve 因 vol floor 或缺失数据被降级。
   - WARN：连续多个 rebalance date 的 target weight 完全不变，需要人工确认是中性回落还是计算未更新。

### 近期交付文档

5. `rebalance-plan-design.md`

   target weight 已稳定，按 Stage N 最小订单草案边界补写。

   范围：

   - 当前持仓输入。
   - 目标权重到 paper rebalance plan。
   - A 股 ETF 100 份交易单位和固定 1% 现金缓冲。
   - BUY / SELL 向下取整、可用持仓与现金阻断。
   - `DRAFT` artifact、幂等键和人工确认边界。
   - 不接券商 API，不自动提交真实订单。

6. `backtest-robustness-evaluation-design.md`

   设计已完成，直接在现有双策略 kernel 之上实现 Stage M，不先建设 read endpoint 或前端。

   范围：

   - strategy 与静态 inverse-vol baseline 的共同覆盖审计。
   - 0 / 5 / 10 / 20 bps 固定成本敏感性。
   - gross / net 指标、换手和差值。
   - sleeve 日期缺口、短样本、初始建仓成本和成交时点 caveat。
   - Markdown / JSON repo-visible 报告。

7. `shadow-run-design.md`

   Stage N 最小计划稳定后补写 Stage O 观察合同。

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
- 前置回测内核能用等权 fixture 跑出确定性净值、指标和换手。
- 输出可解释表能逐月说明状态、预算、权重和降级原因。
- `target_weight` 至少通过前置回测内核检查换手没有异常爆发。
- 后置评估层再与等权、静态 inverse-vol、静态风险平价做 baseline 对比，并纳入成本估算。

## 近期执行顺序

1. 在新分支 `feat/etf-aw-artifact-health-evaluation` 上推进 artifact health 和 evaluation 阶段。
2. 更新本文档为当前事实入口，明确 Stage H/I/J 已落地，下一步从 artifact health check 开始。
3. 已定位并修复 `derived.etf_aw_risk_budget` 多数月份为 `unavailable` 的主因：market-only 场景不再硬阻断，改为 `partial / degraded_research`。
4. 已补充 2025-01 到 2026-05 的 PMI、SHIBOR、LPR 历史数据；国债曲线历史补数受 Tushare `yc_cb` 权限限制，仍需后续处理。
5. 已重跑 `derived.etf_aw_strategy_context`、`derived.etf_aw_risk_budget`、`derived.etf_aw_target_weight` 和 `derived.etf_aw_backtest_kernel`。当前 risk budget 为 75 行 `partial`、10 行 `complete`。
6. 补充 risk budget 人工检查记录，覆盖状态分布、预算合计、tilt 方向、confidence 生效方式和降级原因。已完成。
7. 已复核并修复 `2025-03-20` target weight 为 `unavailable` 的原因：risk budget rounding drift 略超校验阈值，修复后为 `partial`。
8. 已明确 rebalance timing：V1 权重用于调仓日收盘后或次日人工执行，保留 `trade_date <= rebalance_date`；如果未来改为盘前执行，再切换为 `trade_date < rebalance_date` 并重建 artifact。
9. 已按 `etf-aw-cli-design.md` 补齐 `build-risk-budget`、`health-check risk-budget`、`build-target-weight`、`health-check target-weight`、`backtest-kernel`、`backtest-report` 的命令边界。
10. 已增加后端命令行回测报表 Phase 0，覆盖净值、回撤、指标、换手和 diagnostics 摘要。
11. 已增加 monthly explainability table，消费 frozen context / budget / weight / backtest artifact，并补齐 source version 追溯。
12. 已完成 backtest evaluation baseline comparison：baseline 先生成独立 frozen weight artifact，再由同一个 kernel 分别运行策略和 baseline；旧 kernel 分区可原地升级来源字段。
13. `backtest-robustness-evaluation-design.md` 已完成，下一步按其合同实现历史覆盖审计和固定成本敏感性评估。
14. 实现 Stage N 纯函数订单计划，覆盖买入、卖出、不足一手、现金不足、持仓不足和重复计划。
15. 增加 Stage N CLI，输出 `DRAFT` JSON/Markdown 或 Parquet artifact，不接 API、前端或券商。
16. 实现 Stage M coverage + cost robustness report，gross 场景必须严格复现 kernel。
17. 跑通 target weight -> robustness report -> rebalance plan -> paper order draft 的端到端流程。
18. 周一盘前执行 Go / No-Go：权重、价格、持仓、现金、手数、caveat 和复现信息全部通过后，才记录 simulated fill。
19. Stage O 记录持仓、现金、净值、权重偏离、相对 baseline 收益和异常备注；短样本结论继续标记 research-only。
20. simplified ERC、read API、前端绩效归因和小资金 live pilot 均后置，不阻塞 Stage M/N/O 最小闭环。

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
