---
title: "ETF All-Weather Stage G Derived Boundary"
status: draft
mode: "design"
created: 2026-06-06
updated: 2026-06-06
modules: ["backend"]
---

# ETF All-Weather Stage G Derived Boundary

## 概览

Stage G 的职责是发布 ETF all-weather Stage 1 的 strategy-facing derived boundary。

总 ingestion 方案中的 Stage G 指的是：

> strategy-facing derived snapshot boundary

这个阶段不是重新抓取数据，不是 optimizer，不是交易指令，也不是回测引擎。它只把已经稳定的 reference、market、regime、macro、rates context 汇总成下游 research notebook、backtest scaffold、shadow portfolio 和 workflow 可以共同消费的派生数据 contract。

Stage G 的核心目标是让下游代码只读 canonical / derived data，尤其不能再直接调用 Tushare 或其他 source adapter。

## 当前 Repo 对齐

总架构文档中 Stage G 提到：

- `derived.etf_aw_market_features`
- `derived.etf_aw_rebalance_snapshot`

但当前 repo 中，`derived.etf_aw_rebalance_snapshot` 已经在 Stage D 落地，并且 Stage E 已经在它之上落地了 `derived.etf_aw_regime_score`。

因此 Stage G 不应再创建第二个同名 rebalance snapshot。当前分支的 Stage G 命名应调整为：

- 新增 `derived.etf_aw_market_features`
- 新增 `derived.etf_aw_strategy_context`
- 继续把已有 `derived.etf_aw_rebalance_snapshot` 作为输入
- 继续把已有 `derived.etf_aw_regime_score` 作为 market-only regime 输入

这个命名能避免覆盖 Stage D 语义，也能让最终 strategy-facing artifact 的职责更清晰。最终产物使用 `strategy_context` 而不是 `strategy_snapshot`，是为了强调它只提供决策上下文，不提供组合权重或交易建议。

## Stage F 完整性门槛

Stage G 原始前提是 reference、market、macro、rates 各层都稳定。

当前 repo-visible Stage F 状态需要先被显式审计：

- 已注册并测试：`rates.daily_rates`
- 已注册并测试：`rates.lpr`
- 未在当前 registry 中看到：`macro.slow_fields`
- 未在当前 registry 中看到：`rates.gov_curve_points`
- 未在当前 read model 中看到：`get_latest_etf_aw_macro_rates_context(...)`

Stage G 设计必须处理这个事实，不能假装缺失的 macro / curve context 已经存在。

推荐规则：

- Stage G v0 可以在缺失 macro / curve context 时生成 `partial` 或 `unavailable` strategy context。
- Stage G v0 不能把缺失 macro / curve context 的月份标记为 `complete`。
- Stage G full completion 需要 Stage F read service 可用，并且 required primary macro / rates / curve fields 能按 as-of rule 查询。
- 任何进入 Stage G 的 macro / rates observation 必须已经满足 `effective_date <= rebalance_date`。Stage G 不重新推断 release date 或 effective date。

## Stage G v0 与 full completion

Stage G 分成两个完成层级，避免把当前 Stage F 缺口误判为 Stage G 被阻塞。

### Stage G v0 completion

Stage G v0 可以完成以下事情：

- 构建 `derived.etf_aw_market_features`
- 构建 `derived.etf_aw_strategy_context`
- 明确输出 `macro_rates_context_status = "deferred"` 或 `unavailable`
- 将 `context_status` 限制为 `partial` / `stale` / `unavailable`
- 将 `readiness_level` 限制为 `degraded_research` / `not_ready`
- 在 `point_in_time_notes_json` 中记录 deferred 的 macro / curve field families

Stage G v0 不能输出：

- `context_status = "complete"`
- `readiness_level = "research_ready"`
- `context_basis = "market_plus_macro_rates"`

### Stage G full completion

Stage G full completion 需要额外满足：

- Stage F macro / rates read service 已落地
- required primary macro / rates / curve fields 都可以按 `effective_date <= rebalance_date` 查询
- latest-history-only、source caveat 和 revision caveat 都能进入 Stage G context
- 缺失 primary field 时 validation 会阻止 `complete`

只有 full completion 才允许输出 `context_status = "complete"` 和 `readiness_level = "research_ready"`。

## 与前序阶段的关系

### 来自 Stage C

Stage G 继承完整 SH/SZ trading calendar 和 monthly post-20 rebalance calendar。

Stage G 不重新生成交易日历，不改变调仓日规则。

### 来自 Stage D

Stage G 使用已有月度 sleeve-level snapshot：

- `derived.etf_aw_rebalance_snapshot`
- `get_latest_etf_aw_snapshot(as_of_date)`
- `list_etf_aw_snapshots(start, end)`

Stage G 不重新计算复权收益、波动率或回撤。

### 来自 Stage E

Stage G 使用已有 market-only regime context：

- `derived.etf_aw_regime_score`
- `get_latest_etf_aw_regime_context(as_of_date)`
- `list_etf_aw_regime_contexts(start, end)`

Stage G 不静默改变 Stage E 的 `market_only` 语义。即使后续引入 macro-aware label，也必须作为新字段或新 scorer 显式并存。

### 来自 Stage F

Stage G 应消费 Stage F 提供的 timing-safe macro / rates read model。

推荐输入函数：

- `get_latest_etf_aw_macro_rates_context(as_of_date)`
- `list_etf_aw_macro_rates_contexts(start, end)`

如果这些函数尚未落地，Stage G 不应直接在 notebook、workflow 或 frontend 中读取 normalized rates parquet 去临时拼 context。应先增加一个窄 read service，或在 Stage G context 中明确输出 `macro_rates_context_status = "unavailable"`。

## Stage G 目标

- [ ] 注册 `derived.etf_aw_market_features`
- [ ] 注册 `derived.etf_aw_strategy_context`
- [ ] 提供 `derived.etf_aw_market_features.build` bootstrap profile
- [ ] 提供 `derived.etf_aw_strategy_context.build` bootstrap profile
- [ ] 让 Stage G builder 只读取 canonical / derived data
- [ ] 将 market-only regime、rates context、macro context 的可用性合并成一个 strategy-facing contract
- [ ] 对缺失 macro / rates / curve primary fields 做显式降级
- [ ] 保留 source caveat、revision caveat 和 point-in-time caveat
- [ ] 提供 read service 给 workflow、notebook、backtest scaffold 和 shadow portfolio 复用
- [ ] 输出 contract 不包含 target weights、trade action 或 order instruction

## Stage G 非目标

本阶段明确不做：

- source fetch
- Tushare / AKShare direct calls
- macro slow fields normalization
- government curve extraction
- release date 或 effective date 推断
- target weights
- risk budget
- inverse-vol / ERC / optimizer
- buy / sell recommendation
- order generation
- full monthly backtest engine
- parameter search
- dashboard shadow-run UI
- live trading workflow
- generic multi-strategy engine

Stage G 的完成判断以 TradePilot repo 内设计、实现、测试和 contract 可消费性为准，不依赖其他项目的签收或验收流程。

## 设计假设

1. Frozen v1 sleeve universe 不变。
2. Monthly rebalance clock 不变：每月 20 日及以后，SH/SZ 共同开市的第一个交易日。
3. Stage G builder 只能读取已有 canonical / derived datasets。
4. Stage G 只做 strategy-facing context assembly，不做 allocation decision。
5. Missing input 必须体现在 status 和 quality notes 中，不能通过少行、空 JSON 或高 confidence 掩盖。
6. 如果 Stage F macro / curve 仍未落地，Stage G 只能发布 degraded contract。
7. Downstream notebook / backtest 可以基于 Stage G artifact 运行，但 notebook / backtest 本身不属于 Stage G。

## 数据集 1：`derived.etf_aw_market_features`

### 用途

`derived.etf_aw_market_features` 是 Stage D snapshot 和 Stage E market-only regime score 之间的 strategy-facing feature normalization layer。

它的作用是把 sleeve-level market evidence 转换成稳定、可查询、可扩展的 feature rows，避免 notebook 或 backtest 直接解析 Stage E 的 `signals_json`。

保留这个 dataset 的原因：

- notebook / backtest 通常需要 long-form feature table，而不是只消费单条 context JSON。
- Stage E 的 `signals_json` 是解释字段，不应成为 research pipeline 的主要读取入口。
- Stage G strategy context 可以只内嵌精选 features，而完整 feature 历史由 `derived.etf_aw_market_features` 提供。
- 这层只做 market feature normalization，不引入 macro / rates 字段，因此不会扩大 point-in-time 风险面。

### 粒度

每行代表一个 rebalance date 上的一个 market feature：

- `(calendar_name, rebalance_date, feature_name, feature_scope, feature_subject)`

推荐初版：

- `feature_scope = "sleeve"` 用于单个 sleeve role
- `feature_scope = "group"` 用于 equity / bond / gold / cash group
- `feature_scope = "regime"` 用于 market score 和 confidence

### 业务键

```text
(calendar_name, rebalance_date, feature_name, feature_scope, feature_subject)
```

### 初始特征集

从 Stage D / E 派生的初版 features：

| 特征 | 范围 | 主体 | 来源 |
| --- | --- | --- | --- |
| `direction_score` | `sleeve` | sleeve role | Stage E `signals_json` |
| `return_1m` | `sleeve` | sleeve role | Stage D snapshot |
| `return_3m` | `sleeve` | sleeve role | Stage D snapshot |
| `return_6m` | `sleeve` | sleeve role | Stage D snapshot |
| `volatility_3m` | `sleeve` | sleeve role | Stage D snapshot |
| `max_drawdown_6m` | `sleeve` | sleeve role | Stage D snapshot |
| `equity_score` | `group` | `equity` | equity sleeve direction score 平均值 |
| `bond_score` | `group` | `bond` | bond direction score |
| `gold_score` | `group` | `gold` | gold direction score |
| `cash_score` | `group` | `cash` | cash direction score |
| `market_score` | `regime` | `market_only` | Stage E row |
| `market_confidence_score` | `regime` | `market_only` | Stage E row |
| `market_confidence_cap` | `regime` | `market_only` | Stage E row |

不要把 macro 或 rates 字段加入这个 dataset。它们属于 macro / rates context 和最终 strategy context。

### Stage E `signals_json` contract

Stage G 不能临时解析未版本化的 `signals_json`。在实现 `direction_score` 前，必须满足以下任一条件：

1. Stage E `signals_json` 明确声明并测试 schema，例如 `etf_aw_regime_signals_v1`。
2. Stage G 复用 Stage E scorer helper 重新生成 direction score，而不是解析展示用 JSON。

如果采用 `signals_json` contract，最低结构要求是：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定为 `etf_aw_regime_signals_v1`，可以作为外层字段或每个 signal 字段 |
| `sleeve_role` | frozen sleeve role |
| `sleeve_code` | ETF code，可为空但不能替代 role |
| `direction_score` | 数值，允许为空但不能缺字段 |
| `metrics_used` | 参与 score 的 metrics list |
| `missing_metrics` | 缺失 metrics list |
| `quality_notes` | JSON object |

缺少 schema version、缺少 `direction_score` 字段、或 role 不在 frozen universe 中时，Stage G 不能把该 signal 当成 0 分处理，只能降级对应 feature。

### 必需字段

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定为 `etf_aw_market_features_v1` |
| `calendar_name` | 调仓日历名称 |
| `calendar_month` | `YYYY-MM` |
| `rebalance_date` | 调仓日 |
| `feature_name` | 稳定特征 key |
| `feature_scope` | `sleeve` / `group` / `regime` |
| `feature_subject` | sleeve role、group name 或 `market_only` |
| `feature_value` | 数值；只有 source feature 缺失时才允许为空 |
| `unit` | `decimal_return`、`score`、`ratio` 或 `none` |
| `source_dataset` | 来源 derived dataset 名称 |
| `source_status` | 来源行状态 |
| `feature_status` | `complete` / `partial` / `missing` / `stale` |
| `quality_notes` | JSON 文本 |
| `source_rebalance_date` | 来源调仓日 |
| `ingested_at` | 构建时间戳 |

### 状态规则

- Stage D sleeve row 为 `missing` 时，对应 feature row 必须输出 `feature_status = "missing"`。
- Stage D sleeve row 为 `partial` 且 source metrics 不完整时，对应 feature row 必须输出 `feature_status = "partial"`。
- Stage D 或 Stage E 出现 stale 状态时，对应 feature row 必须输出 `feature_status = "stale"`。
- Stage E regime score 为 `unavailable` 时，`market_*` features 应按其 quality notes 输出 `missing` 或 `partial`。
- 缺失 `signals_json` 时，不能把 direction score 当成 0。

## 数据集 2：`derived.etf_aw_strategy_context`

### 用途

`derived.etf_aw_strategy_context` 是 Stage G 的主交付物。

它把 market features、market-only regime context、macro / rates context availability 和 caveats 合并成一个下游可消费的 monthly strategy context。

它回答的问题是：

- 截至某个 rebalance date，下游可以安全使用哪些 strategy context？
- Market、macro、rates 哪些部分完整、缺失、stale 或带 caveat？
- 这个月份是否适合进入 research / backtest / shadow portfolio 消费？

它不回答的问题是：

- 买什么
- 卖什么
- 配多少
- 是否下单

### 粒度

每行代表一个 rebalance date 上的一个 strategy context：

```text
(calendar_name, rebalance_date, strategy_name, strategy_version)
```

推荐固定：

- `strategy_name = "etf_aw_v1"`
- `strategy_version = "stage_g_v1"`

### 必需字段

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定为 `etf_aw_strategy_context_v1` |
| `contract_version` | 固定为 `etf_aw_strategy_context_contract_v1` |
| `calendar_name` | 调仓日历名称 |
| `calendar_month` | `YYYY-MM` |
| `rebalance_date` | 调仓日 |
| `effective_date` | strategy context 生效日，初版等于 `rebalance_date` |
| `strategy_name` | 固定为 `etf_aw_v1` |
| `strategy_version` | 固定为 `stage_g_v1` |
| `context_status` | `complete` / `partial` / `stale` / `unavailable` |
| `readiness_level` | `research_ready` / `degraded_research` / `not_ready` |
| `context_basis` | `market_only` / `market_plus_rates` / `market_plus_macro_rates` |
| `market_context_status` | Stage G market feature 聚合状态 |
| `market_regime_label` | Stage E label；除非显式升级，否则仍是 market-only |
| `market_score` | Stage E market score |
| `market_confidence_score` | Stage E confidence |
| `market_confidence_cap` | Stage E confidence cap |
| `macro_rates_context_status` | `complete` / `partial` / `stale` / `unavailable` / `deferred` |
| `missing_primary_fields_json` | JSON list |
| `missing_confirmatory_fields_json` | JSON list |
| `available_fields_json` | eligible macro / rates fields 的 JSON list |
| `source_caveats_json` | JSON list 或 object |
| `revision_caveats_json` | JSON list 或 object |
| `point_in_time_notes_json` | JSON object |
| `market_features_json` | 面向 API / workflow 展示的精选 market features |
| `source_snapshot_rebalance_date` | Stage D snapshot 日期 |
| `source_regime_rebalance_date` | Stage E score 日期 |
| `source_macro_rates_rebalance_date` | Stage F context 日期，可为空 |
| `ingested_at` | 构建时间戳 |

明确禁止出现的字段：

- `target_weight`
- `target_weights`
- `risk_budget`
- `trade_action`
- `order_instruction`
- `buy_list`
- `sell_list`

### 状态优先级矩阵

Stage G context status 按下面的优先级聚合。越靠前优先级越高。

| 条件 | `context_status` | `readiness_level` | `context_basis` |
| --- | --- | --- | --- |
| Stage D snapshot 缺失 | `unavailable` | `not_ready` | `market_only` |
| Stage E regime score 缺失 | `unavailable` | `not_ready` | `market_only` |
| Required market features 无法构建 | `unavailable` | `not_ready` | `market_only` |
| 任一 required input context 为 stale | `stale` | `not_ready` | 按可用字段决定 |
| Required primary macro / rates fields 缺失且未 deferred | `unavailable` | `not_ready` | 按可用字段决定 |
| Macro / curve fields 被显式 deferred，market context 可用 | `partial` | `degraded_research` | `market_only` 或 `market_plus_rates` |
| 仅缺失 confirmatory macro / rates fields | `partial` | `degraded_research` | 按可用字段决定 |
| 所有 required primary 和 confirmatory inputs 可用 | `complete` | `research_ready` | `market_plus_macro_rates` |

实现时应先判断 `unavailable`，再判断 `stale`，再判断 `partial`，最后才允许 `complete`。

### Context 状态规则

只有满足以下全部条件时，`context_status` 才能为 `complete`：

- Stage D rebalance snapshot 为 complete。
- Stage E regime score 为 complete。
- Stage G market features 为 complete。
- Stage F macro / rates context 为 complete。
- 没有缺失 required primary field。
- 没有未解决的 point-in-time caveat。

以下情况使用 `context_status = "partial"`：

- Market context 可用，但至少一个 confirmatory macro / rates field 缺失。
- Stage F context 存在，但带有非阻塞 caveat。
- Stage F context 只缺失 confirmatory fields。

以下情况使用 `context_status = "stale"`：

- 任一必需输入 context 为 stale。
- Stage F context 相对自身 freshness rule 使用了过期 observation。

以下情况使用 `context_status = "unavailable"`：

- Stage D snapshot 缺失。
- Stage E regime score 缺失。
- Required market features 无法构建。
- Required primary macro / rates fields 缺失，且 implementation 没有显式将其 deferred。

如果当前分支刻意暂缓 macro / curve fields，使用以下表达：

- `macro_rates_context_status = "deferred"`
- 只有在 market 和 rates context 其余部分可用时，`context_status` 才能为 `partial`
- `readiness_level = "degraded_research"`
- `point_in_time_notes_json` 必须包含 deferred field families

### Readiness 规则

`readiness_level = "research_ready"` 要求 `context_status = "complete"`。

`readiness_level = "degraded_research"` 可用于 market-only 或 market-plus-rates research，但下游代码必须能看到 degraded basis。

当 market context 缺失，或无法建立 point-in-time safety 时，必须使用 `readiness_level = "not_ready"`。

### `context_basis` 判定规则

`context_basis` 描述当前 context 实际包含的证据范围，不能只按目标设计填写。

| 条件 | `context_basis` |
| --- | --- |
| 只有 Stage D/E market context 可用 | `market_only` |
| Market context 可用，且 `rates.daily_rates` / `rates.lpr` 至少有 required primary rates field eligible | `market_plus_rates` |
| Market、macro slow fields、daily rates、LPR 和 required curve primary fields 都 eligible | `market_plus_macro_rates` |

如果 macro 或 curve fields 被 deferred，不能使用 `market_plus_macro_rates`。如果 rates 只有 confirmatory field 可用，也不能升级到 `market_plus_rates`。

## Point-In-Time 规则

Stage G 必须保持以下不变量：

1. Stage G 绝不调用 source adapter。
2. Stage G 绝不为 source-backed macro / rates facts 计算 release date 或 effective date。
3. 任何进入 `available_fields_json` 的 macro / rates row 都必须满足 `effective_date <= rebalance_date`。
4. 任何 `effective_date > rebalance_date` 的 row 都必须被排除，并记录到 `point_in_time_notes_json`。
5. Latest-history-only macro caveats 必须向下游传递，不能隐藏。
6. 如果多条 source rows 满足 as-of rule，Stage G 先按 effective date 选择最新 eligible observation，必要时再按 ingested-at 选择最新版本。

## 存储

两个 Stage G datasets 都应使用 derived lakehouse parquet。

推荐路径：

- `data/lakehouse/derived/derived.etf_aw_market_features/<year>/<month>/part-00000.parquet`
- `data/lakehouse/derived/derived.etf_aw_strategy_context/<year>/<month>/part-00000.parquet`

分区日期字段：

- `rebalance_date`

Upsert keys：

- market features: `(calendar_name, rebalance_date, feature_name, feature_scope, feature_subject)`
- strategy context: `(calendar_name, rebalance_date, strategy_name, strategy_version)`

除非 API / 查询性能确实需要，Stage G 第一批 slice 不新增 DuckDB canonical tables。当前 Stage D/E 模式是通过 `tradepilot/etl/read_models.py` 读取 derived parquet，Stage G 应先沿用这个模式。

## Builder 设计

### `derived.etf_aw_market_features.build`

输入：

- `derived.etf_aw_rebalance_snapshot`
- `derived.etf_aw_regime_score`

行为：

- 读取指定窗口内的 Stage D/E derived partitions。
- 每个 feature 生成一条 long-form market feature row。
- 保留 missing / partial / stale status。
- 验证 business keys 和 allowed feature names。
- Upsert 到 derived parquet。

这个 builder 不能读取 raw 或 normalized source datasets。

### `derived.etf_aw_strategy_context.build`

输入：

- `derived.etf_aw_market_features`
- `derived.etf_aw_regime_score`
- Stage F macro / rates read service（可用时）

行为：

- 按 `(calendar_name, rebalance_date)` join。
- 每个 rebalance date 组装一条 strategy context。
- 传递 missing primary / confirmatory fields。
- 传递 source caveats 和 revision caveats。
- 确定性设置 `context_status` 和 `readiness_level`。
- Upsert 到 derived parquet。

当 macro / rates read service 不可用时，这个 builder 可以返回 degraded output，但必须在字段和 validation summary 中显式表达降级原因。

## Validation 设计

为 Stage G 增加窄范围 validators 或 local builder validation helpers。

### `derived.etf_aw_market_features`

检查：

- duplicate business key 必须 fail
- required columns 必须存在
- feature scope 必须在允许集合内
- feature name 必须匹配对应 scope
- feature status 必须在允许集合内
- status 为 `complete` 时，numeric feature values 必须为有限值
- `quality_notes` 必须是合法 JSON
- 不能出现 macro / rates feature names

### `derived.etf_aw_strategy_context`

检查：

- duplicate business key 必须 fail
- status values 必须在允许集合内
- `complete` context 不能存在 missing primary fields
- `complete` context 的 `macro_rates_context_status` 不能是 `partial`、`unavailable` 或 `deferred`
- `readiness_level = "research_ready"` 要求 `context_status = "complete"`
- forbidden allocation 或 trade fields 必须不存在
- `available_fields_json`、caveat fields 和 notes fields 必须是合法 JSON
- macro / rates context 降级或 deferred 时，必须存在 point-in-time notes
- validation 或测试必须显式断言 forbidden fields absent，不能只依赖设计说明

## Read Model

在 `tradepilot/etl/read_models.py` 中增加 Stage G read services。

推荐函数：

- `get_latest_etf_aw_strategy_context(as_of_date)`
- `list_etf_aw_strategy_contexts(start, end)`
- 可选：`get_latest_etf_aw_market_features(as_of_date)`

推荐 API / workflow 集成：

- 只有在 read service contract 稳定后，才把 `etf_aw_strategy_context` 加入 workflow context。
- 保留现有 `etf_aw_context` 和 `etf_aw_regime_context`，确保 backward compatibility。
- 不删除、不重命名 Stage D/E context fields。

推荐返回 JSON：

```json
{
  "schema_version": "etf_aw_strategy_context_v1",
  "contract_version": "etf_aw_strategy_context_contract_v1",
  "calendar_name": "etf_aw_v1_monthly_post_20",
  "calendar_month": "2026-04",
  "rebalance_date": "2026-04-20",
  "strategy_name": "etf_aw_v1",
  "strategy_version": "stage_g_v1",
  "context_status": "partial",
  "readiness_level": "degraded_research",
  "context_basis": "market_plus_rates",
  "market": {
    "label": "risk_on",
    "score": 45.0,
    "confidence_score": 0.62,
    "confidence_cap": 0.70
  },
  "macro_rates": {
    "status": "deferred",
    "missing_primary_fields": ["official_pmi", "cn_gov_10y_yield"],
    "missing_confirmatory_fields": ["cn_yield_curve_slope_10y_1y"]
  },
  "quality_notes": {
    "macro_fields_deferred": true,
    "curve_fields_deferred": true
  }
}
```

## 实施顺序

### Slice G0：前置条件审计

目标：

- 在 implementation notes 或 test fixture comments 中增加一段 repo-visible audit，说明哪些 Stage F inputs 已可用。
- 决定当前分支是先实现 Stage F read service，还是先输出 degraded Stage G contexts。

验证：

- `python -m unittest -v tests/etl/test_stage_f_rates.py`

### Slice G1：Market Feature 数据集

目标：

- 注册 `derived.etf_aw_market_features`。
- 增加 build profile 和 writer。
- 从 Stage D/E outputs 构建 long-form market features。

验证：

- `python -m unittest -v tests/etl/test_stage_g_market_features.py`

### Slice G2：Strategy Context 数据集

目标：

- 注册 `derived.etf_aw_strategy_context`。
- 增加 build profile 和 writer。
- 当 macro / rates context 缺失时，显式输出 `partial` / `unavailable` contexts。

验证：

- `python -m unittest -v tests/etl/test_stage_g_strategy_context.py`

### Slice G3：Read Service

目标：

- 增加 Stage G read services。
- 返回 as-of date 当日或之前最新的 strategy context。
- 以 API-friendly shape 保留 JSON fields 和 caveats。

验证：

- `python -m unittest -v tests/etl/test_stage_g_read_models.py`

### Slice G4：Workflow Context 集成

目标：

- 在现有 `etf_aw_context` 和 `etf_aw_regime_context` 旁边增加 `etf_aw_strategy_context`。
- 保持 backward compatibility。

验证：

- 如已有相关 workflow tests，运行对应测试。
- `python -c "from tradepilot.main import app; print('OK')"`

## 验收标准

### Stage G v0 验收

1. `derived.etf_aw_market_features` 可以针对指定窗口构建。
2. `derived.etf_aw_strategy_context` 可以针对指定窗口构建。
3. 两个 datasets 都能幂等 upsert，且不产生 duplicate business keys。
4. Stage G output 可以通过 `tradepilot/etl/read_models.py` 读取。
5. Stage G builder 不调用 source adapters。
6. Missing macro / rates / curve fields 在 status 和 notes 中可见。
7. Stage F macro / curve 尚未完整时，不输出 `complete` strategy context。
8. `context_basis` 不会在 macro / curve deferred 时错误升级为 `market_plus_macro_rates`。
9. `signals_json` 只有在 schema version 明确且测试覆盖后才被解析；否则必须复用 scorer helper 或降级。
10. 状态优先级矩阵至少覆盖 unavailable、stale、partial 和 deferred fixtures。
11. Output contract 不包含 target weight、trade action、order instruction 或 optimizer result。
12. 测试显式断言 forbidden fields absent。
13. Stage D/E/F narrow tests 继续通过。
14. `python -c "from tradepilot.main import app; print('OK')"` 通过。

### Stage G full completion 验收

1. Stage F macro / rates read service 可用。
2. Required primary macro / rates / curve fields 都能按 as-of rule 查询。
3. Required primary inputs 缺失时，不输出 `complete` strategy context。
4. 只有 `context_status = "complete"` 时，才输出 `readiness_level = "research_ready"`。
5. Effective date 晚于 rebalance date 的 observation 被排除，并记录在 `point_in_time_notes_json`。
6. `context_basis` 覆盖 `market_only`、`market_plus_rates` 和 `market_plus_macro_rates` 三类 fixture。

推荐窄范围回归命令：

```bash
python -m unittest -v \
  tests/etl/test_stage_d.py \
  tests/etl/test_stage_e.py \
  tests/etl/test_stage_f_rates.py \
  tests/etl/test_stage_g_market_features.py \
  tests/etl/test_stage_g_strategy_context.py \
  tests/etl/test_stage_g_read_models.py
```

如果 Stage G 改动 workflow API contract：

```bash
python -c "from tradepilot.main import app; print('OK')"
```

如果 Stage G 改动前端展示：

```bash
cd webapp
yarn build
```

## 风险

### 1. 错误完整性

风险：

- Stage G 可能在 macro / curve context 缺失时发布看起来完整的 strategy context。

缓解：

- Completion rules 必须要求 Stage F context。
- Missing primary fields 必须阻止 `complete`。
- Deferment 必须显式表达。

### 2. 隐性 Lookahead

风险：

- Stage G 可能意外纳入在 rebalance date 尚未 effective 的 macro / rates rows。

缓解：

- Stage G 消费 Stage F read model。
- 任何纳入的 macro / rates field 都必须携带 effective date，并满足 `effective_date <= rebalance_date`。
- 测试应包含 effective date 晚于 rebalance date 的 observation，并断言它被排除。

### 3. Stage E 语义漂移

风险：

- Market-only regime labels 可能被误解为完整 macro regime labels。

缓解：

- 保留 `context_basis`。
- 保持 Stage E scorer name 和 version 可见。
- 只有以新 scorer 或显式字段的形式增加 macro-aware scorer。

### 4. Strategy Boundary 膨胀

风险：

- Stage G 可能变成隐藏的 allocation engine。

缓解：

- Validators 拒绝 target weight、risk budget、trade action 和 order fields。
- Backtest 和 allocation 仍然只是下游 consumer。

## 最终判断

Stage G 应发布稳定的 strategy-facing data boundary，而不是 trading system。

Stage G 最有价值的输出，是用两个 derived datasets 保证以下事实成立：

- research / backtest / shadow code 可以消费同一个本地 contract
- 所有 market、macro、rates、revision 和 source caveats 都保持可见
- 下游 consumer 不需要直接 source fetch
- 不完整输入会被诚实降级，而不是被隐藏在漂亮的 label 后面
