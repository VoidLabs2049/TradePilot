---
title: "ETF All-Weather Stage F Macro And Rates Layer"
status: draft
mode: "design"
created: 2026-05-28
updated: 2026-06-04
modules: ["backend"]
---

# ETF All-Weather Stage F Macro And Rates Layer

## Overview

Stage F 的职责是把 ETF 全天候 v1 从 market-only context 推进到 timing-safe macro / rates context。

总 ingestion 方案中的 Stage F 指的是：

> timing-sensitive macro and rates layer

这与已有策略路径文档中的 Stage E `market-only regime scoring` 不是同一层。当前 Stage F 不生成 target weights，不生成 risk budget，也不发布最终 strategy-facing snapshot。它只负责把宏观慢变量、利率和收益率曲线数据接入到 `tradepilot/etl/` foundation 中，并在进入任何策略消费之前完成 release / effective date 纪律、validation 和 canonical storage。

## Optimization Review Against Current Project

当前 repo 已经不只是 Stage B skeleton。Stage C/D/E 的核心路径已经在代码和测试中落地：

- `ETLService.run_dataset_sync()` 已能执行 source-backed dataset 的 raw landing、normalization、validation、canonical write 和 watermark 推进。
- `ETLService.run_bootstrap()` 已包含窄范围 profile：`reference.trading_calendar.full_history`、`reference.rebalance_calendar.monthly_post_20`、`reference.etf_aw_sleeves.frozen_v1`、`derived.etf_aw_sleeve_daily.build`、`derived.etf_aw_rebalance_snapshot.build`、`derived.etf_aw_regime_score.build`。
- `tradepilot/etl/read_models.py` 已有 Stage D snapshot 和 Stage E market-only regime 的读取 contract。
- `build_stage_b_datasets()` 这个 helper 名称是历史遗留，实际已经注册 Stage B 到 Stage E 的内置 dataset。Stage F 第一批实现应复用这个注册入口，除非另起一个单独重命名 cleanup。

因此 Stage F 的优化方向不是再设计一套宏观数据平台，而是沿着现有项目的扩展点做 additive slice：

1. 给 current registry 增加 macro / rates dataset definition。
2. 给 current `TushareSourceAdapter` 和 `TushareClient` 增加对应 fetch 方法。
3. 给 current normalizer / validator 增加 Stage F dataset-specific 实现。
4. 给 current canonical writer 增加 macro / rates 分区 upsert 路由，不能让未知 dataset 落入 market daily 的默认写入分支。
5. 在 current read model 风格上新增 macro/rates context，并保持它与 Stage E `market-only` context 并列，而不是覆盖 Stage E 语义。

## Relationship With Earlier Stages

Stage F 建立在前面阶段已经完成的边界之上。

### From Stage C

Stage F 依赖完整 SH/SZ canonical trading calendar：

- 用于 slow macro fields 的 `effective_date = next open trading day on or after release_date`
- 用于 rates / curve 的可用性和 rebalance-date as-of 查询
- 用于判断 monthly feature 在调仓日是否可用

Stage F 不重新生成 trading calendar。

### From Stage D

Stage F 依赖 frozen v1 sleeves 与 monthly post-20 rebalance calendar：

- macro / rates context 的主要消费时点是 `canonical_rebalance_calendar`
- Stage F 输出必须能按 `rebalance_date` 做 point-in-time 查询

Stage F 不改变 frozen sleeve universe，也不改变调仓规则。

### From Stage E

Stage F 依赖已经存在的 minimum adjustment-aware market panel 和 market-only scoring context：

- `derived.etf_aw_sleeve_daily`
- `derived.etf_aw_rebalance_snapshot`
- `derived.etf_aw_regime_score`

Stage F 不重算 market-only score。后续 Stage G 可以把 market context 与 macro / rates context 合并成 strategy-facing snapshot。

## Stage F Goal

- [ ] 注册并实现 `macro.slow_fields`
- [ ] 注册并实现 `rates.daily_rates`
- [ ] 注册并实现 `rates.lpr`
- [ ] 注册并实现 `rates.gov_curve_points`
- [ ] 为 slow fields 明确 `period_label`、`release_date`、`effective_date`、`revision_note`
- [ ] 为 macro / rates / curve 字段明确 `field_role`，继承 `docs/etf-all-weather-data-sources/v1-canonical-field-list.md`
- [ ] 为 M1-family 字段明确 `definition_regime` 与 `regime_note`
- [ ] 为 rates / curve 明确 quote date、effective date、unit 与 extraction caveat
- [ ] 保证所有 strategy-facing 查询只能使用 `effective_date <= rebalance_date` 的观测
- [ ] 将 validation results 持久化到现有 `etl_validation_results`
- [ ] 为后续 Stage G 提供稳定 read service，而不是直接生成最终策略判断

## Stage F Non-Goals

本阶段明确不做：

- target risk budget
- target weights
- optimizer / ERC / inverse-vol allocation
- buy / sell recommendation
- monthly backtest
- strategy-facing final snapshot
- generic DAG scheduler
- multi-source automatic reconciliation framework
- vintage macro database
- official-source text parser
- dashboard trading recommendation
- C-Demo 式老师验收或外部签收流程

Stage F 可以提供 read model，但不能把 read model 命名或展示成完整策略结论。
Stage F 的完成判断以 repo 内设计、实现、测试和数据质量证据为准，不要求像 C-Demo 那样找老师做验收。

## Design Assumptions

1. 当前 repo 已经具备 Stage B/C/D/E 风格的 `ETLService.run_dataset_sync()`、narrow bootstrap profiles、raw landing、normalization、validation、canonical write、read model 和 watermark contract。
2. Stage F 第一批实现优先使用 Tushare wrapper path 扩展。AKShare / official source 暂作为后续 fallback 或 validation source，不在第一批实现中做复杂 source arbitration。
3. `docs/etf-all-weather-data-sources/minimum-official-source-verification-note.md` 已确认官方源是有效 anchor 但并非都适合低摩擦自动化。Stage F 必须把 official anchor / wrapper source / source caveat 记入 canonical row 或 read model，不能把 Tushare-only convenience 误写成官方 vintage。
4. 如果 source 不提供真实 first-release vintage，canonical row 必须带 `revision_note = latest_history_only_unless_vintage_captured` 或更具体 caveat。
5. Conservative timing 比追求最早可用日期更重要。
6. 每个 dataset 先做独立纵切，不把 macro、rates、curve、derived macro-aware scorer 放进同一个 implementation batch。

## 近期范围

### 数据集 1：`macro.slow_fields`

粒度：

- `(field_name, period_label, ingested_at)`，用于 version-preserving canonical facts

latest-known view 的业务键：

- `(field_name, period_label)`

必需字段：

| 字段 | 含义 |
| --- | --- |
| `field_name` | Canonical field key |
| `period_label` | 月度或季度 period，例如 `2026-04` |
| `period_type` | `monthly` / `quarterly` |
| `value` | 数值型 reported value |
| `unit` | 显式 unit，例如 `%`、`100m_cny`、`index_point` |
| `field_role` | 从 v1 field list 继承的 `primary` / `confirmatory` |
| `release_date` | 保守 public release date |
| `effective_date` | release date 当日或之后的第一个 canonical open trading day |
| `revision_note` | Vintage / revision caveat |
| `definition_regime` | M1-family 必填，其他字段可为空 |
| `regime_note` | M1-family 必填，其他字段可为空 |
| `source_name` | Source adapter 名称 |
| `raw_batch_id` | Raw batch 血缘 |
| `ingested_at` | Ingestion 时间戳 |
| `source_caveat` | Official / wrapper / vintage caveat |
| `quality_status` | Validation 派生的质量状态 |

第一批优先字段集：

- `official_pmi`
- `ppi_yoy`
- `m1_yoy`
- `m2_yoy`
- `tsf_yoy`

Confirmatory fields 可以共用同一个 dataset，但除非 source endpoint 已经稳定，否则不应阻塞第一批 macro slice：

- `cpi_yoy`
- `industrial_production_yoy`
- `retail_sales_yoy`
- `fixed_asset_investment_ytd`

Stage F 中的 derived slow fields：

- `official_pmi_mom`
- `m1_m2_spread`

这些 derived fields 必须在所需 underlying observations 都独立 effective 之后才能计算。它们可以在 canonical slow fields 写入后由一个窄 derived helper 构建，也可以延后到 Stage F read service 中计算，前提是 read service 保留同样的 timing rule。不能凭记忆在 notebook 中计算。

字段角色规则：

- `official_pmi`、`official_pmi_mom`、`ppi_yoy`、`m1_yoy`、`m2_yoy`、`m1_m2_spread` 和 `tsf_yoy` 是 `primary`。
- `cpi_yoy`、`industrial_production_yoy`、`retail_sales_yoy` 和 `fixed_asset_investment_ytd` 是 `confirmatory`。
- Read model completeness 必须区分 missing primary fields 和 missing confirmatory fields。

### 数据集 2：`rates.daily_rates`

粒度：

- `(field_name, trade_date, ingested_at)`

latest-known 业务键：

- `(field_name, trade_date)`

初始字段集：

- `shibor_1w`

后续可选字段：

- `shibor_overnight`

必需字段：

| 字段 | 含义 |
| --- | --- |
| `field_name` | Canonical rate key |
| `trade_date` | Quote date |
| `value` | 数值型 rate |
| `unit` | 显式设置的百分比或基点口径 |
| `field_role` | `primary` / `confirmatory` |
| `release_date` | 与 quote 相同的日期 |
| `effective_date` | v1 中，如果 decision clock 可用则使用 quote date |
| `source_name` | Source adapter 名称 |
| `raw_batch_id` | Raw batch 血缘 |
| `ingested_at` | Ingestion 时间戳 |
| `revision_note` | 通常为 `low_revision_risk` |
| `source_caveat` | Wrapper / same-day availability caveat |
| `quality_status` | Validation 派生的质量状态 |

### 数据集 3：`rates.lpr`

粒度：

- `(field_name, quote_date, ingested_at)`

latest-known 业务键：

- `(field_name, quote_date)`

初始字段集：

- `lpr_1y`
- `lpr_5y`

必需字段：

| 字段 | 含义 |
| --- | --- |
| `field_name` | Canonical LPR field key |
| `quote_date` | Source quote / publication date |
| `value` | 数值型 rate |
| `unit` | 显式设置的百分比或基点口径 |
| `field_role` | `lpr_1y` 为 `primary`，`lpr_5y` 为 `confirmatory` |
| `release_date` | Source quote date 或 conservative fallback date |
| `effective_date` | Quote date 或下一个 canonical open trading day |
| `source_name` | Source adapter 名称 |
| `raw_batch_id` | Raw batch 血缘 |
| `ingested_at` | Ingestion 时间戳 |
| `revision_note` | 通常为 `low_revision_risk_relative_to_other_slow_fields` |
| `source_caveat` | Wrapper / source-date fallback caveat |
| `quality_status` | Validation 派生的质量状态 |

Timing 规则：

- source date 可用时使用 source date。
- 如果不可用，赋值为当月第 20 个 calendar day。
- 如果该日期是 open trading day，`effective_date` 就是该日期；否则使用下一个 open trading day。

### 数据集 4：`rates.gov_curve_points`

粒度：

- `(curve_code, curve_date, tenor_years, ingested_at)`

latest-known 业务键：

- `(curve_code, curve_date, tenor_years)`

初始字段集：

- `cn_gov_1y_yield`
- `cn_gov_10y_yield`
- `cn_yield_curve_slope_10y_1y` 作为 derived 字段，不是 source-backed 字段

字段角色规则：

- `cn_gov_10y_yield` 是 `primary`。
- `cn_gov_1y_yield` 和 `cn_yield_curve_slope_10y_1y` 是 `confirmatory`。
- 缺失 curve slope 时应暴露为 confirmatory gap。只要 primary 字段存在，它不应让 Stage F 变成 unavailable，但应阻止下游阶段把这个 context 当成无 caveat 的完整上下文。

source-backed 曲线点必需字段：

| 字段 | 含义 |
| --- | --- |
| `curve_code` | Source / canonical 曲线标识 |
| `curve_date` | 曲线观测日期 |
| `tenor_years` | 数值型期限，例如 `1` 或 `10` |
| `field_name` | 该期限对应的 canonical field key |
| `value` | 数值型收益率 |
| `unit` | 显式设置的百分比或基点口径 |
| `field_role` | `primary` / `confirmatory` |
| `release_date` | 与曲线观测日期相同 |
| `effective_date` | 同日状态观测日期或下一期可执行日期 |
| `source_name` | Source adapter 名称 |
| `raw_batch_id` | Raw batch 血缘 |
| `ingested_at` | Ingestion 时间戳 |
| `revision_note` | 验证稳定前通常为 `extraction_method_risk_present` |
| `source_caveat` | Extraction / paging / wrapper caveat |
| `quality_status` | Validation 派生的质量状态 |

Timing 规则：

- `release_date = curve_date`
- 对 state observation 使用 `effective_date = curve_date`，并带上 `revision_note = extraction_method_risk_present`

在 windowed / paged extraction 路径完成验证之前，curve extraction 在操作层面有 caveat。Stage F 不能把这个 caveat 隐藏在 `complete` strategy context 后面。

## Source Adapter 设计

Stage F 按 dataset family 扩展 source adapter，而不是按 strategy feature 扩展。

建议第一批实现：

- 为 `macro.slow_fields` 增加 Tushare 支持
- 为 `rates.daily_rates` 增加 Tushare 支持
- 为 `rates.lpr` 增加 Tushare 支持
- 为 `rates.gov_curve_points` 增加 Tushare 支持

前序项目文档中的 source 优先级：

| Dataset | 第一批实现 | Official anchor | 第一批预期 |
| --- | --- | --- | --- |
| `macro.slow_fields` | Tushare wrapper methods | NBS / PBOC | 必须记录 source caveat |
| `rates.daily_rates` | Tushare wrapper methods | Shibor | 预期使用 wrapper；当前环境下 direct official path 不可靠 |
| `rates.lpr` | Tushare wrapper methods | PBOC / national interbank quoting center | revision risk 较低 |
| `rates.gov_curve_points` | Tushare / curve wrapper | ChinaBond / Chinamoney | 必须记录明确的 extraction caveat |

Adapter 契约：

- Source adapter 返回 provider-shaped DataFrame 和 lineage metadata。
- Source adapter 不计算 conservative effective date。
- Source adapter 不 join trading calendar。
- Source adapter 不决定字段是否允许进入 strategy logic。

Timing enrichment 应放在 normalizer 或专用 timing helper 中，因为它依赖 canonical trading calendar 和 release rule。

## Timing 规则

Stage F 必须编码 `docs/etf-all-weather-data-sources/release-date-rules-v1-slow-fields.md` 中的 release-date rules。

当前 Stage F 字段的 canonical rules：

| 字段组 | 保守 release rule | Effective rule |
| --- | --- | --- |
| Official PMI | 次月 1 日 | 下一个开市交易日 |
| CPI / PPI | 次月 12 日 | 下一个开市交易日 |
| M1 / M2 / TSF / loans | 次月 15 日 | 下一个开市交易日 |
| Industrial / retail / FAI | 次月 17 日 | 下一个开市交易日 |
| LPR | source date 或 20 日 | 当日或下一个开市交易日 |
| Shibor | quote date | 决策日期前最新已发布报价 |
| China gov curve | curve date | 日终 / 下一周期使用 |

M1-family regime tagging：

- 2025 definition boundary 之前的观测，必须与 boundary 之后的观测分开标记。
- 精确 boundary 语义应集中在一个 helper constant 中，并覆盖测试。
- `m1_yoy`、`m2_yoy` 和 `m1_m2_spread` 必须携带 `definition_regime` 和 `regime_note`。

As-of 规则：

```text
对于某个 rebalance_date，macro/rates observation 只有在满足下列条件时才是 strategy-eligible：
  effective_date <= rebalance_date
```

如果不满足这个条件，任何 Stage F read service 都不能把该字段返回为 eligible。

建议 helper 边界：

- 把 release-rule constants 和 next-open-day 逻辑放进一个专用 helper，例如 `tradepilot/etl/timing.py`。
- Normalizer 调用这个 helper；source adapter 不调用。
- 测试应使用 canonical trading-calendar rows 覆盖节假日 / 周末行为，而不是依赖 Python weekday 假设。

## Registry 增量

在 `tradepilot/etl/datasets.py` 中增加 dataset definition：

- `build_macro_slow_fields_dataset()`
- `build_rates_daily_rates_dataset()`
- `build_rates_lpr_dataset()`
- `build_rates_gov_curve_points_dataset()`

建议依赖声明：

| Dataset | 依赖 |
| --- | --- |
| `macro.slow_fields` | `reference.trading_calendar` |
| `rates.daily_rates` | `reference.trading_calendar` |
| `rates.lpr` | `reference.trading_calendar` |
| `rates.gov_curve_points` | `reference.trading_calendar` |

所有 dataset 都应使用 `DependencyType.WINDOW` 覆盖 calendar coverage。

注册说明：

- 当前 `register_stage_b_datasets()` 虽然名称是历史遗留，但实际注册所有内置 ETL dataset。
- Stage F 第一批实现应把新 definition 加到同一条注册流程。
- 将 `build_stage_b_datasets()` / `register_stage_b_datasets()` 重命名为更宽泛名称可以作为后续 cleanup，但不应与 Stage F 数据工作绑在一起。

## Normalization 设计

在 `tradepilot/etl/normalizers.py` 中增加 normalizer：

- `SlowFieldsNormalizer`
- `DailyRatesNormalizer`
- `LprNormalizer`
- `GovCurvePointsNormalizer`

共享 helper 职责：

- 解析 source date 和 period label
- 标准化 field name
- 标准化数值
- 附加 unit
- 附加 `field_role`
- 附加 `source_name`、`raw_batch_id`、`ingested_at`、`quality_status`
- 计算 `release_date`
- 使用 `canonical_trading_calendar` 计算 `effective_date`
- 附加 `revision_note`
- 附加 `source_caveat`
- 附加 M1-family regime metadata

不要把 release / effective date 的构造留给 Stage G、notebook 或 frontend code。

## Canonical Storage

Stage F 应把 normalized facts 写入 lakehouse normalized zone，并在实际可行时使用 year/month 分区。

建议存储路径：

- `data/lakehouse/normalized/macro.slow_fields/year=YYYY/part-00000.parquet`
- `data/lakehouse/normalized/rates.daily_rates/year=YYYY/month=MM/part-00000.parquet`
- `data/lakehouse/normalized/rates.lpr/year=YYYY/part-00000.parquet`
- `data/lakehouse/normalized/rates.gov_curve_points/year=YYYY/month=MM/part-00000.parquet`

写入规则：

- Raw landing 不可变。
- Canonical normalized parquet 按 latest-known business key 做幂等 upsert。
- Revisable dataset 必须保留 `ingested_at`。
- 如果 raw batch 保留了 replayability，第一批实现可以在 serving parquet 中只保留每个 key 的最新行，但设计上不能假装它拥有 vintage history。

如果 version-preserving parquet 对第一批 slice 来说过于复杂，应保持实现简单，并通过 `revision_note` 和测试记录这个限制。

Writer 集成说明：

- 尽量复用 `_write_year_month_partition_upsert()`。
- 在运行 dataset sync 之前，为 Stage F dataset 增加明确的 `_write_canonical()` 分支。
- 不要依赖当前 `_write_canonical()` 的 fallback path，因为未知 dataset 目前会流入 market-daily-style 写入，并预期存在 `instrument_id` / `trade_date`。
- 如果能让 as-of 读取更简单，`macro.slow_fields` 和 `rates.lpr` 使用 `partition_date_column="effective_date"`；daily rates 和 curve points 使用 `trade_date` / `curve_date`。

## Validation 设计

在 `tradepilot/etl/validators.py` 中增加 validator。

### `macro.slow_fields` 检查

- latest-known key 不重复
- 必需字段存在
- 数值有限
- unit 在允许集合中
- release_date 存在
- effective_date 存在
- `effective_date >= release_date`
- effective_date 是 canonical open trading day
- M1-family regime fields 存在
- derived field timing 不早于 underlying fields
- field name 在 allowed v1 set 中
- field_role 匹配 allowed v1 role map
- revision_note 存在
- 当 source 是 wrapper-only 或 latest-history-only 时，source_caveat 存在

### `rates.daily_rates` 检查

- latest-known key 不重复
- value 有限且 plausible
- unit 在允许集合中
- trade_date 存在
- effective_date 存在
- 基于 trading calendar 的 continuity warning
- revision_note 存在
- field_role 匹配 allowed v1 role map
- 未使用 official direct source 时，source_caveat 存在

### `rates.lpr` 检查

- latest-known key 不重复
- 只允许 allowed tenors
- value 有限且 plausible
- quote_date / release_date / effective_date 存在
- 每个 tenor 在每个 publication date 上只有一条 observation
- field_role 匹配 allowed v1 role map
- 当 source quote date 是推断值或 source 是 wrapper-only 时，source_caveat 存在

### `rates.gov_curve_points` 检查

- latest-known key 不重复
- 构建 slope 时 1Y 和 10Y required tenors 存在
- yield values 有限且 plausible
- tenor_years 为正
- curve_date 存在
- extraction caveat 已记录
- field_role 匹配 allowed v1 role map
- source_caveat 存在

Validation status 应复用现有 framework labels：

- `pass`
- `pass_with_caveat`
- `warning`
- `defer`
- `fail`
- `validation_only`

除非必需字段缺失，latest-history macro 或 curve extraction risk 这类 operational caveat 通常应是 `pass_with_caveat`，不是 `fail`。

## Read Model

Stage F 应在 `tradepilot/etl/read_models.py` 中增加一个窄 read service。

建议函数：

- `get_latest_etf_aw_macro_rates_context(as_of_date)`
- `list_etf_aw_macro_rates_contexts(start, end)`

read service 应该：

- 读取 canonical macro / rates / curve datasets
- 读取 `canonical_rebalance_calendar`
- 选择满足 `effective_date <= rebalance_date` 的最新 eligible observations
- 显式暴露 missing fields
- 区分 missing primary fields 和 missing confirmatory fields
- 暴露 revision 和 caveat notes
- 不计算 target budget 或 allocation weights

建议 JSON contract：

```json
{
  "schema_version": "etf_aw_macro_rates_context_v1",
  "rebalance_date": "2026-04-20",
  "context_status": "partial",
  "macro_fields": [
    {
      "field_name": "official_pmi",
      "field_role": "primary",
      "period_label": "2026-03",
      "value": 50.8,
      "unit": "index_point",
      "release_date": "2026-04-01",
      "effective_date": "2026-04-01",
      "revision_note": "latest_history_only_unless_vintage_captured",
      "quality_status": "pass_with_caveat"
    }
  ],
  "rates_fields": [
    {
      "field_name": "shibor_1w",
      "field_role": "primary",
      "trade_date": "2026-04-20",
      "value": 1.85,
      "unit": "percent",
      "effective_date": "2026-04-20",
      "quality_status": "pass"
    }
  ],
  "quality_notes": {
    "market_context_joined": false,
    "missing_primary_fields": ["cn_gov_10y_yield"],
    "missing_confirmatory_fields": ["cn_yield_curve_slope_10y_1y"],
    "latest_history_macro": true
  }
}
```

`context_status` 允许值：

- `complete`
- `partial`
- `stale`
- `unavailable`

Stage F read service 可以返回 `partial`；它不应静默省略缺失的 canonical v1 fields。

## 实施顺序

Stage F 应按小 slice 实现。

### Slice F0：当前 ETL 集成预检查

目标：

- 对照当前 Stage B/E 代码确认 registration、writer、source adapter、normalizer、validator 和 read-model 扩展点。
- 这个 slice 不增加 source fetch。

验证：

- `python -m unittest -v tests/etl/test_registry.py tests/etl/test_models.py`

### Slice F1：Timing 基础

目标：

- 增加 release-rule helper 和 effective-date helper。
- 增加 release date 和 next-open-day 行为测试。

验证：

- `python -m unittest -v tests/etl/test_stage_f_timing.py tests/etl/test_stage_c.py`

### Slice F2：`rates.lpr` 与 `rates.daily_rates`

目标：

- 先增加 revision risk 较低的 rates dataset，用来验证 Stage F 机制。
- 包含带 timing 和 validation 的 `lpr_1y`、`lpr_5y` 和 `shibor_1w`。

验证：

- `python -m unittest -v tests/etl/test_stage_f_rates.py`

### Slice F3：`macro.slow_fields`

目标：

- 增加 registry、Tushare fetch support、normalizer、validator 和 canonical write。
- 先为 primary macro fields 回填一个窄测试窗口。

验证：

- `python -m unittest -v tests/etl/test_stage_f_macro.py`

### Slice F4：`rates.gov_curve_points`

目标：

- 增加带明确 extraction caveat 的 government curve point ingestion。
- 仅在 1Y/10Y 两个 endpoint 都存在后，才增加 slope derivation。

验证：

- `python -m unittest -v tests/etl/test_stage_f_curve.py`

### Slice F5：Macro/Rates Read Service

目标：

- 增加按 rebalance date 返回 eligible fields 的 read service。
- 输出与 market-only regime score 保持分离。

验证：

- `python -m unittest -v tests/etl/test_stage_f_read_models.py`

## 验收标准

Stage F 完成条件：

1. `macro.slow_fields` 能跑通 raw landing、normalization、validation、canonical write 和 watermark update。
2. `rates.daily_rates` 能跑通同一条 ETL path。
3. `rates.lpr` 能跑通同一条 ETL path。
4. `rates.gov_curve_points` 能跑通同一条 ETL path，并且 extraction caveat 可见。
5. Stage F dataset 通过当前 built-in registry path 注册，并拥有明确的 canonical writer branches。
6. Slow fields 始终包含 `field_role`、`release_date`、`effective_date`、`revision_note` 和必需 source caveat。
7. M1-family fields 始终包含 `definition_regime` 和 `regime_note`。
8. Effective date 从 canonical trading calendar 计算，而不是从 ad hoc weekday logic 计算。
9. Read service 绝不把 `effective_date > rebalance_date` 的 observation 返回为 eligible。
10. Missing macro/rates fields 在 `quality_notes` 中表示，并区分 primary gap 和 confirmatory gap。
11. Stage B/C/D/E tests 继续通过。
12. `python -c "from tradepilot.main import app; print('OK')"` 通过。
13. Stage F 不需要 C-Demo 式老师验收；验收依据是上述 repo 内可验证结果。

## 测试策略

建议测试文件：

- `tests/etl/test_stage_f_timing.py`
- `tests/etl/test_stage_f_macro.py`
- `tests/etl/test_stage_f_rates.py`
- `tests/etl/test_stage_f_curve.py`
- `tests/etl/test_stage_f_read_models.py`

最低测试用例：

1. PMI period `2026-03` 在 `2026-04-01` release，effective date 是下一个 canonical open day。
2. CPI/PPI period 使用 conservative day 12 rule。
3. M1/M2/TSF 使用 conservative day 15 rule。
4. Industrial / retail / FAI 使用 conservative day 17 rule。
5. LPR 在 source quote date 存在时使用该日期。
6. LPR 在 source date 缺失时 fallback 到 20 日。
7. Effective date 会跳过闭市交易日。
8. Read service 排除 effective date 晚于 rebalance date 的字段。
9. 缺少 regime metadata 的 M1-family rows validation fail。
10. 只有 1Y 和 10Y endpoints 都存在时，curve slope 才可用。
11. 重跑同一窗口不会产生 duplicate latest-known business keys。
12. Missing primary fields 对 context 的降级强于 missing confirmatory fields。
13. Wrapper-only 或 latest-history-only source caveat 会出现在 read model 中。
14. Stage F canonical write 不调用 market daily writer path。

## 风险

### 1. 隐性 Lookahead

风险：

- Latest-history macro data 可能被误认为真实 historical vintage。

缓解：

- 必须有 `revision_note`。
- Read model 暴露 `latest_history_macro = true`。
- Stage G 必须把它作为 confidence caveat 处理。

### 2. Source 覆盖波动

风险：

- Macro 和 curve endpoint 可能存在历史覆盖不均或 schema drift。

缓解：

- 使用小 backfill window。
- 持久化 raw batches。
- 记录 validation caveat，而不是静默把 context 标成 complete。

### 3. Stage G 范围蔓延

风险：

- Macro/rates ingestion 开始产出 strategy label、budget 或 weight。

缓解：

- Stage F output 命名为 `macro_rates_context`，而不是 final regime 或 allocation。
- Validator 和测试应拒绝 Stage F contract 中出现 budget / weight / trade-action fields。

### 4. Curve Extraction 风险

风险：

- Government curve source extraction 在长窗口下可能截断或变化。

缓解：

- 使用 windowed fetch。
- 要求 tenor completeness checks。
- 在证明稳定之前保留 `extraction_method_risk_present`。

### 5. 历史 helper 名称

风险：

- `build_stage_b_datasets()` / `register_stage_b_datasets()` 名称可能误导实现者，以为 Stage F 需要单独的 registry system。

缓解：

- 在有充分理由做专门 cleanup 之前，把这些名称视为历史遗留。
- 通过现有路径注册 Stage F，并在 implementation summary 中说明 naming debt。

## 最终判断

Stage F 应交付 timing-safe macro / rates canonical layer，而不是完整 strategy engine。

正确输出是一小组 source-backed datasets，加上一个能够回答下列问题的 read service：

- 截至某个 rebalance date，哪些 macro / rates observations 可用
- 这些 observation 携带哪些 timing 和 revision caveat
- 哪些 required v1 fields 缺失、stale 或带 caveat

只有在这个 contract 稳定之后，Stage G 才应合并 market-only regime、macro/rates context 和 derived strategy snapshot logic。
