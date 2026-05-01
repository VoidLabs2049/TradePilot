---
title: "Data Ingestion Stage C"
status: draft
mode: "design"
created: 2026-04-29
updated: 2026-04-29
modules: ["backend"]
---

# Data Ingestion Stage C

## Overview

本文档收缩 Stage C 范围。

`tradepilot/etl/` 的 Stage B 已经完成第一条真实可执行的 ETL 纵切链路：Tushare source adapter、raw Parquet landing、normalized canonical write、DuckDB metadata、validation gating、dependency preflight、watermark advancement 都已经落地。

因此 Stage C 的第一步不应该继续扩张为完整 ETF all-weather serious panel，也不应该同时推进 rebalance calendar、macro、rates、curve、snapshot、profile runner。

Stage C 的正确起点是：

> 先在现有 Stage B ETL foundation 上，补全 SH/SZ 完整交易日历历史数据，形成后续 ETF all-weather v1 的时间基座。

这一步完成后，后续 `reference.rebalance_calendar`、frozen sleeves、adjustment-aware ETF panel 才有稳定基础。

## Divide-And-Conquer Closure Rule

Stage C 后续设计采用分治法收口，不再用一份宽泛设计同时覆盖 reference、market、macro、rates、derived snapshot、scheduler/DAG 等多类问题。

每次推进只能选择一个可执行切片，并明确回答四个问题：

1. 这个切片的 canonical 业务键是什么。
2. 它走 `run_dataset_sync()` 主路径，还是临时窄范围 `run_bootstrap()` materialization profile。
3. 它依赖哪些已存在 canonical/normalized 数据。
4. 它的幂等覆盖规则和最小测试是什么。

当前 Stage C 收口后的分治边界如下：

- Source-backed dataset：继续走 registry / normalizer / validator / writer 主路径，例如 `market.etf_daily`、`market.etf_adj_factor`。
- Static or derived reference：暂时走窄范围 bootstrap profile，例如 frozen sleeves 和 monthly rebalance calendar；这类 profile 只负责 materialization，不演化成通用 DAG runner。
- Derived market panel：只读取已落盘 normalized inputs 并写 derived output，例如 `derived.etf_aw_sleeve_daily`；不在 derived builder 内发起 source fetch。
- Shared storage mechanics：只抽业务无关的 year/month partition upsert writer；业务规则仍留在对应 dataset/profile 内。
- Business constants：ETF all-weather v1 sleeves、rebalance anchor day、profile name 必须集中定义，查询和校验复用同一入口。

后续新增设计必须优先写成一个小切片，而不是“完整 ETF all-weather 数据平台”式的大范围方案。除非当前切片已经通过测试并稳定，否则不同时推进 macro、rates、curve、rebalance snapshot、generic profile runner 或 scheduler。

## Why Stage C Must Start Here

当前 Stage B 的 `reference.trading_calendar` 已经具备以下能力：

- 可从 Tushare 获取交易日历数据
- 可标准化为 `(exchange, trade_date)` canonical rows
- 可执行结构化 validation
- 可写入 DuckDB `canonical_trading_calendar`
- 可作为 `market.*` dataset 的 dependency preflight 基础

但当前能力仍主要是**按请求窗口补数**，还不是**完整历史 bootstrap/backfill**。

如果在没有完整 SH/SZ 历史交易日历的情况下直接推进 ETF all-weather Stage C 后续内容，会出现以下问题：

1. `reference.rebalance_calendar` 无法稳定生成共同开市日规则
2. ETF / index daily 的历史窗口依赖仍然是不完整的
3. 后续 `effective_date` 规则没有可靠 calendar foundation
4. macro / rates / curve 的时间语义验证会失去统一底座

所以 Stage C 第一阶段必须先解决交易日历全量历史覆盖问题。

## Relationship With Stage B

Stage C 继续沿用 Stage B 已冻结的边界：

- 继续使用 `tradepilot/etl/` 作为统一 ETL foundation
- 继续使用 `ETLService.run_dataset_sync()` 作为最小执行单元
- 继续使用 Tushare source adapter 的 typed `DataFrame` fetch contract
- 继续使用 `canonical_trading_calendar` 作为 canonical destination
- 继续使用 `etl_ingestion_runs`、`etl_raw_batches`、`etl_validation_results`、`etl_source_watermarks`
- 继续使用 raw immutable append + structured validation + canonical write 的执行模型

Stage C 第一阶段不重新设计 source adapter、normalizer、validator，也不改写 canonical schema。

## Stage C First-Phase Goal

- [ ] 为 `reference.trading_calendar` 增加可执行的 SH/SZ 全量历史 bootstrap 入口
- [ ] 以按月分块方式回补 SH/SZ 完整历史交易日历
- [ ] 支持重跑时跳过已完整覆盖的月份
- [ ] 保证历史回补不会让 watermark 回退
- [ ] 为后续 `reference.rebalance_calendar` 提供共同开市日可推导的完整基础数据

## Stage C First-Phase Non-Goals

本阶段明确不做：

- 通用 profile runner
- DAG scheduler
- `reference.rebalance_calendar` 物化
- `reference.etf_aw_sleeves`
- `market.etf_adj_factor`
- `derived.etf_aw_sleeve_daily`
- `macro.slow_fields`
- `rates.daily_rates`
- `rates.lpr`
- `rates.gov_curve_points`
- `derived.etf_aw_market_features`
- `derived.etf_aw_rebalance_snapshot`
- coverage metadata framework
- market dataset dependency auto-run 的全量 bootstrap 化

## Immediate Scope

### Dataset In Scope

- `reference.trading_calendar`

### Canonical Destination

- DuckDB `canonical_trading_calendar`

### Business Key

- `(exchange, trade_date)`

### Required Exchange Scope

初版固定：

- `SH`
- `SZ`

这不是可选配置，而是 ETF all-weather v1 第一阶段的必要基础范围。

## Existing Implementation To Reuse

Stage C 第一阶段必须直接复用已有能力，而不是再造一套新机制。

### Existing dataset definition

- `tradepilot/etl/datasets.py`
- 现有 `reference.trading_calendar` registry definition 保持不变

### Existing source path

- `tradepilot/etl/sources/tushare.py`
- 继续使用现有 `trade_cal` 拉取逻辑
- 继续使用现有 `exchange` / `exchanges` 上下文语义
- 继续使用 SH/SZ canonicalization

### Existing normalization path

- `tradepilot/etl/normalizers.py`
- 继续使用 `TradingCalendarNormalizer`

### Existing validation path

- `tradepilot/etl/validators.py`
- 继续使用 `TradingCalendarValidator`
- 继续保留：duplicate key、trade_date required、exchange supported、is_open boolean、pretrade_date 顺序、自然日连续性检查

### Existing canonical write path

- `tradepilot/etl/service.py`
- 继续使用 `_write_trading_calendar(...)`

## Practical Problem To Solve

当前 `reference.trading_calendar` 已经可以被单次 `run_dataset_sync()` 执行，但仍存在以下现实缺口：

1. 没有 bootstrap 入口
2. 没有“从历史起点一直回补到今天”的 orchestration
3. 没有“按月分块”的历史补数循环
4. 没有“已完整覆盖月份跳过”的能力
5. 当前 watermark 语义更适合 incremental，不适合历史回补

Stage C 第一阶段只解决这些问题。

## Recommended Design

### 1. Add a narrow bootstrap entry

在 `ETLService` 中实现一个**窄范围 bootstrap dispatcher**。

建议 profile name：

- `reference.trading_calendar.full_history`

边界：

- 只支持交易日历 bootstrap
- 不引入通用 profile framework
- 不引入 dataset graph
- 不承担后续所有 Stage C dataset orchestration

### 2. Bootstrap operating model

目标模型：

1. 指定一个明确的历史起点
2. 结束日期取当前日期
3. 按月拆分窗口
4. 对每个月窗口调用已有 `run_dataset_sync("reference.trading_calendar", request)`
5. `context = {"exchanges": ["SH", "SZ"]}`
6. `trigger_mode = backfill`
7. 对已完整覆盖月份直接跳过
8. 对缺失月份继续执行 ETL

默认历史起点取 `2016-01-01`。这足以覆盖当前 ETF all-weather v1 的研究与生产基础窗口；调用方仍可为测试、修复或受限回补显式传入 `start` / `end` 覆盖窗口。

这意味着 Stage C 第一阶段不是新写一套 calendar loader，而是**用现有单 dataset ETL path 做月度循环 backfill**。

### 3. Full-history fetch remains service-driven

不在 source adapter 中增加“全历史抓取接口”。

保持当前原则：

- source adapter 只处理单窗口 fetch
- full-history orchestration 在 service 层完成

原因：

- 不破坏 Stage B 已稳定的 typed fetch contract
- 月度分块更容易重试、审计和定位问题
- raw batch 的目录与 metadata 语义保持一致

### 4. Coverage check must be reusable

在执行某个月窗口前，需要检查该月 SH/SZ 是否都已经完整存在于 `canonical_trading_calendar`。

推荐做法：

- 从现有 dependency preflight 中抽出 calendar window coverage helper
- bootstrap path 与 downstream dependency preflight 共用同一套“窗口覆盖完整”判定

完整覆盖的定义：

- SH 与 SZ 两个交易所都存在
- 对请求窗口内的自然日覆盖完整
- 每个交易所的最小/最大日期与窗口边界一致

这样可以保证：

- bootstrap 可重复执行
- 已完成月份不会重复跑
- coverage 语义在 service 内保持一致

### 5. Watermark must remain monotonic

当前 watermark 模型仍可保留，但语义需要收紧。

规则：

- 历史 backfill 不允许把 `latest_fetched_date` 回退
- watermark 更新必须是单调的
- `latest_available_date` / `latest_fetched_date` 应取 `max(existing, new)`

理解方式：

- `etl_source_watermarks` 在本阶段主要表示 freshness upper bound
- 它不能单独证明“完整历史已经全部覆盖”
- 历史完整性仍应以 `canonical_trading_calendar` 的覆盖检查为准

### 6. Canonical schema remains unchanged

本阶段不修改以下表结构：

- `canonical_trading_calendar`
- `etl_ingestion_runs`
- `etl_raw_batches`
- `etl_validation_results`
- `etl_source_watermarks`

原因：

- 当前问题是 orchestration 和 completeness，不是 schema 不足
- `canonical_trading_calendar` 已足以表达交易日历 canonical facts
- coverage 可以通过查询现有 canonical table 验证

## Validation And Success Criteria

### Validation during backfill

每个窗口仍然沿用现有 Stage B validation：

- duplicate key absence
- `trade_date` required
- supported exchange only
- `is_open` boolean
- `pretrade_date < trade_date`
- open-day `pretrade_date` sequence
- natural-day continuity within the window

### Additional bootstrap-level checks

bootstrap 完成后，应额外确认：

1. SH 和 SZ 都有完整覆盖
2. 不存在重复 `(exchange, trade_date)`
3. watermark 没有因历史窗口回补而倒退
4. 对后续共同开市日推导已具备数据条件

## Testing Strategy

在 `tests/etl/test_stage_b.py` 增加面向 Stage C 第一阶段的补充测试。

### Required tests

1. `run_bootstrap("reference.trading_calendar.full_history")` 可以执行
2. bootstrap 使用 `TriggerMode.BACKFILL`
3. bootstrap 以月为单位调用已有 `run_dataset_sync()`
4. bootstrap 会向 source path 传入 `SH` / `SZ`
5. 已完整覆盖月份会被跳过
6. 重复执行 bootstrap 不会产生重复 `(exchange, trade_date)`
7. 历史 backfill 不会让 `etl_source_watermarks.latest_fetched_date` 回退
8. 多月 bootstrap 后，`canonical_trading_calendar` 对 SH/SZ 覆盖完整

### Stage B regression requirement

- 所有现有 Stage B 测试继续通过
- Stage C 第一阶段不能破坏当前 market dependency preflight

## Acceptance Criteria

Stage C 第一阶段完成的标准为：

1. `ETLService.run_bootstrap("reference.trading_calendar.full_history")` 可执行。
2. bootstrap 能用 SH/SZ 双交易所口径按月回补历史交易日历。
3. 重跑时会跳过已完整覆盖月份。
4. `canonical_trading_calendar` 在目标窗口内对 SH/SZ 都具备完整自然日覆盖。
5. 不会产生重复 `(exchange, trade_date)` 业务键。
6. watermark 在历史回补后保持单调，不会回退。
7. 现有 `market.*` dataset 的 dependency preflight 行为无回归。
8. 后续可以基于 `canonical_trading_calendar` 推导 SH/SZ 共同开市日。

## Recommended Implementation Sequence

1. 在 `tradepilot/etl/service.py` 实现窄范围 `run_bootstrap()` dispatcher
2. 增加 `reference.trading_calendar` bootstrap helper
3. 抽出并复用 calendar coverage helper
4. 收紧 watermark 更新语义为单调更新
5. 在 `tests/etl/test_stage_b.py` 增加 bootstrap / skip / idempotency / watermark 测试
6. 用临时 DuckDB 与临时 lakehouse root 验证 SH/SZ historical coverage

## Deferred After This Phase

以下内容明确留到交易日历基础完成之后：

- `reference.rebalance_calendar`
- `reference.etf_aw_sleeves`
- `market.etf_adj_factor`
- `derived.etf_aw_sleeve_daily`
- benchmark / market confirmation features
- slow macro / rates / curve datasets
- rebalance snapshot

## Final Judgment

Stage C 不应从“完整 ETF all-weather serious panel”开始。

Stage C 的真实第一步应当是：

> 先把 `reference.trading_calendar` 从“窗口型可执行 dataset”推进到“可稳定构建 SH/SZ 完整历史交易日历的基础设施能力”。

只有这一步稳定后，后续共同开市日 rebalance calendar、ETF all-weather frozen universe、adjustment-aware sleeve panel 才有可靠时间基座。
