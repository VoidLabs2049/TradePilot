---
title: "Data Ingestion Stage B"
status: draft
mode: "design"
created: 2026-04-25
updated: 2026-04-25
modules: ["backend"]
---

# Data Ingestion Stage B

## Overview

本文档是 `data-ingestion-stage-a.md` 和 `data-ingestion-architecture.md` 之后的下一阶段落地设计。

Stage A 已经把 `tradepilot/etl/` 的骨架、DuckDB metadata schema、lakehouse 根目录约定、dataset registry 和基础协议落下，但它仍然是“可定义、不可执行”的 foundation skeleton。Stage B 的职责，是把这套骨架推进到“第一条真实可运行的 ETL 纵切链路”。

Stage B 不追求一次性完成 ETF all-weather Stage 1 的全部 serious panel，也不在这一阶段接入 scheduler、profile orchestration、复杂并发控制和多 source fallback。它的目标是先把新 ETL foundation 从静态骨架推进到可实际 ingest 首批通用 dataset 的运行态系统。

## Relationship With Stage A

Stage A 已冻结的边界，在 Stage B 中继续成立：

- 继续保持 additive only，不替换现有 `tradepilot/ingestion/` 与现有 API 路径
- 继续保留 `tradepilot/etl/` 作为独立演进的新 ETL foundation
- 继续沿用 `etl_*` metadata tables、`canonical_*` reference tables、`data/lakehouse/*` zone 约定
- 继续采用 dataset-oriented 模型，而不是围绕旧 provider 方法堆功能

Stage B 的变化点是：

- 把 `service.py` 从占位 contract 推进为真实可执行的单 dataset orchestration
- 把 `storage.py` 从“只规划路径”推进为“可写 raw / normalized 输出”
- 把 `sources/base.py` 从抽象协议推进到首个真实 source adapter
- 把 `normalizers.py` 和 `validators.py` 从抽象协议推进到首批 dataset 的实际实现
- 把 registry 从“可注册”推进到“注册后可直接被 service 消费”

## Stage B Goals

- [ ] 让 `tradepilot.etl.ETLService.run_dataset_sync()` 具备真实执行能力
- [ ] 交付首批可运行的通用 dataset：`reference.trading_calendar`、`reference.instruments`、`market.etf_daily`、`market.index_daily`
- [ ] 实现 raw-first 的真实落盘路径与 batch manifest 记录
- [ ] 实现首批 deterministic normalizer 与 dataset-specific validator
- [ ] 完成 DuckDB metadata 持久化、reference table 写入与 watermark 推进
- [ ] 为 Stage C 的 ETF all-weather serious panel 提供稳定的 market/reference ingestion pattern

## Stage B Non-Goals

Stage B 明确不包含以下内容：

- 宏观 slow fields、rates、curve points 的正式实现
- `reference.rebalance_calendar` 的正式生成逻辑
- profile runner、dependency scheduler、跨进程并发锁
- 完整多 source fallback / validation-source 切换框架
- 通用 read model、freshness dashboard、health API
- 现有 `tradepilot/ingestion/service.py` 的替换或迁移
- 广义 schema evolution、compaction、retention、lakehouse lifecycle management

## Why Stage B Exists

如果在 Stage A 之后继续停留在抽象层，后续 Stage C 会被迫在“service contract 还没稳定、raw batch 还没真实落盘、validation 还没定义 blocking 语义”的状态下同时推进 ETF、index、macro、rates，复杂度会突然陡增。

因此 Stage B 的正确职责不是“开始做 ETF all-weather 策略数据”，而是先收敛出一套最小但真实的执行路径：

1. 有真实 source adapter
2. 有真实 raw batch 落盘
3. 有真实 canonical normalize
4. 有真实 validation gating
5. 有真实 metadata / watermark 更新

只要这条链路跑通，Stage C 就可以在同一条执行模型上继续增加数据集，而不是重新设计引擎。

## Scope

### Modules Involved

| Module |
|--------|
| Backend |

### Primary Files

| File | Stage B Role |
|------|--------------|
| `tradepilot/etl/service.py` | 实现单 dataset 同步 orchestration |
| `tradepilot/etl/models.py` | 增加运行期结果模型与 typed fetch / sync contract |
| `tradepilot/etl/datasets.py` | 增加 Stage B 首批 dataset definition |
| `tradepilot/etl/registry.py` | 注册和暴露首批 dataset |
| `tradepilot/etl/storage.py` | 增加 raw / normalized 文件写入与命名规则 |
| `tradepilot/etl/normalizers.py` | 增加首批 dataset normalizer 实现 |
| `tradepilot/etl/validators.py` | 增加首批 dataset validator 实现与规则映射 |
| `tradepilot/etl/sources/base.py` | 收紧 fetch contract，避免 `Any` 继续扩散 |
| `tradepilot/etl/sources/tushare.py` | Stage B 首个真实 source adapter |
| `tradepilot/db.py` | 参考表 schema 微调、metadata 读写支撑 |
| `tradepilot/data/tushare_client.py` | 作为新 source adapter 的底层 client 复用边界 |

### Out of Scope Files For Stage B

- `tradepilot/ingestion/service.py`
- `tradepilot/api/*`
- `tradepilot/scheduler/*`
- `tradepilot/workflow/*`
- `tradepilot/etf_all_weather/*`

Stage B 可以复用旧 client 或 provider 中稳定的取数逻辑，但不能把新 ETL path 反向耦合到旧 orchestration。

## Deliverables

Stage B 的交付物应收敛为以下 6 类。

### 1. Executable Single-Dataset ETL Flow

Stage B 必须把 `ETLService.run_dataset_sync(dataset_name, request)` 落成真实执行链路。

最小真实流程应为：

1. 从 registry 解析 `DatasetDefinition`
2. 选择 primary source adapter
3. 创建 `etl_ingestion_runs` 记录并置为 `running`
4. 从 source fetch 原始 payload
5. 落 raw batch 到 `data/lakehouse/raw/`
6. 记录 `etl_raw_batches`
7. 运行 normalizer 生成 canonical payload
8. 运行 validator 生成结构化校验结果
9. 若校验通过或仅 warning，则写入 canonical destination
10. 写入 `etl_validation_results`
11. 若本次 run 成功，则推进 `etl_source_watermarks`
12. 更新 `etl_ingestion_runs` 最终状态

Stage B 不要求实现复杂 DAG runner。唯一必须稳定的入口是“单 dataset 同步”。

`run_multi_dataset_sync()` 在 Stage B 最多只应作为顺序执行薄封装，不做 profile 依赖排序。

### 2. First Concrete Datasets

Stage B 应正式交付以下 4 个 dataset definition，并保证它们可被新 ETL service 真实执行。

#### `reference.trading_calendar`

作用：

- 为 market datasets 提供校验和日期对齐基础
- 为后续 `effective_date` enrichment 保留日历依赖

建议 source：

- primary: `tushare`

建议粒度：

- `(exchange, trade_date)`

建议 canonical destination：

- DuckDB `canonical_trading_calendar`

#### `reference.instruments`

作用：

- 为 `market.etf_daily` 和 `market.index_daily` 提供 instrument universe 与基础元信息

建议 Stage B 收敛边界：

- 只要求覆盖 ETF 和 index 两类 instrument
- 不在本阶段扩张到完整 stock master
- 不在本阶段引入 benchmark map、sector map、复杂基金分类

建议 canonical destination：

- DuckDB `canonical_instruments`

#### `market.etf_daily`

作用：

- 作为第一条 market-daily 真实事实表路径
- 为 Stage C ETF all-weather sleeve daily data 提供复用模式

建议 canonical destination：

- `data/lakehouse/normalized/market.etf_daily/`

#### `market.index_daily`

作用：

- 与 ETF daily 共用 market-daily ingestion pattern
- 为 Stage C benchmark series 提前打通同类路径

建议 canonical destination：

- `data/lakehouse/normalized/market.index_daily/`

这 4 个 dataset 的组合，正好覆盖 Stage B 在总设计文档中的定义：

- trading calendar ingestion
- instrument metadata ingestion
- core market-daily ingestion pattern
- initial validation engine

### 3. Real Source Adapter Contract

Stage A 的 `BaseSourceAdapter.fetch()` 返回 `Any`，这是故意的占位。但在 Stage B 里，继续保持 `Any` 会让 raw landing、normalization、validation 和 metadata 写入之间缺乏统一 contract。

因此 Stage B 应把 source fetch contract 收紧为 typed result。

建议新增模型：

- `SourceFetchResult`
- `RawPayloadEnvelope`
- `DatasetSyncResult`

其中 `SourceFetchResult` 至少应包含：

- `dataset_name`
- `source_name`
- `source_endpoint`
- `payload`
- `row_count`
- `window_start`
- `window_end`
- `partition_hints`
- `fetched_at`
- `schema_version`
- `is_fallback_source`

这里的 `payload` 在 Stage B 可以先收敛为 `pandas.DataFrame` 或“行字典列表”，不要求设计完整跨格式对象模型。

Stage B 首个真实 adapter 应为：

- `tradepilot/etl/sources/tushare.py`

复用原则：

- 可以复用 `tradepilot/data/tushare_client.py` 中稳定且已存在的取数方法
- 不要通过旧 `ingestion/service.py` 间接取数
- 不要把旧 `DataProvider` 作为新 ETL 的强依赖接口

### 4. Raw Landing And Storage Contract

Stage A 只定了 zone 和目录层级；Stage B 需要把“真实怎么写文件”定下来。

建议 Stage B 采用以下规则。

#### Raw file format

对于 Stage B 的 4 个 dataset，raw batch 统一先使用 Parquet：

- Tushare 返回本质上是结构化表格，直接落 Parquet 最稳妥
- 可保留列级 fidelity，且便于后续 replay / normalize / sampling
- 对 Stage B 而言，没有必要为了 tabular dataset 引入 JSONL 和 sidecar manifest 双轨复杂度

#### Raw partition rules

- `reference.trading_calendar`: `year=YYYY/month=MM`
- `reference.instruments`: `snapshot_date=YYYY-MM-DD`
- `market.etf_daily`: `year=YYYY/month=MM`
- `market.index_daily`: `year=YYYY/month=MM`

#### Raw file naming

建议统一为：

- `batch-<raw_batch_id>.parquet`

不要在 Stage B 引入包含 hash、版本、复杂窗口编码的长文件名。批次身份以 metadata 表为准，文件名只承担最小定位职责。

#### Storage path semantics

`etl_raw_batches.storage_path` 建议保存相对 `LAKEHOUSE_ROOT` 的相对路径，而不是机器相关的绝对路径。这样后续测试、迁移和本地重放都更稳定。

#### Directory side effects

Stage B 之后，`storage.py` 应区分两类能力：

- 纯路径规划函数
- 显式写入函数

只有显式写入函数负责 `mkdir(parents=True, exist_ok=True)`。纯路径规划函数仍保持无副作用。

### 5. Canonical Loading Rules

Stage B 需要正式决定“不同 dataset 写到哪里，以及如何保持幂等”。

#### Reference datasets

`reference.trading_calendar` 和 `reference.instruments` 在 Stage B 应写入 DuckDB canonical tables，而不是额外复制一份 normalized Parquet。原因是：

- 数据量小
- 读写简单
- 后续 market dataset validation 需要快速 join
- 这些表本来就在 Stage A 中被定义为 small reference tables

建议加载语义：

- `reference.trading_calendar`: 按 `(exchange, trade_date)` upsert
- `reference.instruments`: 按 `instrument_id` upsert

#### Market daily datasets

`market.etf_daily` 与 `market.index_daily` 在 Stage B 应写入 normalized Parquet，而不是直接扩展出 DuckDB 大事实表。

建议规范：

- canonical rows 必须包含 `source_name`、`raw_batch_id`、`ingested_at`、`quality_status`
- 目录按 `year/month` 分区
- 对同一业务键允许通过重跑做“去重后覆盖分区”或“同分区重建写入”，但不允许产生无界重复
- 当请求窗口只覆盖月分区中的部分交易日时，不能只用本次窗口结果直接重建整月分区
- Stage B 的月分区重写必须采用 `read existing partition -> merge current window rows -> dedupe by business key -> rewrite owned partition` 语义，避免 partial backfill 覆盖未请求日期
- 只有在请求窗口完整覆盖目标月分区，或该分区此前不存在时，才允许直接以本次 run 结果重建整月分区

Stage B 推荐收敛为：

- 以“分区级幂等重建”作为初版加载规则
- 同一 run 只写自己负责的分区
- 分区内按业务键去重后输出
- “幂等重建”的精确定义，是对 run 负责的每个目标分区做 deterministic merge-and-rewrite，而不是盲删后直写当前窗口

不要在 Stage B 里同时设计 append-only、merge-on-read、version-preserving 三套机制。对 market daily，先把 deterministic partition rewrite 做稳定。

### 6. Initial Validation Engine

Stage B 必须把 validator 从协议推进到真实 gating 机制。

#### Execution model

- validation 与 normalization 在同一进程内同步执行
- 每个 dataset 使用显式规则列表
- 规则输出统一映射到 `ValidationResultRecord`
- `fail` 会阻断 canonical write 和 watermark advancement
- `warning` 与 `pass_with_caveat` 允许写入，但必须持久化

#### Minimum Stage B rules

`reference.trading_calendar`：

- duplicate key absence
- `trade_date` 非空
- `is_open` 类型正确
- `pretrade_date` 时序合理
- 日历连续性基本检查

`reference.instruments`：

- `instrument_id` 唯一
- `instrument_name` 非空
- `instrument_type` 属于预期集合
- `exchange` 非空

`market.etf_daily` / `market.index_daily`：

- duplicate business key absence
- `trade_date` 必须落在 open trading day
- OHLC 非负且 `close` 非空
- volume / amount 非负
- 极端收益做 warning 级 sanity check

Stage B 不需要实现 cross-source agreement，也不需要通用 rule DSL。规则可以先是显式 Python 实现。

## Proposed Stage B Architecture

### Runtime Boundary

Stage B 后，系统内存在三层并存能力：

1. 旧 `tradepilot/ingestion/` 路径，继续服务当前产品功能
2. 新 `tradepilot/etl/` foundation，已可同步首批 reference / market datasets
3. Stage C 未来要建立的 ETF all-weather serious panel，建立在 Stage B 的真实 ETL path 之上

因此 Stage B 的成功标准不是“旧路径被替换”，而是“新路径已经能稳定 ingest 第一批通用数据集”。

### Minimal Flow In Stage B

Stage B 最小需要跑通的真实链路应是：

1. registry 中存在 `reference.trading_calendar`
2. `ETLService.run_dataset_sync("reference.trading_calendar", request)` 被调用
3. tushare adapter 返回 typed fetch result
4. raw parquet 写入 `data/lakehouse/raw/reference.trading_calendar/...`
5. `etl_raw_batches` 写入 manifest
6. normalizer 生成 canonical calendar rows
7. validator 运行并写入 `etl_validation_results`
8. DuckDB `canonical_trading_calendar` 被 upsert
9. watermark 更新
10. `etl_ingestion_runs.status = success`

只要这条链路稳定，其他 Stage B dataset 才是在同一模式上的扩展，而不是额外发明机制。

## Detailed Module Design

### `tradepilot/etl/models.py`

Stage B 应在现有模型基础上增加真实运行时结果模型，避免 service 内部继续流动无结构 dict。

建议新增：

- `SourceFetchResult`
- `DatasetSyncResult`
- `CanonicalWriteResult`

`DatasetSyncResult` 建议至少包含：

- `run_id`
- `dataset_name`
- `status`
- `raw_batch_ids`
- `validation_counts`
- `records_discovered`
- `records_written`
- `watermark_updated`
- `started_at`
- `finished_at`
- `error_message`

Stage B 不需要引入大型事件模型，只需要把 service 的输入输出从占位 dict 收敛成稳定模型。

### `tradepilot/etl/datasets.py`

Stage B 应把首批 dataset definition 直接固化到代码中，而不是只允许测试里临时注册。

建议新增显式构造函数或常量：

- `build_reference_trading_calendar_dataset()`
- `build_reference_instruments_dataset()`
- `build_market_etf_daily_dataset()`
- `build_market_index_daily_dataset()`

关键建议：

- `reference.trading_calendar` 无增量复杂性，按请求窗口拉取
- `reference.instruments` 默认做 snapshot refresh
- `market.etf_daily` / `market.index_daily` 标记 `supports_incremental=True`
- 明确依赖：
  - `reference.instruments` 依赖为空
  - `reference.trading_calendar` 依赖为空
  - `market.etf_daily` 依赖 `reference.instruments`、`reference.trading_calendar`
  - `market.index_daily` 依赖 `reference.instruments`、`reference.trading_calendar`

依赖在 Stage B 不只是 registry 元数据，还应带有最小执行语义：

- `market.*` dataset 在正式 fetch 前必须执行 dependency preflight
- preflight 至少检查依赖 dataset 是否已存在、是否成功写入 canonical、以及是否覆盖当前请求窗口所需范围
- 若依赖缺失、过期或窗口覆盖不足，service 应自动顺序补跑缺失依赖，而不是立即终止
- 自动补跑过程中产生的问题必须被记录为结构化运行问题，建议写入 `etl_validation_results`，例如 `check_name = "dependency_preflight"`
- 若自动补跑后依赖恢复可用，则主 dataset run 继续执行，并保留该问题记录
- 若自动补跑后依赖仍不可用，则主 dataset run 在 source fetch 前失败退出，不进入下游 raw landing 和 canonical write

### `tradepilot/etl/registry.py`

Stage B 不需要把 registry 升级为插件发现系统，但应解决一个 Stage A 留下的空白：首批 dataset 谁来注册。

建议：

- 提供 `register_stage_b_datasets()` 显式初始化函数
- 在 ETL service 首次执行前确保该函数被调用
- 保持“显式注册”原则，不做仓库扫描

### `tradepilot/etl/storage.py`

Stage B 应在现有 path planner 之上增加两个明确层次：

#### Path planning

- `build_zone_path(...)`
- `build_partition_path(...)`
- `build_raw_batch_path(...)`
- `build_normalized_file_path(...)`

#### IO helpers

- `write_raw_parquet(...)`
- `write_normalized_parquet(...)`

关键要求：

- 写入函数负责目录创建
- 所有写入都返回实际相对路径
- normalized 写入必须支持“同分区重建后替换”

不要在 Stage B 里提前做 dataset discovery、manifest scan 或 compaction。

### `tradepilot/etl/normalizers.py`

Stage B 不应把所有 dataset-specific 逻辑塞进一个巨型 `if dataset_name == ...` 文件。更稳妥的做法是保留同文件内的显式注册，但把实现对象化。

建议新增：

- `TradingCalendarNormalizer`
- `InstrumentNormalizer`
- `MarketDailyNormalizer`

规范要求：

- 输出列名严格 canonical 化
- ETF 和 index daily 共用 `MarketDailyNormalizer`
- `MarketDailyNormalizer` 通过 context 区分 `instrument_type`
- 统一注入 `source_name`、`raw_batch_id`、`ingested_at`、`quality_status`

### `tradepilot/etl/validators.py`

建议引入小型 rule registry，而不是通用 DSL。

建议新增：

- `TradingCalendarValidator`
- `InstrumentValidator`
- `MarketDailyValidator`

Validator 的输入应是 normalized rows，而不是 raw payload。理由是：

- Stage B 的 blocking 语义主要针对 canonical 质量
- reference / market datasets 的关键规则更依赖 canonical schema
- raw payload 已通过 `etl_raw_batches` 保留，后续仍可回放

### `tradepilot/etl/sources/tushare.py`

这是 Stage B 的首个真实 source adapter。

建议实现方式：

- `fetch("reference.trading_calendar", request)` 走 `TushareClient.get_trade_calendar(...)`
- `fetch("reference.instruments", request)` 组合 ETF / index 的 metadata 获取逻辑
- `fetch("market.etf_daily", request)` 拉 ETF 日线
- `fetch("market.index_daily", request)` 拉 index 日线

Stage B 关键收敛：

- 只实现 `tushare` 一个 source
- `fallback_sources` 和 `validation_sources` 先只是 registry 字段，不做真实切换
- 若未来要加 `akshare` 或 `official`，沿同一 typed fetch contract 扩展

### `tradepilot/etl/service.py`

这是 Stage B 最重要的实现文件。

建议职责明确收敛为：

- 解析 dataset definition
- 执行 dependency preflight，并在需要时触发依赖 dataset 自动补跑
- 创建和更新 `etl_ingestion_runs`
- 选择 source adapter
- 管理 raw landing -> normalize -> validate -> load 的顺序
- 成功时推进 watermark
- 暴露最小查询接口：`list_runs()`、`list_validation_results()`

Stage B 不应让 `service.py` 直接变成 scheduler。它是 dataset execution service，不是 job graph engine。

### Write Commit And Recovery Semantics

Stage B 虽然不引入完整 workflow engine，但必须把文件系统与 DuckDB metadata 的最小一致性边界定清楚。

建议规则：

- raw 与 normalized 写入都先落到同目录下的临时路径，再在写入完成后切换到最终路径
- metadata 记录只在对应最终文件已经可见后才更新，避免 metadata 指向不存在文件
- market daily 的分区重写必须以“先准备新分区，再替换旧分区”为准，不允许先删除旧分区再开始生成新分区
- watermark 只能在 canonical write 与 validation result 持久化都成功后推进
- 若 run 在写入过程中异常退出，已提交的 raw batch 允许保留，未完成的 normalized 临时文件必须在下次运行前清理

建议最小恢复语义：

- service 在新 run 开始前，可扫描并清理本 dataset 残留的临时写入路径
- 发现上一次 `status = running` 但已异常中断的 run 时，应将其标记为 `failed`，并在 `error_message` 中注明恢复原因
- 对 `retry_failed` 触发的重跑，允许直接复用既有 raw batch 重新 normalize / validate / load，只要对应 raw 文件仍然存在且 manifest 完整

## DuckDB Schema Refinements

Stage A 只创建了 placeholder schema，Stage B 允许做增量收紧，但必须保持 additive migration。

### `canonical_instruments`

建议在 Stage B 追加最小必要字段：

- `source_name`
- `list_date`
- `delist_date`

原因：

- `list_date` 是 market daily backfill 与有效窗口判断的必要字段
- `source_name` 有助于 reference drift 审计
- `delist_date` 即使暂时经常为空，也应预留

不要在 Stage B 一次性塞入 benchmark、sector、tracking index、费用等大而全字段。

### `source_registry`

Stage B 应至少在初始化或首次运行时写入：

- `tushare`

并标记其 role 为 `primary`。

### Watermark semantics

Stage B 应把 `etl_source_watermarks` 语义收紧为：

- 只有 canonical write 成功后才更新
- `latest_fetched_date` 表示成功进入 canonical 的最大业务日期
- 对 `reference.instruments` 这类 snapshot dataset，可把 `latest_fetched_date` 记为 snapshot date

## Migration Boundary With Existing Code

### What Stage B May Touch

- `tradepilot/db.py`
- `tradepilot/etl/*`
- `tradepilot/data/tushare_client.py`
- `tests/etl/*`

### What Stage B Must Not Change

- 现有 API 返回结构
- 现有 scheduler 行为
- legacy `ingestion_runs` 与 `ingestion/service.py` 的运行语义
- 现有 `tradepilot/etf_all_weather/` 数据路径与模块边界

必须接受以下并存状态：

- 旧路径继续写 legacy 表
- 新路径开始写 `etl_*`、`canonical_*` 和 `data/lakehouse/*`
- 两套路径短期内不做自动同步或统一查询

## Testing Strategy

Stage B 的测试重点不再只是结构契约，而是“第一条真实纵切链路是否稳定”。

建议测试文件：

- `tests/etl/test_tushare_source.py`
- `tests/etl/test_normalizers.py`
- `tests/etl/test_validators.py`
- `tests/etl/test_storage.py`
- `tests/etl/test_service.py`
- `tests/etl/test_db_init.py`

建议测试内容：

### 1. Source Contract Tests

- tushare adapter 对 4 个 dataset 返回统一 `SourceFetchResult`
- 空数据窗口能返回结构正确的空 payload，而不是抛出不透明异常
- source endpoint 和 row_count 元信息可用

### 2. Normalizer Tests

- trading calendar 列名与类型标准化正确
- instrument metadata 能稳定产生 `instrument_id`
- ETF / index daily 共用 normalizer 后输出列集一致
- lineage 字段被正确注入

### 3. Validator Tests

- duplicate key 会产生 `fail`
- 非交易日数据会产生 `fail`
- 极端收益会产生 `warning`
- required field 缺失会被正确分类

### 4. Storage Tests

- raw / normalized 路径生成符合约定
- 相对路径保存稳定
- 分区重写不会残留旧文件
- partial backfill 只更新请求窗口涉及的业务键，不会覆盖同月未请求日期
- 临时路径在异常中断后可被下次 run 清理

### 5. Service Integration Tests

使用临时 DuckDB 和临时 lakehouse root，验证：

1. run record 被创建并正确结束
2. raw parquet 成功写入
3. `etl_raw_batches` 被写入
4. validator 结果被写入
5. reference dataset 成功写入 canonical table
6. market daily 成功写入 normalized Parquet
7. watermark 只在成功场景推进
8. validation fail 时 raw batch 保留、canonical 不写、watermark 不推进
9. 依赖缺失时会先自动补跑依赖，并留下结构化问题记录
10. 依赖自动补跑成功后主 dataset 可继续执行；补跑失败时主 dataset 在 fetch 前终止
11. 异常中断后再次运行可清理临时路径，并将旧 `running` run 标记为 `failed`

### 6. No-Live-Network Default

默认测试不应依赖真实 Tushare 网络。Stage B 测试应使用 fixture DataFrame 或 mock client，确保 CI 稳定。

## Acceptance Criteria

Stage B 完成的验收标准应为：

1. `ETLService.run_dataset_sync()` 可以真实执行至少 4 个 Stage B dataset。
2. `tradepilot/etl/sources/tushare.py` 存在并实现统一 fetch contract。
3. raw batch 会真实写入 `data/lakehouse/raw/` 并记录到 `etl_raw_batches`。
4. `reference.trading_calendar` 可写入 `canonical_trading_calendar`。
5. `reference.instruments` 可写入 `canonical_instruments`。
6. `market.etf_daily` 与 `market.index_daily` 可写入 normalized Parquet。
7. validation result 会写入 `etl_validation_results`，且 `fail` 会阻断 canonical write。
8. watermark 仅在成功 run 后推进。
9. 现有 legacy API、legacy ingestion、legacy scheduler 无回归。

## Implementation Sequence

推荐实现顺序：

1. 在 `models.py` 与 `sources/base.py` 中收紧 typed fetch / sync contract。
2. 在 `storage.py` 中补齐 raw / normalized 写入函数与文件命名规则。
3. 实现 `sources/tushare.py`，优先打通 `reference.trading_calendar`。
4. 实现 `TradingCalendarNormalizer`、`TradingCalendarValidator` 和对应 service path。
5. 实现 `reference.instruments` 的 source / normalize / validate / DuckDB load。
6. 实现共享 `MarketDailyNormalizer` 与 `MarketDailyValidator`。
7. 打通 `market.etf_daily` 与 `market.index_daily` 的 normalized Parquet 写入。
8. 补齐 `list_runs()`、`list_validation_results()` 和 integration tests。

这个顺序能先跑通最小 reference dataset，再扩张到 shared market-daily pattern，风险最低。

## Risks

### 1. Scope Creep Into Stage C

最容易出现的问题，是一边做 Stage B 一边把 macro、rates、rebalance calendar、profile runner 一起塞进来。必须坚持 Stage B 只解决“第一条真实可执行通用链路”。

### 2. Instrument Scope Explosion

如果 `reference.instruments` 在 Stage B 就扩展到全市场 stock、ETF、index、bond、option，会让 source adapter、schema、validation 同时膨胀。应只覆盖 Stage B 真正需要的 ETF / index universe。

### 3. Reintroducing Direct-To-DuckDB Fact Loads

如果为了快而把 `market.etf_daily` 直接写回 DuckDB 大表，会违背总架构中“Parquet 作为大历史事实层”的核心边界。Stage B 必须守住这个边界。

### 4. Validation Semantics Becoming Too Weak

如果 validation 只是日志而不是 structured gating，Stage B 看起来“跑通了”，实际上没有形成可靠的 ingestion contract。必须明确 `fail` 的阻断语义。

## Deferred Decisions

以下问题继续留到 Stage C 或更后续阶段：

### 1. Multi-source fallback 真实切换

当前结论：Stage B 只实现 primary source `tushare`。

### 2. Cross-process execution lock

当前结论：Stage B 不引入真正的 scheduler lock；如需最小保护，可在 service 内做进程内互斥。

### 3. Profile bootstrap runner

当前结论：不做。Stage C 再围绕 serious panel 定义 profile。

### 4. Generic validation DSL

当前结论：不做。Stage B 用显式 Python 规则即可。

### 5. Version-preserving canonical facts

当前结论：Stage B 的 market daily 先使用分区级幂等重建，不引入多版本保留。

### 6. Source-level raw contract validation

当前结论：总设计文档允许 validation 同时覆盖 raw 与 normalized 两层，但 Stage B 先以 canonical validation gating 为主。

是否在 Stage C 或后续阶段补充独立的 source-schema validation 层，留待结合更广的数据源扩展一起决定。

### 7. Instrument snapshot history

当前结论：Stage B 的 `reference.instruments` 先保持当前态 canonical upsert。

是否补充 snapshot 或 SCD 式历史保留，留待结合 Stage C 的 point-in-time replay 需求再决定。

### 8. Raw batch identifier allocation before file naming

当前结论：Stage B 暂不扩大文件命名机制设计，只要求 raw batch 命名与 metadata 关系保持稳定、可追溯。

是否把 `raw_batch_id` 的预分配与文件命名彻底固化为框架约束，留待后续阶段结合 replay / compaction / manifest 演进一起决定。

## Final Judgment

Stage B 的正确产物不是“ETF all-weather 数据已经齐全”，而是“TradePilot 已经拥有一条真实可运行、可落盘、可校验、可追溯的通用 ETL 执行链路”。

只要 `reference.trading_calendar`、`reference.instruments`、`market.etf_daily`、`market.index_daily` 能在 `tradepilot/etl/` 新路径上稳定完成 raw landing、normalize、validate、load、metadata / watermark 更新，Stage B 就算完成。宏观、利率、rebalance calendar 与 serious panel 的扩展，应该留给 Stage C 在这条已稳定的执行链路上继续推进。
