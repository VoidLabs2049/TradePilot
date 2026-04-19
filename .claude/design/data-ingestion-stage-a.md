---
title: "Data Ingestion Stage A"
status: draft
mode: "design"
created: 2026-04-18
updated: 2026-04-19
modules: ["backend"]
---

# Data Ingestion Stage A

## Overview

本文档是 `data-ingestion-architecture.md` 的 Stage A 落地设计，目标是把总设计里的“foundation skeleton”收敛成一个可直接实施的最小增量。

Stage A 不交付首批真实数据集同步能力，也不替换现有 `tradepilot/ingestion/`、`tradepilot/data/` 或 `tradepilot/etf_all_weather/` 现有流程。它只交付新的通用摄取骨架，使后续 Stage B 和 Stage C 可以在统一框架上继续推进。

## Stage A Goals

- [ ] 在仓库内引入新的通用 `tradepilot/etl/` 模块骨架
- [ ] 建立 dataset-oriented 的核心模型和注册表机制
- [ ] 在 `tradepilot/db.py` 中增加 ETL 元数据表和参考表初始化逻辑
- [ ] 明确本地 lakehouse 目录结构与路径约定
- [ ] 为后续 Stage B 数据集实现提供稳定接口边界

## Stage A Non-Goals

Stage A 明确不包含以下内容：

- 任何真实上游数据抓取逻辑
- Trading calendar、instrument、market daily 等首批 dataset 的正式实现
- 通用 validation engine 的完整规则执行
- Scheduler 集成、profile 调度、并发锁实现
- 派生特征、read model、策略快照
- 对现有 API 或现有 ingestion 服务进行行为替换

## Why Stage A Exists

总设计文档已经明确，新架构应采用并行引入、渐进替换的方式。当前仓库里已经存在：

- `tradepilot/data/`：围绕现有 provider 抽象的取数层
- `tradepilot/ingestion/`：现有同步服务
- `tradepilot/etf_all_weather/`：策略域专属模块
- `tradepilot/db.py`：当前 DuckDB 初始化入口

如果直接进入 Stage B，会在“数据集定义、路径约定、元数据、存储边界”尚未稳定的情况下开始堆积具体同步逻辑，后面容易反复返工。Stage A 的职责就是先把这些横切基础设施定型。

## Design Principles

### 1. Additive Only

Stage A 只新增结构，不破坏现有生产路径。已有表、已有 API、已有调度逻辑都保持原样。

### 2. Generic Before Domain-Specific

虽然首个明确业务目标是 ETF all-weather Stage 1，但 Stage A 不能把模块命名、模型边界、路径结构绑死在 ETF 场景上。

### 3. Raw-First Boundary Must Be Visible Early

即使 Stage A 尚未落真实抓取逻辑，也必须先把 `raw / normalized / derived` 三层存储边界定下来，否则后续实现会天然滑回“直接写 DuckDB 表”的旧模式。

### 4. Metadata Is Part of The Product

新的 ingestion 框架不是“能拉到数据就算完成”。运行记录、批次清单、watermark、validation 结果必须从 Stage A 起就有明确 schema 和模型。

### 5. Smallest Correct Skeleton

Stage A 只定义后续实现一定会依赖的抽象，不提前设计复杂但暂时无消费者的机制，例如完整 schema evolution、告警系统、对象存储适配器。

## Scope

### Modules Involved

| Module |
|--------|
| Backend |

### Primary Files

| File | Stage A Role |
|------|--------------|
| `tradepilot/db.py` | 增加 ETL metadata / reference tables 初始化 |
| `tradepilot/config.py` | 增加通用 lakehouse root 路径常量 |
| `tradepilot/etl/__init__.py` | 新模块入口 |
| `tradepilot/etl/models.py` | 核心领域模型 |
| `tradepilot/etl/datasets.py` | dataset 定义结构 |
| `tradepilot/etl/registry.py` | registry 注册与查询 |
| `tradepilot/etl/storage.py` | 路径规划与存储约定 |
| `tradepilot/etl/service.py` | 未来 orchestration service 的占位入口 |
| `tradepilot/etl/normalizers.py` | 标准化协议占位 |
| `tradepilot/etl/validators.py` | 校验协议占位 |
| `tradepilot/etl/sources/base.py` | source adapter 基类 |

### Out of Scope Files For Stage A

- `tradepilot/ingestion/service.py`
- `tradepilot/data/provider.py`
- `tradepilot/api/*`
- `tradepilot/scheduler/*`

这些模块在 Stage A 不需要被重写，只需要保证未来能与 `tradepilot/etl/` 并存。

## Deliverables

Stage A 的交付物应严格收敛为以下 5 类。

### 1. Module Skeleton

新增 `tradepilot/etl/`，形成稳定的包结构。

建议目录：

```text
tradepilot/etl/
├── __init__.py
├── models.py
├── datasets.py
├── registry.py
├── storage.py
├── normalizers.py
├── validators.py
├── service.py
└── sources/
    ├── __init__.py
    └── base.py
```

Stage A 不需要创建 `tushare.py`、`akshare.py`、`official.py` 等具体 source adapter 文件，除非团队希望提前占位；如果创建，也必须保持空实现或协议级骨架，不得引入真实抓取复杂度。

### 2. Core Domain Models

Stage A 应定义后续实现一定会依赖的最小公共模型。

建议模型：

- `DatasetCategory`
- `StorageZone`
- `TriggerMode`
- `RunStatus`
- `ValidationStatus`
- `DatasetDefinition`
- `IngestionRequest`
- `IngestionRunRecord`
- `RawBatchRecord`
- `ValidationResultRecord`
- `SourceWatermarkRecord`

这些模型建议使用 `pydantic v2` 与 `StrEnum`，并满足：

- 所有字段带类型注解
- 公开类和公开函数写 docstring
- 不引入任何策略域特有字段

补充边界说明：

- `tradepilot.etl.models` 是新 ETL foundation 的独立契约
- 它不复用当前 `tradepilot.ingestion.models` 中的 `RunStatus`、`TriggerMode`、`IngestionRun`
- 两套模型在 Stage A 和 Stage B 中并存，直到后续迁移阶段再决定是否统一
- 这种并存是有意设计：新 ETL 路径先独立演进，后续再评估是否替换旧数据获取路径

### 3. Dataset Registry Skeleton

Stage A 应提供一个注册表，使“新增数据集”不再等于“修改引擎内部代码”。

建议能力：

- 注册 dataset definition
- 按 `dataset_name` 查询定义
- 返回全部定义
- 校验 dataset name 唯一性
- 校验必要字段完整性

Stage A 里注册表可以先是进程内静态注册，不需要做数据库持久化。

### 4. Storage Layout Contract

Stage A 必须确定统一存储根目录和 zone 规则。

建议路径：

- `data/lakehouse/raw/`
- `data/lakehouse/normalized/`
- `data/lakehouse/derived/`

对应 `tradepilot/config.py` 建议新增：

- `LAKEHOUSE_ROOT = DATA_ROOT / "lakehouse"`
- `LAKEHOUSE_RAW_ROOT = LAKEHOUSE_ROOT / "raw"`
- `LAKEHOUSE_NORMALIZED_ROOT = LAKEHOUSE_ROOT / "normalized"`
- `LAKEHOUSE_DERIVED_ROOT = LAKEHOUSE_ROOT / "derived"`

Stage A 还应提供一个统一的路径构造器，例如：

- `build_zone_path(dataset_name, zone)`
- `build_partition_path(dataset_name, zone, partition_parts)`

这两个函数在 Stage A 只负责路径规划，不负责真实写入。

`partition_parts` 的精确输入 contract、目录创建副作用边界、文件级命名规则都留到 Stage B 再收紧。Stage A 只需要把 zone 级和目录级边界说明清楚。

### 5. DuckDB Metadata And Reference Schema

Stage A 应在 `tradepilot/db.py` 中增量创建下列表：

- `etl_ingestion_runs`
- `etl_raw_batches`
- `etl_validation_results`
- `etl_source_watermarks`
- `canonical_instruments`
- `canonical_trading_calendar`
- `canonical_rebalance_calendar`
- `canonical_sleeves`
- `source_registry`

这里的 reference tables 只建立 schema，不要求 Stage A 完成数据填充。

## Proposed Architecture For Stage A

### Runtime Boundary

Stage A 后，系统内将并存两套能力：

1. 现有业务同步路径
2. 新的通用 ETL foundation skeleton

新的 `tradepilot/etl/` 在 Stage A 只提供“定义能力”，即：

- 定义数据集是什么
- 定义元数据怎么记
- 定义存储目录怎么规划
- 定义后续 service 的公共输入输出模型

它还不承担“实际把数据拉下来”的职责。

### Minimal Flow In Stage A

Stage A 唯一需要跑通的逻辑链路应是：

1. import `tradepilot.etl`
2. 构建 dataset definition
3. 注册到 registry
4. 调用 storage path planner 获得目标路径
5. 调用 `get_conn()` 后完成新元数据表初始化

只要这条链路稳定，Stage B 就可以开始在该骨架上填首批 dataset。

## Detailed Module Design

### `tradepilot/etl/models.py`

该文件负责定义运行时公共模型，而不是具体业务数据 schema。

建议内容：

- `DatasetCategory`：`reference`、`market`、`rates`、`macro`、`alt`、`derived`
- `StorageZone`：`raw`、`normalized`、`derived`
- `TriggerMode`：`manual`、`scheduled`、`backfill`、`retry_failed`、`validation_only`
- `RunStatus`：`pending`、`running`、`success`、`failed`、`partial_success`
- `ValidationStatus`：`pass`、`pass_with_caveat`、`warning`、`validation_only`、`defer`、`fail`

记录模型建议最小字段如下。

`IngestionRunRecord`:

- `run_id`
- `job_name`
- `dataset_name`
- `source_name`
- `trigger_mode`
- `status`
- `started_at`
- `finished_at`
- `request_start`
- `request_end`
- `records_discovered`
- `records_inserted`
- `records_updated`
- `records_failed`
- `partitions_written`
- `error_message`
- `code_version`

`RawBatchRecord`:

- `raw_batch_id`
- `run_id`
- `dataset_name`
- `source_name`
- `source_endpoint`
- `storage_path`
- `file_format`
- `compression`
- `partition_year`
- `partition_month`
- `window_start`
- `window_end`
- `row_count`
- `content_hash`
- `fetched_at`
- `schema_version`
- `is_fallback_source`

`ValidationResultRecord`:

- `validation_id`
- `run_id`
- `raw_batch_id`
- `dataset_name`
- `check_name`
- `check_level`
- `status`
- `subject_key`
- `metric_value`
- `threshold_value`
- `details_json`
- `created_at`

`SourceWatermarkRecord`:

- `dataset_name`
- `source_name`
- `latest_available_date`
- `latest_fetched_date`
- `latest_successful_run_id`
- `updated_at`

ID 策略建议在 Stage A 直接固定，避免后续 metadata 表之间主键类型分裂：

- `run_id`
- `raw_batch_id`
- `validation_id`

都建议先使用 `BIGINT` 风格 ID，与当前仓库中基于时间戳的运行记录风格保持一致；是否迁移到 UUID，留待后续阶段统一决策。

### `tradepilot/etl/datasets.py`

该文件定义 dataset registry 的配置模型，重点是“一个 dataset 的元信息长什么样”。

建议将 `DatasetDefinition` 拆成“必填字段”和“可选字段带默认值”两层。

必填字段：

- `dataset_name`
- `category`
- `grain`
- `primary_source`
- `storage_zone`

可选字段（但建议保留在模型中，提供默认值或 `None`）：

- `fallback_sources`
- `validation_sources`
- `partition_strategy`
- `canonical_schema_name`
- `validation_rule_names`
- `supports_incremental`
- `watermark_key`
- `timing_semantics`
- `dependencies`

Stage A 这里有两个收敛决策：

1. `canonical_schema` 先只存 schema name 或 schema descriptor，不在 Stage A 里设计完整列级 schema engine。
2. `validation_rules` 先只存 rule name 列表，不在 Stage A 里实现规则执行器。

这样可以把复杂度留给 Stage B，同时不丢失总设计要求的结构边界。

### `tradepilot/etl/registry.py`

建议暴露一个很小的 API 面：

- `register_dataset(definition: DatasetDefinition) -> None`
- `get_dataset(dataset_name: str) -> DatasetDefinition`
- `list_datasets() -> list[DatasetDefinition]`
- `has_dataset(dataset_name: str) -> bool`

实现建议：

- 使用进程内 `dict[str, DatasetDefinition]`
- 在注册时做唯一性校验
- 在模块 import 期间不自动扫描整个仓库
- 不做动态插件加载

这样可以避免 Stage A 过早引入复杂 discovery 机制。

### `tradepilot/etl/storage.py`

该文件负责统一 lakehouse 路径规则，不负责实际 Parquet IO。

建议职责：

- 根据 dataset 和 partition 构造目录
- 为后续 raw batch manifest 提供标准路径格式

建议路径规则：

- daily datasets: `zone/<dataset_name>/year=YYYY/month=MM/`
- monthly datasets: `zone/<dataset_name>/year=YYYY/`
- long-form slow fields: `zone/<dataset_name>/field_name=<field>/year=YYYY/`

Stage A 不应在这里提前实现 compaction、manifest 扫描、schema merge。

### `tradepilot/etl/normalizers.py`

Stage A 只需要定义协议，不需要实现具体 normalizer。

建议：

- `BaseNormalizer` 抽象类或协议
- 统一方法签名，例如 `normalize(raw_payload, context)`
- 输出约束说明：返回 canonical rows 与 lineage metadata

### `tradepilot/etl/validators.py`

Stage A 同样只定义协议。

建议：

- `BaseValidator` 抽象类或协议
- `ValidationRuleDefinition` 轻量模型
- 统一结果输出类型指向 `ValidationResultRecord`

### `tradepilot/etl/service.py`

Stage A 的 service 不是正式 orchestrator，只是后续入口占位。

建议暴露占位级方法签名，内部可以先抛出 `NotImplementedError` 或返回清晰占位结果：

- `run_dataset_sync(dataset_name, request)`

如果团队希望提前占位，也可以保留更宽的接口，例如：

- `run_multi_dataset_sync(dataset_names, request)`
- `run_bootstrap(profile_name)`
- `list_runs(dataset_name=None)`
- `list_validation_results(dataset_name=None, run_id=None)`

这些接口在 Stage A 仍然只是规划占位，不代表 orchestrator API 已经定稿。精确 service contract 留到 Stage B 再根据首批 dataset 的真实消费者收敛。

### `tradepilot/etl/sources/base.py`

该文件定义通用 source adapter 基类。

建议能力：

- `source_name`
- `source_role`
- `supports_dataset(dataset_name)`
- `fetch(dataset_name, request)`

Stage A 不必定义每种 dataset family 的细粒度方法，避免过早锁死 adapter API。更稳妥的做法是先定义一个通用 `fetch()` 入口，Stage B 再按需要演进。

## DuckDB Schema Design

### Metadata Tables

#### `etl_ingestion_runs`

角色：一条 ingestion 任务执行记录。

关键要求：

- 允许按 `run_id` 唯一定位
- 允许按 `dataset_name` 和 `started_at` 查询历史
- `status` 必须是框架级状态，不要混入上游 source 私有语义

#### `etl_raw_batches`

角色：raw immutable batch 的 manifest。

关键要求：

- `storage_path` 必须保存实际落地路径
- `content_hash` 用于识别重复批次或后续审计
- `run_id` 与 `dataset_name` 必须能回连任务执行记录

#### `etl_validation_results`

角色：结构化校验结果。

关键要求：

- 结果必须能关联 `run_id`
- 可以选择关联 `raw_batch_id`
- `details_json` 允许保存规则自定义细节，但主状态字段必须结构化

#### `etl_source_watermarks`

角色：记录增量同步边界。

关键要求：

- 主键建议 `(dataset_name, source_name)`
- 只记录“已成功推进”的边界，不记录失败尝试

### Reference Tables

这些表在 Stage A 只创建 schema：

- `canonical_instruments`
- `canonical_trading_calendar`
- `canonical_rebalance_calendar`
- `canonical_sleeves`
- `source_registry`

补充迁移语义：

- 当前仓库中的 `trading_calendar` 仍视为 legacy app table
- `canonical_trading_calendar` 是新 ETL foundation 的 reference table
- Stage A 和 Stage B 不做两者自动同步，也不替换现有读取方

`source_registry` 的建议最小字段：

- `source_name`
- `source_type`
- `source_role`
- `is_active`
- `base_note`
- `updated_at`

这样后续可以显式表达 primary / fallback / validation source 角色。

另外建议在 schema 层保证 `source_name` 唯一，避免把同一个 source 因 role 不同拆成多条含混记录。

## Config Changes

Stage A 建议在 `tradepilot/config.py` 增加通用路径常量，但不要删除现有 `ETF_AW_DATA_ROOT`。原因是当前仓库已存在 `tradepilot/etf_all_weather/` 模块，直接替换其路径约定会引入不必要风险。

建议策略：

- 保留 `ETF_AW_DATA_ROOT`
- 新增通用 `LAKEHOUSE_*` 路径常量
- 未来新的 ETL datasets 统一使用 `LAKEHOUSE_*`
- 现有 ETF 专属模块是否迁移，放到后续阶段决策
- 当前目标不是兼容旧路径，而是先让新路径作为独立数据体系落地

## Migration Boundary With Existing Code

### What Stage A May Touch

- `tradepilot/config.py`
- `tradepilot/db.py`
- 新增 `tradepilot/etl/` 目录
- 对应测试文件

### What Stage A Must Not Change

- 现有 provider 具体实现行为
- 现有 API 响应结构
- 现有 scheduler job 执行方式
- 现有 `tradepilot/etf_all_weather/` 业务逻辑
- 现有 `tradepilot/ingestion.models` 与 `ingestion_runs` 表的行为语义

这条边界很重要，因为 Stage A 的成功标准不是“功能替换”，而是“新骨架已可并存并可被后续阶段消费”。

特别地，Stage A 应明确接受以下并存状态：

- 旧路径继续写 `ingestion_runs`、`trading_calendar` 等 legacy 表
- 新路径未来写 `etl_*` 和 `canonical_*` 表
- 两套路径在迁移完成前不要求共用同一套运行枚举与 service 接口
- 新路径的数据获取和存储边界先独立演进，不要求复用旧 provider 或旧同步实现

## Testing Strategy

Stage A 需要的测试集中在结构与契约层，而不是数据正确性层。

建议测试文件：

- `tests/etl/test_models.py`
- `tests/etl/test_registry.py`
- `tests/etl/test_storage.py`
- `tests/etl/test_db_init.py`

建议测试内容：

### 1. Model Tests

- enum 值稳定
- pydantic model 必填字段检查正确
- 默认值与序列化行为稳定

### 2. Registry Tests

- 可注册合法 dataset
- 重复 `dataset_name` 会报错
- 查询不存在 dataset 会报错

### 3. Storage Tests

- zone 根目录和目录层级约定稳定
- 路径规划函数能返回符合约定的路径
- `dataset_name` 中的点号保持为目录名一部分或有明确转换规则

这里建议直接保留点号，例如 `market.etf_daily/`，避免 Stage A 过早引入额外命名映射。

### 4. DB Initialization Tests

- `get_conn()` 后新表存在
- 原有表仍存在
- 多次初始化幂等
- legacy `ingestion_runs`、`trading_calendar` 等既有表仍可读写

## Acceptance Criteria

Stage A 完成的验收标准应为：

1. 仓库中存在可 import 的 `tradepilot.etl` 模块骨架。
2. 可定义并注册一个最小 `DatasetDefinition`。
3. `tradepilot/config.py` 提供通用 lakehouse 路径常量。
4. `tradepilot/db.py` 初始化后包含新的 ETL metadata / reference tables。
5. 新增测试覆盖 registry、storage、db init 三类基础契约。
6. 现有 API、现有 ingestion、现有 scheduler 行为不发生回归。

## Implementation Sequence

推荐实现顺序：

1. 在 `tradepilot/config.py` 增加 `LAKEHOUSE_*` 路径常量。
2. 新增 `tradepilot/etl/models.py` 与 `tradepilot/etl/datasets.py`。
3. 新增 `tradepilot/etl/registry.py` 与 `tradepilot/etl/storage.py`。
4. 新增 `tradepilot/etl/service.py`、`normalizers.py`、`validators.py`、`sources/base.py` 占位接口。
5. 在 `tradepilot/db.py` 增加 metadata / reference tables 初始化。
6. 补齐 `tests/etl/` 基础测试。

这个顺序能先稳定纯 Python 契约层，再扩展数据库初始化，最后用测试封口。

## Risks

### 1. Stage A Scope Creep

最容易出现的问题是把 Stage B 的 dataset 实现提前塞进来，导致 Stage A 失焦。必须坚持“只做骨架，不做首批同步逻辑”。

### 2. Overdesign

如果在 Stage A 里就设计插件发现、schema evolution、对象存储后端、复杂 profile runner，会显著拖慢交付，而且这些能力当前没有直接消费者。

### 3. Naming Lock-In

`tradepilot/etl/`、metadata 表名、lakehouse 路径一旦进入实现，后面迁移成本会上升，因此 Stage A 文档必须明确命名边界。

### 4. Hidden Conflict With Existing ETF Module

仓库中已有 `tradepilot/etf_all_weather/` 与 `data/etf_all_weather/` 路径约定。Stage A 必须并存，不应隐式重定向已有 ETF 模块的数据路径。

## Deferred Decisions

以下问题故意留到后续阶段：

### 1. Dataset registry 是否自动扫描模块

当前结论：不做。Stage A 先采用显式注册。

### 2. `service.py` 是否直接写数据库

当前结论：不做完整实现，只保留占位 contract，具体 API 面留待 Stage B。

### 3. Raw batch 文件名规范是否包含 hash 和时间戳

当前结论：只定义目录级 contract，文件级命名放到 Stage B 决定。

### 4. Canonical schema 是否使用统一 schema descriptor

当前结论：Stage A 只保留 schema name 或 descriptor 占位。

### 5. Storage path helper 的精确输入输出 contract

当前结论：Stage A 只定义 zone 和目录层级边界，精确 partition 输入形态与副作用边界放到 Stage B 决定。

## Final Judgment

Stage A 的正确产物不是“已经能同步数据”，而是“已经有一套明确、最小、可测试、可扩展的 ETL foundation skeleton”。

只要 `tradepilot/etl/`、DuckDB metadata schema、lakehouse 路径契约、dataset registry 和基础测试全部落下，Stage A 就算完成。首批 dataset 的真正价值实现，应该留给 Stage B 在这个骨架上推进。
