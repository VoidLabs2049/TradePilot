---
title: "TradePilot Workflow Data Platform Refactor"
status: in-review
mode: refactoring
created: 2026-04-06
updated: 2026-04-06
modules:
  - workflow
  - scheduler
  - summary
  - briefing
  - ingestion
  - webapp-dashboard
  - the-one-integration
---

# TradePilot Workflow Data Platform Refactor

## Overview

TradePilot 已经完成了第一轮 workflow-first 重构，但当前系统仍然带有明显的过渡态特征：

- 产品层仍残留大量旧时代并行能力：`analysis`、`signal`、`trade_plan`、`summary/daily`、`summary/5m`、`SectorMap`、`StockAnalysis`、`TradePlan`
- 数据层虽然已经开始围绕 pre/post workflow 组织，但 briefing 的大量关键数据仍主要保存在 `workflow_runs.summary_json` 中
- workflow 主路径虽然已经部分摆脱 `DailyScanner.run()`，但观察配置、市场 reference 配置、新闻链路、历史 reference block、价格口径与交易日口径都还没有完全定型

本轮设计需要进一步明确一个更稳定、更长期可扩展的方向：

> **TradePilot 的最终目标，不是成为另一个负责最终主观结论输出的“大脑”，而是成为面向 daily workflow 的结构化数据底座。**

它应该负责：
- 数据采集入库
- 数据清洗整合
- workflow-ready structured context 生成
- workflow snapshot / reference persistence
- 对外提供快速查询服务
- Web 展示 structured context 与上层 insight 结果

而 The-One 的职责则是：
- 消费 TradePilot 提供的 structured context
- 做进一步分析、归纳与认知增强
- 生成最终盘前/盘后 insight JSON
- 回写给 TradePilot，由 TradePilot 统一查询与展示

因此，TradePilot 和 The-One 的关系不是“谁替代谁”，而是：

- **TradePilot = workflow data platform / context producer**
- **The-One = workflow insight engine / context consumer**

## Final Product Goal

TradePilot 的最终目标是成为：

## Daily Workflow Data Platform

它面向主观波段交易，长期稳定承担四类职责：

### 1. 数据采集入库
把 daily workflow 真正需要的数据稳定采集并持久化：
- 交易日历
- 指数 / 个股 / 板块 / 市场统计
- 新闻
- 持仓 / watch config / alerts
- 风格代理 / ETF 风险代理
- 盘后复盘 reference blocks
- 盘前准备所需 carry-over blocks

### 2. 数据清洗整合
把原始数据整理成 workflow 可以直接消费的 structured context：
- market context
- sector context
- watch context
- position context
- overnight news context
- carry-over context
- market reference context

### 3. 对外提供服务
向外部系统稳定暴露 structured workflow data：
- 给 The-One 提供 pre/post workflow context
- 给 Web 提供 briefing-ready / context-ready 数据
- 给 scheduler、agent、其他调用方提供统一入口

### 4. Web 展示
TradePilot 的 Web 不再自己拼装复杂分析逻辑，而是直接展示：
- TradePilot 的 structured context
- The-One 的最终 insight JSON
- workflow history / snapshots / replay

## Product Positioning

参考 `/Users/lhh/Github/The-One/.claude/skills/the-one/skills/daily-workflow` 与 `/Users/lhh/Github/The-One/briefings`，并结合当前讨论，TradePilot 应长期围绕以下产品定位收口：

- 第一优先服务对象：**主观波段**
- 盘后核心输出：**市场大势、板块定位、持仓健康度、明日准备**
- 盘前核心输出：**昨日复盘摘要、隔夜信息、今日关注清单、操作计划**
- 操作计划粒度：**只给观察框架，不直接给买卖动作**
- 板块组织方式：**当日主线 + 固定观察池**
- 第二阶段盘中观察：**以盘前判断验证为主、持仓监控为先、热点跟踪为辅**

## Why TradePilot Exists Separately from The-One

TradePilot 相比 The-One 的核心优势，不应该定义为“更聪明”，而应该定义为：

### 1. 更强的持久化能力
TradePilot 应成为长期运行的数据底座，提供：
- DuckDB 持久化
- workflow history
- scheduler history
- ingestion history
- structured snapshots
- replay 能力

### 2. 更强的结构化服务能力
TradePilot 应长期提供：
- 稳定 API
- 快速查询
- 可回放 structured context
- 多调用方复用能力

它不是只生成一篇报告，而是提供整套 workflow structured service。

### 3. 更强的数据清洗与标准化能力
TradePilot 应统一沉淀：
- 交易日口径
- 价格口径 / 复权口径
- 板块映射口径
- 指数 / ETF reference 口径
- watch config 口径
- alert 口径
- carry-over 口径

### 4. 更适合作为上游基础设施
The-One 适合做推理、归纳与结论增强；TradePilot 适合长期承担：
- 采集
- 存储
- 清洗
- 服务
- 展示

## Goals

- [ ] 把 `workflow` 的产物从 `scan-driven payload` 升级为 `workflow context + insight-ready payload`
- [ ] 让盘后 workflow 输出真正稳定的 post structured context
- [ ] 让盘前 workflow 承接上一交易日 post reference，而不是重新拼装重分析
- [ ] 让 Dashboard 只围绕 structured context + final insight 渲染
- [ ] 下线不属于 workflow data platform 范围的旧页面与旧 API
- [ ] 去掉 workflow 主路径中固定 `2024-01-01 ~ 2025-12-31` 的历史窗口依赖
- [ ] 明确 TradePilot ↔ The-One 的交互协议
- [ ] 为第二阶段 intraday watch 预留清晰边界，但不在本阶段做成独立重产品

## Constraints

- 维持现有 `workflow` API 与 scheduler 的基础入口，不从产品入口层推翻重来
- 第一阶段优先改造主路径，不追求一次性删除全部旧代码
- `portfolio` 仍然是 position 输入源，短期不宜破坏其数据读写能力
- `summary/watchlist` 当前仍可作为过渡输入，但长期要升级为 richer watch config
- 不引入回测系统，不让长期历史研究成为主路径依赖
- UI 与后端语义必须一致：TradePilot 是 workflow data platform，不是 legacy analysis shell
- The-One 不直接读取底层表，不直接管理 TradePilot 的内部 schema

## Scope

### Modules Involved

| Module | Current Role | Planned Changes |
|--------|-------------|-----------------|
| `tradepilot/workflow` | pre/post workflow orchestration | 成为 structured workflow context 的核心编排层 |
| `tradepilot/scheduler` | 定时触发 workflow job | 保持入口不变，继续服务 pre/post workflow |
| `tradepilot/ingestion` | 行情/新闻同步 | 保留为数据接入层，不主导 UI 语义 |
| `tradepilot/summary` | watchlist、trading-status、旧 summary | 保留过渡输入能力，逐步向 context service 靠拢 |
| `tradepilot/scanner` | 历史扫描引擎 | 退出 workflow 主路径，保留少量兼容读取能力 |
| `tradepilot/webapp` | 当前 workflow-first UI | 改为 context + insight 双层展示 |
| `the-one-integration` | 当前尚未正式建模 | 增加 context/output contract 与 insight write-back |

### Key Files

| File | Role | Impact |
|------|------|--------|
| `tradepilot/workflow/models.py` | Workflow summary/schema | 从 briefing-first 进一步演进为 context + insight aware schema |
| `tradepilot/workflow/service.py` | pre/post workflow orchestration | 重写主逻辑，稳定输出 structured context |
| `tradepilot/db.py` | DuckDB schema and tables | 增加 insight/result storage 与必要的 reference/snapshot 表 |
| `tradepilot/api/workflow.py` | Workflow API surface | 提供 context 查询、history 与 insight write-back/read API |
| `tradepilot/main.py` | Router registration | 持续收口 legacy surface |
| `tradepilot/ingestion/service.py` | Sync market/news | 继续作为 structured context 的上游输入层 |
| `tradepilot/api/summary.py` | Watchlist and summary routes | 过渡期保留 watchlist/trading-status，后续对齐 richer watch config |
| `tradepilot/scanner/daily.py` | Old daily scanner | 逐步退出 workflow 主路径 |
| `tradepilot/scheduler/jobs.py` | Workflow jobs/history | 保持 workflow 触发入口与失败记录 |
| `webapp/src/pages/Dashboard/index.tsx` | Main Daily Workflow UI | 改为 context / insight / history 三层呈现 |
| `webapp/src/App.tsx` | Routes and navigation | 保持 workflow-first shell |
| `webapp/src/services/api.ts` | Frontend API layer | 增加 insight read/write state 获取接口 |

### Out of Scope

- 全市场重分析与回测体系
- 多年期因子研究与回测平台
- 自动买卖决策引擎
- 复杂 trade plan 生命周期管理
- 让 TradePilot 替代 The-One 做最终主观推理
- 本阶段完整实现 intraday watch

## Target Architecture

## Layer 1: Fact Layer
底层事实表与原始数据：
- `stock_daily`
- `index_daily`
- `market_daily_stats`
- `sector_data`
- `news_items`
- `portfolio`
- `alerts`
- `trading_calendar`
- 其他资金/估值/reference 表

## Layer 2: Structured Workflow Context Layer
由 TradePilot 生成并持久化，供 Web 和 The-One 消费：
- post-market structured context
- pre-market structured context
- carry-over context
- watch context
- market reference context
- overnight news context
- position context

## Layer 3: Final Insight Layer
由 The-One 生成并回写，供 Web 优先展示：
- post insight JSON
- pre insight JSON
- 后续 intraday insight JSON

## Layer 4: Presentation Layer
由 TradePilot Web 统一展示：
- 默认展示 The-One insight
- 可展开查看 TradePilot context
- 若无 insight，回退展示 context 并提示触发

## TradePilot ↔ The-One Interaction Model

这是本次设计中最关键的边界定义。

### Role Split

#### TradePilot
负责：
- 数据采集
- 数据入库
- 数据清洗整合
- workflow structured context 生产
- workflow snapshot / reference persistence
- context / insight API
- Web 展示

不负责：
- 最终长篇主观归纳
- 高度灵活推理链条
- 直接输出最终策略判断作为唯一结果

#### The-One
负责：
- 读取 TradePilot 提供的 structured context
- 进一步分析、归纳、认知增强
- 输出最终盘前/盘后 insight JSON
- 回写到 TradePilot

不负责：
- 直接操作 TradePilot 底层数据库
- 重复做数据抓取与底层清洗
- 管理 Web 展示逻辑

### Interaction Mode

采用：

> **Pull for context + Write-back for insight**

即：
- The-One 主动从 TradePilot 拉取 structured context
- The-One 生成 insight JSON 后，通过 TradePilot API 回写
- TradePilot 负责持久化、查询与展示

### Why This Mode

相比“TradePilot 直接调用 The-One”或“The-One 直接读数据库”，该方案的好处是：
- 解耦
- 口径统一
- 易调试
- 易回放
- 易扩展到其他上层分析器
- 更符合 TradePilot 作为 data platform 的定位

## Interaction Contract v1

### 1. TradePilot -> The-One
TradePilot 输出的是：

## Structured Workflow Context

而不是给 Web 专用的展示 payload，也不是零散底层表。

第一阶段直接通过现有 workflow API 提供，The-One 不直接读取底层表。

#### Context Read API

- `GET /api/workflow/context/latest?phase=pre_market`
- `GET /api/workflow/context/latest?phase=post_market`

其中：
- `phase` 为必填 query parameter
- 合法值固定为 `pre_market` / `post_market`
- 返回值为 `WorkflowContextPayload | null`

当前 contract 对齐 `tradepilot/api/workflow.py` 与 `tradepilot/workflow/models.py`，返回结构至少包含：

- `schema_version`
- `producer`
- `producer_version`
- `generated_at`
- `workflow_run_id`
- `workflow_date`
- `phase`
- `context`
- `metadata`

其中：
- `workflow_run_id` 是本次 structured context 的唯一上游 run 标识
- The-One 在回写 insight 时，必须把这个值作为 `source_run_id` 回传
- `metadata` 用于提供标题、overview、日期解析、执行状态、step 数等补充信息

建议分 phase 输出：

#### Post-market Context
- `workflow_date`
- `phase`
- `market_context`
- `sector_context`
- `position_context`
- `alerts_context`
- `reference_context`
- `metadata`

#### Pre-market Context
- `workflow_date`
- `phase`
- `carry_over_context`
- `overnight_news_context`
- `watch_context`
- `market_reference_context`
- `action_frame_inputs`
- `metadata`

当前实现中，`context` 的 phase 语义进一步收敛为：

#### Current Post-market Context Keys
- `market_overview`
- `sector_positioning`
- `position_health`
- `next_day_prep`
- `watch_context`
- `alerts`

#### Current Pre-market Context Keys
- `yesterday_recap`
- `overnight_news`
- `today_watchlist`
- `action_frame`
- `watch_context`
- `alerts`
- `carry_over`

### 2. The-One -> TradePilot
The-One 输出的是：

## Structured Insight JSON

而不是 markdown-only 的最终结果。

第一阶段直接通过现有 write-back API 回写：

#### Insight Write-back API

- `PUT /api/workflow/insight`

请求体为 `WorkflowInsightUpsertRequest`，至少包含：

- `workflow_date`
- `phase`
- `producer`（默认应为 `the_one`）
- `status`
- `schema_version`
- `producer_version`
- `generated_at`
- `source_run_id`
- `source_context_schema_version`
- `insight`
- `error_message`（失败时可选）

其中：
- `source_run_id` 必须对应上一步 context read 返回的 `workflow_run_id`
- `source_context_schema_version` 当前应对齐 `workflow-context.v1`
- TradePilot 以 `(workflow_date, phase, producer)` 作为 upsert 语义的自然唯一键

完成回写后，可通过以下接口读取最新状态：

- `GET /api/workflow/insight/latest?phase=pre_market&producer=the_one`
- `GET /api/workflow/insight/latest?phase=post_market&producer=the_one`

该接口用于判断：
- 当前 insight 是否已存在
- `state` 是否为 `completed` / `failed` / `pending` / `stale`
- `is_stale` 是否为 `true`
- `latest_run_id` 是否已经落后于 The-One 回写时使用的 `source_run_id`

建议包含：
- `phase`
- `workflow_date`
- `generated_at`
- `summary`
- `sections`
- `metadata`

例如：
- `market_view`
- `theme_view`
- `position_view`
- `tomorrow_view`
- `action_frame`
- `risk_notes`
- `source_run_id`
- `model`
- `version`

markdown 如有需要，可以作为附加渲染产物，而不是主产物。

### 3. Recommended Integration Sequence

The-One 与 TradePilot 的建议交互顺序为：

1. 调用 `GET /api/workflow/context/latest?phase=...` 拉取对应 phase 的最新 structured context
2. 读取返回中的 `workflow_run_id` / `workflow_date` / `schema_version`
3. 基于该 context 生成 The-One insight JSON
4. 调用 `PUT /api/workflow/insight` 回写 insight，并显式传入 `source_run_id=workflow_run_id`
5. 调用 `GET /api/workflow/insight/latest?phase=...&producer=the_one` 校验状态与 freshness

该流程明确保证：
- The-One 始终消费 API contract，而不是内部表结构
- TradePilot 始终以 workflow run 为 context / insight 关联基准
- stale 检测可以基于 `source_run_id` 与最新 workflow run 自动完成

## Storage Model

### Current State
当前数据库已包含：

#### Fact Tables
- `stock_daily`
- `index_daily`
- `etf_flow`
- `margin_data`
- `northbound_flow`
- `stock_valuation`
- `sector_data`
- `stock_weekly`
- `stock_monthly`
- `sector_stocks`
- `stock_sector_map`
- `news_items`
- `video_content`
- `trading_calendar`
- `market_daily_stats`

#### Workflow / Support Tables
- `ingestion_runs`
- `daily_scan_results`
- `alerts`
- `scheduler_history`
- `workflow_runs`
- `portfolio`
- `trades`
- `signals`
- `trade_plan`

### Current Problem
当前 workflow 关键产物大多仍保存在：
- `workflow_runs.summary_json`

虽然“有持久化”，但粒度偏粗，不利于：
- post -> pre 精细 reference 复用
- insight 回写后的字段级关联
- intraday 复用
- 单块数据调试与回放

### Target Storage Direction

建议长期形成三层持久化：

#### 1. Fact Persistence
保留现有底层事实表

#### 2. Context Persistence
通过 `workflow_runs` 继续保存完整 summary/context snapshot，并逐步评估是否拆出：
- workflow reference snapshots
- workflow watch reviews
- workflow market references
- workflow news mappings

#### 3. Insight Persistence
新增独立的 insight result storage，例如：
- `workflow_insights`
  - id
  - workflow_run_id
  - workflow_date
  - phase
  - producer (`the_one`)
  - status
  - insight_json
  - generated_at
  - error_message
  - version

## Canonical Domain Model

为避免实现时继续围绕临时 payload 和页面字段演化，系统需要先明确最小领域对象。

### Core Domain Objects

#### `WorkflowRun`
表示一次 workflow 执行过程，关注执行元数据而不是业务内容。

关键属性：
- `id`
- `workflow_date`
- `phase`
- `triggered_by`
- `execution_status`
- `started_at`
- `finished_at`
- `error_message`

#### `WorkflowContext`
表示某次 workflow 产出的机器可消费 structured context，是 TradePilot 对 The-One 的正式交付对象。

关键属性：
- `workflow_run_id`
- `workflow_date`
- `phase`
- `context_json`
- `schema_version`
- `producer_version`
- `generated_at`

#### `WorkflowInsight`
表示上层分析器（当前为 The-One）基于某个 context 生成的最终 insight 结果。

关键属性：
- `workflow_run_id`
- `workflow_date`
- `phase`
- `producer`
- `insight_status`
- `insight_json`
- `schema_version`
- `producer_version`
- `generated_at`
- `error_message`

#### `WatchConfig`
表示长期稳定的观察对象与持仓语义配置，而不是临时 UI 输入。

#### `MarketReferenceConfig`
表示核心指数、风格代理、ETF 风险代理与名称映射的统一配置源。

#### `TradingDayReference`
表示交易日历、前后交易日关系、phase fallback 规则与新闻窗口切分规则。

#### `PricePolicy`
表示 workflow 使用的统一价格口径、复权方式与相关因子策略。

### Architectural Principle

后续实现必须遵守：
- `WorkflowRun` 负责执行过程元数据
- `WorkflowContext` 负责事实整理后的 structured context
- `WorkflowInsight` 负责最终认知结果
- Web 展示层不直接替代上述 domain model

## Context / Insight Schema Versioning

TradePilot ↔ The-One 的交互协议必须显式版本化，避免 schema 演进时出现静默不兼容。

### Required Fields

所有 context payload 与 insight payload 都必须包含：
- `schema_version`
- `producer`
- `producer_version`
- `generated_at`

例如：
- context: `schema_version = "workflow-context.v1"`
- insight: `schema_version = "workflow-insight.v1"`

### Versioning Rules

- schema 的 breaking change 必须升级主版本
- 非 breaking 的字段扩展可以在同一主版本内追加可选字段
- Web 与 The-One 都必须按 `schema_version` 解析，而不能依赖隐式字段假设
- payload 内的 `producer_version` 用于回溯具体生成逻辑版本

### Compatibility Rule

在过渡期：
- `workflow_runs.summary_json` 可以继续承载旧结构
- 但新的 context / insight API 必须只承诺新 schema
- 旧字段兼容只在内部保留，不应成为对外 contract 的一部分

## Idempotency and Freshness Rules

为了支持重复触发、失败重试与历史回放，context 和 insight 的写入与展示必须有明确规则。

### Idempotency

#### Context
- 同一 `(workflow_date, phase, trigger source)` 可以产生多个 run
- 但“latest official context” 应明确指向最近一次成功或部分成功的 run

#### Insight
- 同一 `(workflow_date, phase, producer)` 的 insight write-back 应至少支持 upsert
- 若保留历史版本，应额外有 revision/version 字段
- 如果不保留 revision，则后写覆盖前写，但必须保留写入时间与来源版本

### Provenance

每条 insight 必须明确指向其来源 context，至少包含：
- `source_run_id`
- `source_context_schema_version`
- 可选：`source_context_hash`

这样才能判断 insight 是否基于当前最新 context 生成。

### Freshness

Web 展示时必须判断 insight 是否过期：
- 若 `insight.source_run_id != latest_context.run_id`，则 insight 视为 `stale`
- stale insight 可以展示，但必须明确标识“基于旧 context”
- 用户可选择重新触发 The-One 分析

### Recommended Insight States

- `not_requested`
- `pending`
- `completed`
- `failed`
- `stale`

## Failure Semantics

workflow data platform 必须显式定义失败语义，避免前端、scheduler 与上层分析器对状态理解不一致。

### Execution Failure

#### Context generation failed
- `WorkflowRun.execution_status = failed`
- 无有效 context
- Web 提示先重跑 TradePilot workflow
- 不允许触发新的 insight 生成

#### Context generation partial
- `WorkflowRun.execution_status = partial`
- 允许保存 context，但必须在 metadata 中标注缺失数据源
- 允许触发 insight，但 The-One 应看到缺失说明

### Insight Failure

#### Insight generation failed
- `WorkflowInsight.insight_status = failed`
- 保留 context 展示
- Web 显示“分析失败，可重试”
- 不覆盖已有成功 insight，除非用户明确要求替换

#### Insight stale
- context 更新但 insight 未更新
- 可继续展示旧 insight，但必须标记为 stale
- 默认提示重新触发

### Fallback Display Rules

- 优先显示 `completed and fresh` 的 insight
- 若 insight 不存在或失败，则回退显示 TradePilot context
- 若只有 stale insight，可显示但需高亮提示“当前 context 已更新”

## Transition Plan from `summary_json` to Dedicated Context/Insight Storage

当前 `workflow_runs.summary_json` 是过渡承载方式，不应视为长期最终模型。

### Transitional Principle

- 短期：继续使用 `workflow_runs.summary_json` 保存完整 workflow snapshot
- 中期：增加独立的 `workflow_insights` 表
- 中长期：根据复用与调试需要，逐步引入更明确的 `workflow_contexts` 或 specialized reference tables

### Stage 1

保留：
- `workflow_runs.summary_json`

新增：
- `workflow_insights`

目标：
- 先完成 The-One write-back 闭环
- 不在这一阶段一次性拆散所有 context block

### Stage 2

视需要引入：
- `workflow_contexts`
- `workflow_reference_snapshots`
- `workflow_news_mappings`
- `workflow_watch_reviews`

目标：
- 提升 post -> pre reference block 复用能力
- 提升单块数据调试/回放能力
- 支持后续 intraday phase

### Stage 3

逐步将 `summary_json` 从“唯一真实来源”降级为：
- convenience snapshot
- UI aggregation cache
- backward compatibility artifact

长期应避免让 `summary_json` 同时承担：
- execution metadata
- machine-readable context
- final insight
- UI presentation payload

## Web Display Strategy

### Display Priority

#### Case 1: 有 The-One insight
- 默认展示 The-One 最终 insight
- 可展开查看 TradePilot structured context
- 可查看 workflow run metadata / generated time / version

#### Case 2: 无 The-One insight，但有 TradePilot context
- 展示 TradePilot structured context
- 顶部提示：尚未生成 The-One 分析结果
- 提供触发入口

#### Case 3: 两者都没有
- 提示先触发 TradePilot workflow
- context 生成后，再可触发 The-One

### Why This Strategy
这样可以确保：
- TradePilot 自己始终可用
- The-One 是增强层，而不是单点依赖
- 页面不会因为 insight 缺失而完全空白

## State Model

建议将 workflow 的 context 与 insight 分开管理状态。

### Context State
- `not_generated`
- `generated`
- `failed`

### Insight State
- `not_requested`
- `pending`
- `completed`
- `failed`

这样 Web 能清楚展示：
- TradePilot 数据是否已经准备好
- The-One 分析是否已经完成
- 是否需要重新触发
- 是没跑、跑中、还是失败

## Data Model Mapping

## Workflow Context Blocks

### Post-market
- `market_overview`
  - 指数、breadth、limit stats、style、ETF 风险代理、市场总结
- `sector_positioning`
  - 当日主线、当日弱线、固定观察池板块、观察重点
- `position_health`
  - 持仓板块健康度、关注个股/持仓状态、风险标记
- `next_day_prep`
  - 明日偏向、重点方向、风险提示、明日 checkpoints
- `alerts`
- `watch_context`
- `metadata`

### Pre-market
- `yesterday_recap`
  - 由上一条 post context 精简而来
- `overnight_news`
  - 新闻亮点、分类、可选板块映射
- `today_watchlist`
  - 市场 checkpoints、重点板块、持仓观察
- `action_frame`
  - posture、focus_directions、risk_warnings、notes
- `alerts`
- `watch_context`
- `metadata`

## Watch Config Target Model

The-One 的 `config/watch.json` 比 TradePilot 当前版本更完整。长期应对齐到以下语义能力：

### Group Structure
- `positions`
- `watchlist`

每个 group 都包含：
- `sectors`
- `stocks`

### Sector Metadata
- `name`
- `role`
- `thesis`
- `report_aliases`
- `report_include_keywords`
- `report_exclude_keywords`
- `signal_carrier`

### Stock Metadata
- `code`
- `name`
- `cost`
- `theme`
- `role`
- `eco_position`
- `controller_quality`
- `organization_quality`
- `commercialization_stage`
- `thesis`
- `notes`

这意味着 watch config 在 TradePilot 中长期不应只是“平面 watchlist”，而应成为：
- workflow 输入对象清单
- 新闻/研究映射语义配置
- 持仓与观察池的长期记忆层

## Market Reference Config

当前代码中仍存在大量 hardcode 的指数代码 / ETF 代码。长期应统一收口为 market reference config，集中定义：
- 核心指数池
- 风格代理指数池
- ETF 风险代理池
- 名称映射
- 角色定义
- SQL / builder / UI 公共消费配置

## Trading-Day Reference Layer

当前 `trading_calendar` 已经存在，但还没有正式提升为 workflow 的统一 reference layer。

长期应统一用于：
- requested date -> resolved date 解析
- pre-market 非交易日 fallback 到下一交易日
- post-market 非交易日 fallback 到上一交易日
- 新闻时间窗口切分
- scheduler 对 phase/date 的判断

应避免多个模块各自判断“今天是不是交易日”。

## Price / Adjustment Policy

当前系统已有：
- `stock_daily`
- `stock_weekly`
- `stock_monthly`

但还没有清晰定义：
- 前复权 / 后复权 / 不复权
- 是否存复权因子
- workflow 使用哪种价格口径进行趋势判断与持仓健康度分析

长期必须统一：
- 系统主价格口径
- 复权因子的存储与使用方式
- `market_overview` / `position_health` / `watch stock review` 的统一价格基准

## Current Implementation Status

### 已完成
- [x] 定义 Briefing Schema v1
- [x] `WorkflowSummary` 转为 briefing-first 结构
- [x] post-market 主路径摆脱 `DailyScanner.run()`
- [x] `market_overview` 初版
- [x] `sector_positioning` DuckDB `sector_data` 优先
- [x] `position_health` 初版
- [x] `next_day_prep` 初版
- [x] pre-market 承接 post carry-over 的基础链路
- [x] 前端导航收口为 `Daily Workflow` + `持仓管理`
- [x] 主应用卸载 legacy router
- [x] `summary` 收口为 `watchlist` / `trading-status`
- [x] 默认结构化数据源切到 Tushare

### 已明确但未完全落地
- [x] 明确 TradePilot = context producer / data platform
- [x] 明确 The-One = insight producer / context consumer
- [x] 明确 Pull for context + Write-back for insight 交互模式
- [x] 明确 Web 优先显示 insight、缺失时回退显示 context

### 进行中 / 未完成
- [x] richer watch config（已完成最小可用版，兼容旧平面 watchlist）
- [x] market reference config（已完成最小收口版，workflow 主链 hardcode 已集中）
- [ ] trading-day unified reference layer
- [ ] price/adjustment policy
- [x] real overnight news pipeline（已完成最小可用版：财联社 / 东方财富采集 + 分类 + sector mappings）
- [x] post -> pre reference block refinement（已完成基础版：overnight news -> sector mappings -> today_watchlist/action_frame）
- [x] The-One insight JSON schema（已完成 v1，含标准 section key）
- [x] insight write-back storage/API
- [ ] workflow regression tests

## Remaining Work

### P0 — 当前阶段后的剩余关键项

1. **TradePilot ↔ The-One 的运行级联调尚未完成**
   - context / insight contract、write-back API、Dashboard 展示都已具备。
   - 当前主要缺口是 The-One 侧正式接入、回写时机、失败与 stale 处理流程。

2. **briefing/context 数据目前仍主要以 `workflow_runs.summary_json` 持久化，粒度偏粗**
   - 当前已足够支撑本阶段功能，但仍不利于 reference block 复用、intraday 复用与单块调试。
   - 后续再评估 `workflow_contexts` / `workflow_reference_snapshots` 是否值得拆出。

3. **交易日数据尚未真正成为 workflow 的统一 reference layer**
   - 仍需统一 fallback、新闻窗口与 scheduler 逻辑。

4. **复权数据口径尚未明确**
   - 仍需定义 workflow 的统一价格基准，避免不同模块使用不同价格口径。

### P1 — 应做（影响交易帮助与系统质量）

8. **`next_day_prep` 仍偏模板化**
   - 尚未真正把 `market_overview` + `sector_positioning` + `position_health` 汇总成高质量明日准备。

9. **pre-market reference context 仍不完整**
   - 需要更明确承接：
     - `last_limit_stats`
     - `last_style`
     - `last_etf_proxies`
     - `sector_reference_records`

10. **`market_overview` 还需进一步做实**
   - 当前仍欠缺：炸板数/率、连板高度、更准确风格代理、ETF 风险代理与更自然文案。

11. **`sector_positioning` 仍需增强**
   - 当前仍欠缺：更可靠主线识别、`leader_stock`、更强角色语义与更像 briefing 的叙事。

12. **`position_health` 还不够深入**
   - 当前仍欠缺：与成本/盈亏状态结合、更清晰风险分层、更明确的持有/观察/风险升级表达。

13. **The-One Skill Gap Checklist 仍未补齐**
   - 当前仍缺：
     - watch loader / normalizer
     - reference snapshot block
     - richer narrative layer
     - 更清晰 workflow history reuse 规范

### P2 — 可后做（工程收尾与下一阶段）

14. **Legacy 代码还未彻底清理**
   - 目前仅完成主路由与主导航收口，仓库内 legacy 模块仍需决定长期冻结还是删除/隔离。

15. **缺少回归测试护栏**
   - 至少应补：
     - pre/post workflow API 测试
     - 非交易日 fallback 测试
     - 新闻状态测试
     - insight write-back/read 测试
     - Tushare 默认 provider 主路径测试

16. **第二阶段 intraday watch 尚未开始**
   - 已有设计方向，但尚未形成正式 schema、API 与 UI。

## Recommended Next Step

基于当前实现进度，本阶段基础功能已经足够，推荐把下一步明确为：

### Step 1 — 和 The-One 完成最小联调闭环
- The-One 拉取 `GET /api/workflow/context/latest`
- The-One 按当前标准 insight schema 生成结果
- The-One 回写 `PUT /api/workflow/insight`
- 验证 fresh / stale / failed 三种状态的 Dashboard 行为

### Step 2 — 只补必要的护栏，而不是继续扩张主路径
- workflow regression tests
- summary/watchlist 兼容性测试
- news mapping 与 direction 规则测试

### Step 3 — 将更大范围的 schema / storage / policy 工作后置
- `workflow_contexts` / reference snapshot 拆分
- trading-day unified reference layer
- price policy
- intraday watch

当前阶段不再建议继续扩张业务面；优先把现有能力稳定住，并完成 The-One 真正接入。

## Open Questions

- [ ] 第一阶段是否隐藏 `Portfolio` 独立页面，只保留后端持仓数据读写？
- [ ] `briefing` API 是否保留仅 alerts/read 兼容，还是进一步内收进 workflow？
- [ ] context / insight 是否共用同一个 run id 作为最终对外主键？
- [ ] insight JSON 是否需要同时保存 markdown 渲染版本？
- [ ] The-One 结果失败时，是否允许保留上一次成功 insight 作为参考展示？
- [ ] intraday phase 后续是否也沿用同样的 context + insight 双层模式？

## Session Log

- **2026-04-06**: 完成 briefing-first 重构方向讨论，明确 TradePilot 第一优先服务主观波段交易，不做回测/研究平台。
- **2026-04-06**: 参考 The-One daily workflow 与 briefings，确认盘后核心输出为市场大势、板块定位、持仓健康度、明日准备；盘前核心输出为昨日复盘摘要、隔夜信息、今日关注清单、操作计划。
- **2026-04-06**: 完成 workflow summary briefing 化、post/pre builder 初版、legacy app shell 收口、summary 路由收口，以及默认结构化数据源切换到 Tushare。
- **2026-04-06**: 明确最终系统定位：TradePilot 是 workflow data platform / context producer，The-One 是 insight producer / context consumer。
- **2026-04-06**: 明确两者交互协议方向：Pull for context + Write-back for insight；Web 优先展示 insight，缺失时回退展示 context 并提示触发。
- **2026-04-06**: 将现有 TODO 与最终目标统一重写为本设计文档，作为后续实现的主参照。
