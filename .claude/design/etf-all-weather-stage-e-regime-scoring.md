---
title: "ETF All-Weather Stage E Regime Scoring"
status: draft
mode: "design"
created: 2026-05-07
updated: 2026-05-07
modules: ["backend", "frontend"]
---

# ETF All-Weather Stage E Regime Scoring

## Overview

Stage E 的职责是把 Stage D 已经生成的 ETF 全天候月度调仓 snapshot，转换成一个可解释、可降级、可被 workflow / dashboard 消费的规则型状态评分层。

这个阶段必须先解决一个命名和边界问题：

- `data-ingestion-architecture.md` 里的 Stage E 指的是 `minimum market panel`，当前代码与 Stage C 报告中已经基本落地。
- 本文的 Stage E 指的是 ETF all-weather strategy path 中 Stage D 之后的 `regime scoring / confidence score`。

为避免混淆，本文后续将该阶段称为：

> ETF All-Weather Stage E：market-only regime scoring

这里的 `market-only` 是刻意的。当前系统还没有完成 timing-sensitive macro / rates / curve 数据接入，因此 Stage E 不能声称已经实现完整宏观 regime 判断。它只能基于 Stage D 的 adjustment-aware market snapshot 做市场确认型 scoring，并且必须把缺少宏观/利率输入反映到 confidence cap 中。

## Relationship With Stage D

Stage E 直接依赖 Stage D 的输出：

- `derived.etf_aw_rebalance_snapshot`
- `get_latest_etf_aw_snapshot(as_of_date)`
- `list_etf_aw_snapshots(start, end)`
- workflow 中已有的 `etf_aw_context`
- dashboard 中已有的 ETF 全天候上下文展示区域

Stage E 不重新读取 raw / normalized market parquet，不重新计算复权收益，不重新生成 rebalance dates，也不发起 source fetch。

Stage D 已经负责：

- 每个 rebalance date 输出 frozen 5 sleeves
- 计算 1m / 3m / 6m return
- 计算 3m volatility
- 计算 6m max drawdown
- 标记 `complete` / `partial` / `missing` / `stale`
- 返回稳定 snapshot JSON contract

Stage E 只在这个 contract 上继续加工。

## Stage E Goal

- [ ] 生成 ETF all-weather v1 market-only regime score
- [ ] 生成 confidence score，并显式受 data quality 和 market-only 输入边界约束
- [ ] 输出可解释 signal breakdown，说明每个 sleeve 如何影响判断
- [ ] 对 `missing` / `partial` / `stale` snapshot 做明确降级，而不是输出伪确定结论
- [ ] 提供 workflow / dashboard 可消费的 read model
- [ ] 为后续 Stage F risk budget 提供输入 contract，但本阶段不生成预算或权重

## Stage E Non-Goals

本阶段明确不做：

- macro slow fields 接入
- rates / curve datasets 接入
- 完整 growth x inflation / growth x credit 宏观四象限
- target risk budget
- inverse-vol / ERC / optimizer
- target weights
- rebalance orders
- trade recommendation
- monthly backtest
- threshold optimization
- machine learning classifier
- generic strategy engine
- 多策略、多 universe 框架

Stage E 的输出可以被 Stage F 使用，但不能提前把 Stage F 的风险预算或权重逻辑塞进来。

## Design Assumptions

1. Frozen v1 sleeve universe 不变：
   - `510300.SH`
   - `159845.SZ`
   - `511010.SH`
   - `518850.SH`
   - `159001.SZ`

2. Monthly rebalance clock 不变：
   - 每月 20 日及以后，SH/SZ 共同开市的第一个交易日。

3. Stage E 只消费 Stage D snapshot：
   - 不直接访问 `market.etf_daily`
   - 不直接访问 `market.etf_adj_factor`
   - 不重新计算 Stage D 指标

4. Confidence 不是收益预测概率：
   - 它表达的是当前 market-only evidence 的一致性和可用性。
   - 它不能被解释为某个资产上涨概率。

5. Market-only score 必须有 confidence cap：
   - 在 macro / rates 未接入前，即使市场信号非常一致，也不能给出接近满分的 regime confidence。

## Immediate Scope

### Inputs

Stage E 输入固定为 Stage D read model：

| Input | Role |
| --- | --- |
| `schema_version` | 确认输入 contract |
| `calendar_name` | 确认 rebalance clock |
| `calendar_month` | 输出分区与展示 |
| `rebalance_date` | 业务时间点 |
| `data_status` | 总体数据质量 gate |
| `sleeves[]` | sleeve-level market evidence |
| `return_1m` | 短期动量 |
| `return_3m` | 中期动量，初版主信号 |
| `return_6m` | 较长确认信号 |
| `volatility_3m` | confidence penalty / risk intensity |
| `max_drawdown_6m` | drawdown penalty |
| `source_max_trade_date` | freshness audit |

### Output

推荐新增一个窄范围 derived read model：

- `derived.etf_aw_regime_score`

推荐 builder profile：

- `derived.etf_aw_regime_score.build`

推荐 read service：

- `get_latest_etf_aw_regime_context(as_of_date)`
- `list_etf_aw_regime_contexts(start, end)`

### Business Key

初版业务键：

- `(calendar_name, rebalance_date, scorer_name, scorer_version)`

推荐固定：

- `scorer_name = "etf_aw_market_only_regime"`
- `scorer_version = "v1"`

这个 key 允许后续引入 macro-aware scorer 时并存，而不是静默覆盖 market-only 历史判断。

### Storage

初版继续写 lakehouse derived zone：

- `data/lakehouse/derived/derived.etf_aw_regime_score/<year>/<month>/part-00000.parquet`

不新增 DuckDB 表。API / workflow 通过 read service 读取 derived parquet，保持 schema churn 最小。

## Regime Contract

### Required Fields

每行代表一个 rebalance date 上的一个 scorer 输出。

| Field | Meaning |
| --- | --- |
| `schema_version` | 初版固定 `etf_aw_regime_score_v1` |
| `calendar_name` | 初版固定 `etf_aw_v1_monthly_post_20` |
| `calendar_month` | `YYYY-MM` |
| `rebalance_date` | 调仓日 |
| `scorer_name` | 初版固定 `etf_aw_market_only_regime` |
| `scorer_version` | 初版固定 `v1` |
| `input_snapshot_status` | Stage D read model 聚合后的总体 data_status |
| `scoring_status` | `complete` / `degraded` / `unavailable` |
| `market_regime_label` | `risk_on` / `defensive` / `hedge_bid` / `mixed` / `insufficient_data` |
| `market_score` | `-100` 到 `100` 的 market-only 状态分 |
| `confidence_score` | `0.0` 到 `1.0` |
| `confidence_level` | `low` / `medium` / `high` |
| `confidence_cap` | 当前输入质量允许的最高 confidence |
| `signal_summary` | 简短解释文本 |
| `signals_json` | sleeve-level signal breakdown，parquet 中存 JSON text |
| `quality_notes` | JSON text，记录降级原因、缺失项、cap 规则 |
| `source_snapshot_rebalance_date` | 输入 snapshot 的 rebalance date |
| `ingested_at` | 生成时间 |

`input_snapshot_status` 仅用于审计和展示。Stage E 的 quality gate 必须检查 sleeve-level statuses，不能只依赖这个聚合字段。

### Allowed Labels

`market_regime_label` 初版只允许：

- `risk_on`
- `defensive`
- `hedge_bid`
- `mixed`
- `insufficient_data`

这些不是完整宏观象限。

解释：

- `risk_on`：权益 sleeve 的市场确认较强，且防御/黄金信号没有明显压倒权益。
- `defensive`：权益走弱，债券或现金 sleeve 相对稳定。
- `hedge_bid`：黄金 sleeve 明显强于权益和债券，可能代表避险或通胀/不确定性定价，但 Stage E 不判断宏观原因。
- `mixed`：信号冲突或强度不足。
- `insufficient_data`：输入质量不足以进行有效 scoring。

不要在 Stage E 使用 `goldilocks` / `reflation` / `deflation` / `stagflation` 这类完整宏观象限标签。那些需要 Stage F 或后续 macro / rates 数据支持。

## Scoring Rules

Stage E 的初版规则应保持小而可测。不要做参数搜索，不要为了回测表现调阈值。

### 1. Sleeve Direction Score

每个 sleeve 先生成一个 `sleeve_direction_score`，范围 `-100` 到 `100`。

建议初版使用三段式 threshold，而不是 z-score。原因是当前 Stage E 只消费单月 snapshot，若为了 z-score 再加载长历史，会扩大边界并重复 Stage D / backtest 层职责。

建议规则：

| Metric | Positive | Negative | Weight |
| --- | ---: | ---: | ---: |
| `return_1m` | `>= 0.015` | `<= -0.015` | `0.25` |
| `return_3m` | `>= 0.030` | `<= -0.030` | `0.45` |
| `return_6m` | `>= 0.050` | `<= -0.050` | `0.30` |

每个指标按以下规则映射：

- 达到正向阈值时记为 `+100`
- 达到负向阈值时记为 `-100`
- 否则记为 `0`

然后计算：

```text
sleeve_direction_score =
  0.25 * signal(return_1m) +
  0.45 * signal(return_3m) +
  0.30 * signal(return_6m)
```

回撤本身不改变方向判断，只作为 confidence penalty：

- `max_drawdown_6m <= -0.12`：强回撤惩罚
- `max_drawdown_6m <= -0.08`：中等回撤惩罚

波动率同样只作为 confidence penalty，不作为方向信号。

### 2. Sleeve Group Scores

Stage E 使用 frozen sleeve roles：

| Group | Sleeves |
| --- | --- |
| `equity_score` | average of `equity_large`, `equity_small` |
| `bond_score` | `bond` |
| `gold_score` | `gold` |
| `cash_score` | `cash` |

### 3. Market Score

推荐初版公式：

```text
risk_appetite_score =
  0.70 * equity_score -
  0.15 * max(bond_score, 0) -
  0.15 * max(gold_score, 0)

market_score =
  clamp(risk_appetite_score, -100, 100)
```

这不是目标配置，也不是资产权重。它只是一个压缩后的 market evidence score。

解释：

- `market_score` 为正，表示权益 sleeve 的风险偏好确认更强
- `market_score` 为负，表示权益走弱且防御 / 黄金 sleeve 有相对确认
- 接近 `0`，表示混合状态或低置信环境

`bond_score` 和 `gold_score` 只有在正向时才降低 `risk_appetite_score`。这避免把防御资产上涨误读成 risk-on，同时不因防御资产下跌而机械提高 risk-on 判断。

### 4. Label Mapping

推荐初版映射：

| Condition | Label |
| --- | --- |
| 输入不可用 | `insufficient_data` |
| `equity_score >= 35` and `market_score >= 25` | `risk_on` |
| `equity_score <= -35` and (`bond_score >= 0` or `cash_score >= 0`) | `defensive` |
| `gold_score >= 45` and `gold_score - equity_score >= 40` | `hedge_bid` |
| 其他情况 | `mixed` |

判断顺序很重要：

1. `insufficient_data`
2. `hedge_bid`
3. `defensive`
4. `risk_on`
5. `mixed`

这个顺序用于避免强黄金避险信号被轻微正向的权益数据掩盖。

## Confidence Rules

Confidence 是一个有上限的 evidence-quality score。它应该回答：

> 下游阶段可以在多大程度上信任这个 market-only label？

它不应该回答：

> 这个 regime 有多大概率带来收益？

### Confidence Caps

先确定 confidence cap，再考虑信号强度。

| Input condition | `scoring_status` | Max confidence |
| --- | --- | ---: |
| 没有 Stage D snapshot | `unavailable` | `0.00` |
| Stage D snapshot 少于 frozen 5 sleeve rows | `unavailable` | `0.20` |
| 任一 sleeve `data_status = "missing"` | `unavailable` | `0.20` |
| 任一 sleeve `data_status = "stale"` | `degraded` | `0.35` |
| 任一 sleeve `data_status = "partial"` | `degraded` | `0.55` |
| 所有 sleeves 均为 `complete`，但仍只有 market-only inputs | `complete` | `0.70` |

`0.70` 上限是刻意设计的。在 macro / rates 满足 point-in-time safety 之前，Stage E 不应该暗示完整 regime 确定性。

cap 判断必须使用 sleeve-level `sleeves[].data_status`，不能只依赖 Stage D read model 聚合后的总体 `data_status`。如果同一个 snapshot 同时存在多种降级状态，采用最严格 cap：

1. 无 snapshot：`0.00`
2. 少于 frozen 5 sleeve rows：`0.20`
3. 任一 sleeve 为 `missing`：`0.20`
4. 任一 sleeve 为 `stale`：`0.35`
5. 任一 sleeve 为 `partial`：`0.55`
6. 全部 `complete`：`0.70`

Stage D 的正常 contract 应该始终输出 5 个 frozen sleeve rows。少行属于结构性 contract violation；Stage E 可以生成 `unavailable` 诊断输出，但不能把少行解释成 universe 变化。

### Raw Confidence

推荐初版公式：

```text
agreement_score = 可用 sleeves 中方向与最终 label 一致的比例
strength_score = min(abs(market_score) / 100, 1.0)
penalty_score = drawdown_penalty + volatility_penalty

raw_confidence =
  0.35 +
  0.35 * agreement_score +
  0.30 * strength_score -
  penalty_score

confidence_score = min(confidence_cap, max(raw_confidence, 0.0))
```

`agreement_score` 必须按 label 明确定义，避免实现自由发挥：

| Final label | Consistent sleeve evidence |
| --- | --- |
| `risk_on` | `equity_large` 或 `equity_small` 的 `direction_score > 0` |
| `defensive` | equity sleeve `direction_score < 0`，或 `bond` / `cash` 的 `direction_score >= 0` |
| `hedge_bid` | `gold` 的 `direction_score > 0`，或 equity sleeve `direction_score <= 0` |
| `mixed` | 固定 `agreement_score = 0.25` |
| `insufficient_data` | 固定 `agreement_score = 0.0` |

分母只包含可用 sleeves。`data_status = "missing"` 或三项 return 指标都为空的 sleeve 不参与分母；如果分母为 0，`agreement_score = 0.0`。

Penalty 规则应在实现中作为显式常量，并由测试覆盖：

- 强回撤惩罚：`0.15`
- 中等回撤惩罚：`0.08`
- 高波动惩罚：`0.10`

初版 penalty 触发规则：

| Penalty | Trigger |
| --- | --- |
| 强回撤惩罚 | 任一可用 sleeve `max_drawdown_6m <= -0.12` |
| 中等回撤惩罚 | 不满足强回撤，且任一可用 sleeve `max_drawdown_6m <= -0.08` |
| 高波动惩罚 | 任一 equity sleeve `volatility_3m >= 0.28`，或任一非 equity sleeve `volatility_3m >= 0.18` |

同一类 penalty 不重复叠加。例如多个 sleeve 触发强回撤时，drawdown penalty 仍为 `0.15`。

`volatility_3m` 沿用 Stage D contract，是基于日收益计算的年化波动率小数值。

Stage E 不应根据回测校准这些常量。参数校准属于后续 research / backtest 阶段。

### Confidence Levels

| `confidence_score` | Level |
| ---: | --- |
| `< 0.35` | `low` |
| `< 0.60` | `medium` |
| `>= 0.60` | `high` |

由于 market-only confidence 被限制在 `0.70`，这里的 `high` 只表示“在这个有限 market-only scorer 内较高”，不是完整宏观判断的高置信度。

## Data Quality Discipline

Stage E 必须严格继承 Stage D 的 data-quality 语义。

规则：

1. 缺失行不能被解释为 universe 变化。
2. `missing` 不能生成 `risk_on`、`defensive` 或 `hedge_bid`。
3. `stale` 可以生成方向 label，但 confidence 必须被 cap，且 quality notes 必须说明 stale source。
4. `partial` 可以生成方向 label，但 confidence 必须被 cap，且 quality notes 必须说明窗口不足。
5. 即使输入是 `complete`，如果信号冲突，confidence 仍可以很低。
6. Market-only completeness 不等于 macro completeness。

这是防止 false confidence 的主要约束。

## Read Contract

推荐 JSON contract：

```json
{
  "schema_version": "etf_aw_regime_score_v1",
  "calendar_name": "etf_aw_v1_monthly_post_20",
  "rebalance_date": "2026-04-20",
  "scorer_name": "etf_aw_market_only_regime",
  "scorer_version": "v1",
  "scoring_status": "complete",
  "market_regime_label": "mixed",
  "market_score": 12.5,
  "confidence_score": 0.48,
  "confidence_level": "medium",
  "confidence_cap": 0.7,
  "signal_summary": "Equity confirmation is modest; gold and bond signals do not dominate.",
  "signals": [
    {
      "sleeve_code": "510300.SH",
      "sleeve_role": "equity_large",
      "direction_score": 25.0,
      "data_status": "complete"
    }
  ],
  "quality_notes": {
    "market_only": true,
    "macro_inputs_available": false,
    "rates_inputs_available": false
  }
}
```

parquet 存储层使用 `signals_json` 和 `quality_notes` 两个 JSON text 字段；read service 对外返回时应把 `signals_json` 解码成 `signals` 数组，把 `quality_notes` 解码成对象。前端类型以 read service contract 为准。

Workflow 应把它作为独立对象暴露，例如：

- `etf_aw_regime_context`

它不应该覆盖现有 A 股 workflow 的 `market_overview.regime`，因为那个字段描述的是另一套 workflow market context。

## Dashboard Scope

如果 Stage E 涉及前端，dashboard 初版只展示：

- 最新 rebalance date
- market regime label
- confidence level / confidence score
- confidence cap reason
- sleeve signal breakdown
- `partial` / `missing` / `stale` 的 quality notes

Dashboard 必须不展示：

- target weights
- buy / sell recommendation
- “should allocate” 类语言
- backtest performance
- optimizer output

## Validation And Success Criteria

### Builder Validation

Stage E builder 必须验证：

1. input snapshot schema version 受支持。
2. 正常 scoring 前，frozen sleeve universe 完整。
3. 每个 sleeve 都使用允许的 `data_status`。
4. 必需指标字段是数值，或按 Stage D status 显式为空。
5. 生成的 `market_regime_label` 属于允许枚举。
6. `market_score` 是有限数，并且位于 `[-100, 100]`。
7. `confidence_score` 是有限数，并且位于 `[0, confidence_cap]`。
8. `confidence_cap` 符合 data-quality 规则。
9. `signals_json` 和 `quality_notes` 是合法 JSON。
10. 输出行不包含 target weights 或 budget 字段。
11. 重复运行不会产生重复业务键。

### Acceptance Criteria

Stage E 完成标准：

1. `derived.etf_aw_regime_score.build` 可以针对指定 rebalance window 运行。
2. 完整的 Stage D snapshot 会生成一条 market-only regime context row。
3. `missing` snapshot data 会生成 `scoring_status = "unavailable"`，并限制 confidence。
4. `stale` snapshot data 会生成 `scoring_status = "degraded"`，且 confidence `<= 0.35`。
5. `partial` snapshot data 会生成 `scoring_status = "degraded"`，且 confidence `<= 0.55`。
6. 完整 market-only data 不会产生高于 `0.70` 的 confidence。
7. sleeve 信号冲突时生成 `mixed` 或低 confidence。
8. Read service 为 workflow / dashboard 返回稳定 JSON。
9. Stage E 测试覆盖 label mapping、confidence caps、degraded inputs 和 idempotent writes。
10. Stage B / C / D 现有测试继续通过。

## Testing Strategy

推荐测试文件：

- `tests/etl/test_stage_e.py`

必需测试：

1. complete risk-on fixture 生成 `risk_on`，且 confidence 不高于 `0.70`。
2. 强黄金 fixture 生成 `hedge_bid`。
3. 权益为负且债券稳定的 fixture 生成 `defensive`。
4. 冲突信号生成 `mixed`。
5. 缺失 sleeve row 生成 `insufficient_data` / `unavailable`。
6. stale input 将 confidence 限制在 `0.35`。
7. partial input 将 confidence 限制在 `0.55`。
8. complete input 将 confidence 限制在 `0.70`。
9. 重复 rebuild 可以 upsert，不产生重复业务键。
10. read service 返回 latest regime context。
11. output contract 不包含 budget、weight 或 trade action 字段。

如果改前端：

- 运行 `cd webapp && yarn build`

Backend:

- 运行 `python -m unittest -v tests/etl/test_stage_e.py`
- 如果 Stage B / C / D 测试文件存在，运行相邻回归测试，例如 `python -m unittest -v tests/etl/test_stage_b.py tests/etl/test_stage_c.py tests/etl/test_stage_d.py tests/etl/test_stage_e.py`
- 运行 `python -c "from tradepilot.main import app; print('OK')"`

## Recommended Implementation Sequence

1. 在 `tradepilot/etl/datasets.py` 注册 `derived.etf_aw_regime_score`。
2. 在 `tradepilot/etl/service.py` 增加窄范围 profile dispatcher。
3. 实现一个小型 scorer helper，使用显式常量和枚举。
4. 只从 Stage D read model 构建 Stage E 输出行。
5. 使用幂等分区覆盖写入 derived parquet。
6. 在 `tradepilot/etl/read_models.py` 增加读取函数。
7. 新增 `tests/etl/test_stage_e.py`。
8. 增加 workflow context 字段 `etf_aw_regime_context`。
9. 后端 contract 稳定后，再选择性接入 dashboard 展示。

## Deferred After Stage E

Stage F:

- 将 regime context 转换为 target risk budgets
- 实现简单 inverse-vol 或 budgeted risk balancing
- 仍然避免 full learnable ERC

Stage G:

- monthly backtest
- baseline comparison pack
- turnover and cost assumptions
- perturbation tests

Stage H:

- shadow run dashboard
- monthly recommendation freeze
- post-mortem review loop

未来 macro / rates integration：

- 只有在 release/effective-date discipline 被编码后，才引入 point-in-time macro / rates features。
- 将 `market_only_regime` 升级为独立的 macro-aware scorer，而不是静默改变历史 Stage E 语义。

## Risks

### 1. False Macro Confidence

风险：

- Market-only score 可能被误认为完整 macro regime label。

缓解：

- 使用 `market_regime_label`，不使用 macro quadrant labels。
- 将完整 market-only confidence 限制在 `0.70`。
- 在 quality notes 中包含 `market_only = true`。

### 2. Scope Creep Into Stage F

风险：

- Regime scoring 悄悄开始生成 budgets 或 weights。

缓解：

- Validation 应拒绝 Stage E output 中的 budget / weight / trade action 字段。
- Stage F 单独写设计文档。

### 3. Threshold Overfitting

风险：

- 阈值被不断调整，直到历史结果看起来更好。

缓解：

- Stage E 常量应保持简单、固定、可测试。
- Backtest-driven calibration 延后。

### 4. Confusing Existing Workflow Regime Fields

风险：

- 现有 A 股 workflow 的 `market_overview.regime` 和 ETF all-weather regime context 被混在一起。

缓解：

- 使用独立字段名，例如 `etf_aw_regime_context`。
- 没有清晰 section 边界时，不复用 dashboard label。

## Final Judgment

Stage E 应从窄范围 market-only scoring layer 开始。

正确交付物不是 “the ETF all-weather strategy engine”。它应该是一个稳定、可解释、质量感知的 regime context，告诉下游阶段：

- 当前 market evidence 指向什么
- 证据强度有多高
- confidence 为什么被 cap
- 输入是 complete、partial、stale，还是 unavailable

只有这个 contract 稳定后，Stage F 才应该把 regime context 转换成 risk budgets 和 weights。
