# ETF 全天候风险预算设计

## 目的

本文冻结 `derived.etf_aw_risk_budget` 的 V1 设计。

风险预算层位于 `derived.etf_aw_strategy_context` 之后、`derived.etf_aw_target_weight` 之前。它只回答一个问题：在当前月度 rebalance context 下，每个 sleeve 应分配多少风险预算。

该层不生成目标权重、不生成交易建议、不读取当前持仓。

## 范围

输入：

- `derived.etf_aw_strategy_context`
- `derived.etf_aw_regime_score`

输出：

- 每个 rebalance date、每个 sleeve role 一行风险预算。
- 中性预算、regime 偏移、最终 tilted budget。
- 预算状态、预算依据和诊断信息。

非范围：

- ETF 目标权重。
- 波动率或协方差估计。
- inverse-vol、ERC 或风险平价求解。
- no-trade band。
- 当前持仓、订单、交易计划。
- Dashboard 展示。

## 数据集

数据集名称：

```text
derived.etf_aw_risk_budget
```

粒度：

```text
calendar_name + rebalance_date + strategy_name + sleeve_role
```

建议 schema：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定为 `etf_aw_risk_budget_v1` |
| `contract_version` | 固定为 `etf_aw_risk_budget_contract_v1` |
| `calendar_name` | 调仓日历名称 |
| `rebalance_date` | 月度调仓日期 |
| `strategy_name` | 策略名称，V1 使用 `etf_aw_v1` |
| `strategy_version` | 策略版本，V1 使用 `risk_budget_v1` |
| `sleeve_role` | `equity_large` / `equity_small` / `bond` / `gold` / `cash` |
| `base_budget` | 中性风险预算 |
| `delta_budget` | market regime 对该 sleeve 的风险预算偏移 |
| `tilted_budget` | 最终风险预算 |
| `confidence_score` | 来源置信度原始值 |
| `effective_confidence_score` | 用于缩放 `delta_budget` 的生效置信度 |
| `market_regime_label` | 来源 market regime |
| `budget_status` | `complete` / `partial` / `stale` / `missing` / `unavailable` |
| `budget_basis` | 预算生成规则说明 |
| `quality_notes_json` | 降级原因、来源 caveat 和数值诊断 |
| `source_strategy_context_rebalance_date` | 来源 strategy context 日期 |
| `source_regime_rebalance_date` | 来源 regime score 日期 |
| `ingested_at` | 写入时间 |

禁止字段：

- `target_weight`
- `raw_target_weight`
- `constrained_target_weight`
- `trade_action`
- `order_instruction`
- `rebalance_instruction`

风险预算层必须保持和 Stage G 一样的上下文边界，不得把目标权重或交易动作混入本数据集。

## Sleeve 顺序

所有预算向量按以下顺序定义：

```text
equity_large, equity_small, bond, gold, cash
```

V1 frozen sleeves 对应：

| Sleeve role | Code |
| --- | --- |
| `equity_large` | `510300.SH` |
| `equity_small` | `159845.SZ` |
| `bond` | `511010.SH` |
| `gold` | `518850.SH` |
| `cash` | `159001.SZ` |

## 中性预算

V1 使用均衡中性风险预算：

| Sleeve role | `base_budget` |
| --- | ---: |
| `equity_large` | `0.20` |
| `equity_small` | `0.20` |
| `bond` | `0.20` |
| `gold` | `0.20` |
| `cash` | `0.20` |

理由：

- 当前 Stage E regime 仍是 market-only scoring，不能支持激进风险预算。
- V1 的重点是验证合同、降级和后续权重引擎输入，不是寻找最优参数。
- 更复杂的长期中性预算应等 target weight 和后置 baseline evaluation 完成后再评估。

## Regime Delta 向量

`delta_budget` 按 market regime 固定，且每个向量合计必须为 `0`。

| Market regime | equity_large | equity_small | bond | gold | cash |
| --- | ---: | ---: | ---: | ---: | ---: |
| `risk_on` | `0.05` | `0.05` | `-0.02` | `-0.03` | `-0.05` |
| `hedge_bid` | `-0.04` | `-0.05` | `0.02` | `0.05` | `0.02` |
| `defensive` | `-0.05` | `-0.05` | `0.05` | `0.01` | `0.04` |
| `mixed` | `0.00` | `0.00` | `0.00` | `0.00` | `0.00` |
| `insufficient_data` | `0.00` | `0.00` | `0.00` | `0.00` | `0.00` |

设计约束：

- 单个 sleeve 的最大偏移绝对值不超过 `0.05`。
- 偏移只表达风险预算方向，不等于 ETF 权重变化。
- `risk_on` 不把防御 sleeve 清零。
- `hedge_bid` 主要提高黄金和现金/防御预算。
- `defensive` 主要提高债券和现金预算。
- `mixed` 和 `insufficient_data` 不做主动 tilt。

## 计算规则

核心公式：

```text
raw_budget = base_budget + effective_confidence_score * delta_budget
tilted_budget = normalize(raw_budget)
```

归一化规则：

```text
normalize(x) = x / sum(x)
```

V1 约束：

- `base_budget` 合计必须为 `1`。
- `delta_budget` 合计必须为 `0`。
- `raw_budget` 任一项不得小于 `0.05`。
- `tilted_budget` 合计必须为 `1`。
- 写出前预算值保留 6 位小数。

因为 V1 delta 向量较小，正常情况下 `raw_budget` 不应触发下限。如果触发，应降级为 `partial` 并回落到中性预算，而不是裁剪后继续输出主动 tilt。

## Confidence 规则

`confidence_score` 来源于 `derived.etf_aw_regime_score` 或 strategy context 中透传的 regime confidence。

V1 使用：

```text
effective_confidence_score = clamp(confidence_score, 0.0, 0.70)
```

原因：

- 当前 regime 是 market-only scoring，置信度不能被解释为完整宏观状态置信度。
- 宏观/利率字段在 V1 只用于上下文和 caveat，不单独触发大幅预算切换。

如果 confidence 缺失、非数值或小于 `0.25`：

- `budget_status = partial`
- `effective_confidence_score = 0.0`
- 输出中性预算
- `quality_notes_json.reasons` 包含 `low_or_missing_confidence`

字段语义：

- `confidence_score` 保存来源原始值，便于审计。
- `effective_confidence_score` 保存经过缺失处理、低置信度回落和上限 clamp 后的实际缩放值。
- 如果来源缺失，`confidence_score` 可为 `null`，但 `effective_confidence_score` 必须为 `0.0`。

## 状态映射

| 输入状态 | 输出行为 |
| --- | --- |
| strategy context `context_status = complete` 且 `readiness_level = research_ready`，regime `scoring_status = complete` | 按 regime delta 生成 tilted budget |
| strategy context `context_status = partial` 且 `readiness_level = degraded_research` | `budget_status = partial`，confidence 上限降为 `0.35` |
| strategy context `context_status = stale` | `budget_status = stale`，输出中性预算 |
| strategy context `context_status = unavailable` | `budget_status = unavailable`，输出中性预算 |
| strategy context 缺失 | `budget_status = missing`，输出中性预算 |
| regime `scoring_status = partial` | `budget_status = partial`，confidence 上限降为 `0.35` |
| regime `scoring_status = stale` | `budget_status = stale`，输出中性预算 |
| regime `market_regime_label = insufficient_data` 或 `scoring_status = unavailable` | `budget_status = partial`，输出中性预算 |
| regime 缺失 | `budget_status = unavailable`，输出中性预算 |

降级原则：

- 缺失数据只能降低主动偏移，不能放大偏移。
- 宏观字段缺失不能单独改变方向，只能写入 caveat。
- 如果 market regime label 不在允许集合内，输出 `unavailable` 和中性预算。

## Point-in-time 规则

风险预算不得使用 rebalance date 之后才可见的数据。

约束：

- `source_strategy_context_rebalance_date` 必须小于或等于当前 `rebalance_date`。
- `source_regime_rebalance_date` 必须小于或等于当前 `rebalance_date`。
- 如果来源 context 的 effective date 或 release date 晚于当前 `rebalance_date`，必须标记 `unavailable` 并输出中性预算。
- 不允许为了填满某个月度预算而向后查找未来 context。
- 如果使用最近一期非同日 context，必须在 `quality_notes_json` 中记录来源日期和 stale / carry-forward 原因。

## Budget Basis

`budget_basis` 使用短字符串，便于 read model 和 Dashboard 后续展示。

允许值：

| `budget_basis` | 含义 |
| --- | --- |
| `neutral_equal_risk_budget` | 中性等风险预算 |
| `market_regime_tilt` | market regime 小幅偏移预算 |
| `degraded_neutral_budget` | 降级回落中性预算 |
| `unavailable_neutral_budget` | 来源不可用，回落中性预算 |

## Quality Notes

`quality_notes_json` 必须是 JSON object。

建议字段：

```json
{
  "reasons": [],
  "source_context_status": "complete",
  "source_readiness_level": "research_ready",
  "source_regime_status": "complete",
  "raw_confidence_score": 0.55,
  "effective_confidence_score": 0.55,
  "raw_budget_min": 0.1725,
  "delta_budget_sum": 0.0,
  "tilted_budget_sum": 1.0,
  "macro_rates_context_status": "complete",
  "caveats": []
}
```

常见 `reasons`：

- `low_or_missing_confidence`
- `strategy_context_missing`
- `strategy_context_stale`
- `strategy_context_partial`
- `strategy_context_unavailable`
- `regime_missing`
- `regime_insufficient_data`
- `unsupported_regime_label`
- `raw_budget_floor_breach`
- `macro_fields_missing`
- `source_context_after_rebalance_date`
- `source_regime_after_rebalance_date`

## Read Model 合同

建议新增：

- `get_latest_etf_aw_risk_budget`
- `list_etf_aw_risk_budgets`

返回合同：

- `schema_version = etf_aw_risk_budget_v1`
- `contract_version = etf_aw_risk_budget_contract_v1`
- 每个 rebalance date 必须包含 5 个 sleeve role。
- `base_budget_sum` 和 `tilted_budget_sum` 应在 read model 中显式返回或可计算。
- read model 不返回 target weight 或 trade action。

如果最新数据缺失，应返回 `None` 或空列表，不能合成预算。

## 测试要求

最小测试集：

- `risk_on` 生成权益预算上调、债券/黄金/现金按 delta 下调。
- `hedge_bid` 生成黄金和现金/防御预算上调。
- `defensive` 生成债券和现金预算上调。
- `mixed` 输出中性预算。
- `insufficient_data` 输出中性预算并标记 `partial`。
- confidence 缺失或过低时输出中性预算。
- strategy context partial 时降低 confidence 上限。
- strategy context stale/missing/unavailable 时输出中性预算。
- regime partial 时降低 confidence 上限。
- regime stale/missing/unavailable 时输出中性预算。
- macro 字段缺失只写 caveat，不改变方向。
- 每个 rebalance date 输出 5 行。
- `base_budget` 合计为 `1`。
- `delta_budget` 合计为 `0`。
- `tilted_budget` 合计为 `1`。
- 来源日期晚于 rebalance date 时输出中性预算并标记不可用。
- 输出不包含 `target_weight`、`trade_action`、`order_instruction`。
- 重复运行 upsert 不产生重复 business key。

测试应使用临时 DuckDB 和临时 lakehouse fixture，不依赖网络。

## 健康检查清单

`derived.etf_aw_risk_budget` 写出前必须运行规则化健康检查。健康检查应是纯函数，不读盘、不触发数据重算；构建流程拿到 finding 后决定阻断或降级。

### FAIL

出现以下情况必须阻断写出：

- 任一 rebalance date 不是 5 个 sleeve role。
- `base_budget` 合计不等于 `1`，容忍浮点误差不超过 `1e-6`。
- `delta_budget` 合计不等于 `0`，容忍浮点误差不超过 `1e-6`。
- `tilted_budget` 合计不等于 `1`，容忍浮点误差不超过 `1e-6`。
- 任一 `base_budget`、`tilted_budget` 为负数或非数值。
- `market_regime_label` 不在允许集合内但仍输出 `market_regime_tilt`。
- 输出字段包含 `target_weight`、`trade_action`、`order_instruction` 或 `rebalance_instruction`。
- 同一 business key 出现重复行。
- 来源 context 或 regime 日期晚于当前 `rebalance_date`。

出现以下情况必须回落到中性预算后才能写出；如果仍输出主动 tilt，则视为 FAIL：

- `raw_budget` 任一项低于 `0.05`。
- `budget_status` 为 `complete` 时使用了 partial、missing、stale 或 unavailable 的来源上下文。
- confidence 缺失、非数值或低于 `0.25`。
- regime `scoring_status` 不是 `complete`。

### WARN

出现以下情况允许写出，但必须进入 `quality_notes_json.reasons` 或 health finding：

- `confidence_score` 低于 `0.25`，预算回落中性。
- strategy context 为 `partial`，effective confidence 被降级上限约束。
- regime scoring status 为 `partial`，effective confidence 被降级上限约束。
- 宏观/利率字段缺失，但不改变预算方向。
- `tilted_budget` 与中性预算完全相同，但来源 regime 不是 `mixed` 或 `insufficient_data`。
- `budget_status` 连续多个 rebalance date 为非 `complete`。

### 输出建议

健康检查 finding 建议包含：

- `level`: `FAIL` / `WARN`
- `check_name`
- `rebalance_date`
- `sleeve_role`，如适用
- `message`
- `details`

FAIL 不能被 Dashboard 或后续 target weight 静默忽略。后续 `derived.etf_aw_target_weight` 只能消费通过健康检查的 risk budget artifact。

## 实现边界

后续实现应遵循现有 derived dataset 风格：

```text
_build_*() -> _make_*_frame(df) -> _write_*()
```

建议拆分：

- `_build_etf_aw_risk_budget(start, end)`
- `_make_etf_aw_risk_budget_frame(strategy_context, regime)`
- `_write_etf_aw_risk_budget(frame)`
- `_validate_risk_budget_frame(frame)`

`_make_etf_aw_risk_budget_frame` 必须保持纯函数，便于构造降级测试。

## 后续衔接

`derived.etf_aw_target_weight` 应把 `tilted_budget` 作为输入风险预算。

target weight 层负责：

- 读取 sleeve 日频收益。
- 估计波动率或协方差。
- 把风险预算转换为 ETF 目标权重。
- 处理 vol floor、cash 低波动、权重上限和 no-trade band。

risk budget 层不得提前处理这些问题。

## 验收标准

本文档冻结后，进入实现前必须满足：

- 数值向量已固定。
- 降级规则已固定。
- schema 和 read model 合同已固定。
- 测试用例边界已固定。
- 明确不输出目标权重和交易建议。

实现完成的最低验收：

- `derived.etf_aw_risk_budget` dataset 注册完成。
- bootstrap profile 可运行。
- read model 可读取最新预算。
- 单元测试覆盖有效、降级、缺失、低置信度和禁止字段。
- 不改变 Stage B-G 既有合同。
