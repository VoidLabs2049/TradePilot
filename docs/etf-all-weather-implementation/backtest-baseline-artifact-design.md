# ETF 全天候回测基线权重 Artifact 设计

## 目的

本文定义 Backtest Evaluation Baseline 的最小设计边界。

本阶段先把基线权重冻结成独立 artifact，再用现有 `derived.etf_aw_backtest_kernel` 分别运行策略和基线结果。它不直接建设 read model、API、前端展示或 rebalance plan。

## 阶段边界

输入：

- `derived.etf_aw_sleeve_daily`
- `reference.rebalance_calendar.monthly_post_20`
- frozen sleeve universe
- 当前策略权重 `derived.etf_aw_target_weight`

输出：

- 独立的 frozen baseline weight artifact。
- 静态 inverse-vol baseline 回测结果。
- 使用同一个 backtest kernel 生成的策略和基线指标、净值、换手和 diagnostics。

非范围：

- 不在回测循环中临时计算或调参 baseline。
- 不把 baseline 权重混入 `derived.etf_aw_target_weight`。
- 不新增 `/api/workflow/etf-aw/backtest`。
- 不建设 backtest read model。
- 不做前端多策略对比图。
- 不生成 `derived.etf_aw_rebalance_plan`。
- 不输出交易动作、订单或持仓调整建议。

## Artifact 定义

建议新增数据集：

```text
derived.etf_aw_baseline_weight
```

粒度：

```text
calendar_name + rebalance_date + baseline_name + baseline_version + sleeve_code
```

V1 schema：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定为 `etf_aw_baseline_weight_v1` |
| `contract_version` | 固定为 `etf_aw_baseline_weight_contract_v1` |
| `calendar_name` | 调仓日历名称 |
| `rebalance_date` | 月度调仓日期 |
| `effective_date` | 权重生效日期，V1 等于 `rebalance_date` |
| `baseline_name` | 基线名称 |
| `baseline_version` | 基线版本 |
| `sleeve_code` | frozen ETF 代码 |
| `sleeve_role` | frozen sleeve role |
| `target_weight` | frozen baseline 目标权重 |
| `estimation_window_days` | 估计波动使用的交易日窗口 |
| `min_observation_days` | 最少有效观测要求；不适用时为 `null` |
| `volatility_estimate` | inverse-vol 使用的波动率估计；不适用时为 `null` |
| `optimizer_name` | V1 固定为 `static_inverse_vol` |
| `optimizer_basis` | 生成规则说明 |
| `quality_notes_json` | 降级原因、样本覆盖和数值诊断 |
| `source_sleeve_daily_max_trade_date` | 用于估计的最大交易日 |
| `ingested_at` | 写入时间 |

禁止字段：

- `trade_action`
- `order_instruction`
- `rebalance_instruction`
- `order_quantity`
- `order_amount`
- `broker_account`

V1 不写出 partial baseline 行。任一 `rebalance_date` 无法形成 5 sleeve 完整、非负、有限且权重和为 1 的向量时，本次 baseline artifact 写出应被阻断，并通过构建结果或 diagnostics 返回原因。baseline 是评价基准，不应把不完整基线落库后再交给 kernel 解释。

## Baseline 范围

### 必做：静态 inverse-vol

V1 必须先实现：

```text
baseline_name = static_inverse_vol
baseline_version = static_inverse_vol_v1
optimizer_name = static_inverse_vol
```

计算规则：

1. 对每个 `rebalance_date`，只使用 `trade_date <= rebalance_date` 的 sleeve 日收益。
2. 使用最近 `63` 个交易日收益估计波动率，至少要求 `42` 个有效观测。
3. 对每个 sleeve 计算：

```text
inverse_vol_score_i = 1 / max(volatility_i, volatility_floor)
target_weight_i = inverse_vol_score_i / sum(inverse_vol_score)
```

4. V1 使用与目标权重层一致的 `volatility_floor = 0.005`，避免低波动 cash sleeve 权重发散。
5. 每个 `rebalance_date` 必须输出 5 个 frozen sleeve。缺任一 sleeve 时不能写出 baseline artifact。

静态 inverse-vol 是基线，不读取 `derived.etf_aw_risk_budget`，也不读取当前策略的 target weight。

### 暂缓：risk-parity-like

risk-parity-like baseline 暂不进入本阶段实现。它容易把当前目标从“建立评价基线”扩大成“研究新优化器”，应等 `static_inverse_vol_v1` artifact、kernel 运行和 report 对比稳定后再单独设计。

后续如需新增，应作为独立 artifact：

```text
baseline_name = risk_parity_like
baseline_version = risk_parity_like_v1
optimizer_name = risk_parity_like
```

完整 ERC 求解器、协方差收缩、约束优化或参数搜索均不属于当前阶段。

## 与 Backtest Kernel 的关系

`derived.etf_aw_backtest_kernel` 是唯一回测执行层。

本阶段应让 kernel 可以消费两类 frozen 权重输入：

- 当前策略：`derived.etf_aw_target_weight`
- 基线：`derived.etf_aw_baseline_weight`

每条回测结果必须保留独立标识：

| 权重来源 | `strategy_name` | `strategy_version` | `weight_source_type` | `source_weight_dataset` |
| --- | --- | --- | --- | --- |
| 当前策略 | 沿用 `derived.etf_aw_target_weight.strategy_name` | 沿用 `derived.etf_aw_target_weight.strategy_version` | `target_weight` | `derived.etf_aw_target_weight` |
| 静态 inverse-vol baseline | `static_inverse_vol` | `static_inverse_vol_v1` | `baseline` | `derived.etf_aw_baseline_weight` |

`weight_source_type` 和 `source_weight_dataset` 应写入 backtest kernel 输出。这样 read model 和 report 不需要从 `strategy_name` 反推来源，也可以清楚地区分当前策略权重和 baseline 权重。

kernel 不应知道 baseline 的生成细节。它只消费已经冻结的月度权重序列、日频 sleeve 收益和调仓日历。

## CLI 顺序

建议命令顺序：

```text
python -m tradepilot.etf_aw.cli build-target-weight
python -m tradepilot.etf_aw.cli build-baseline-weight --baseline static-inverse-vol
python -m tradepilot.etf_aw.cli backtest-kernel --strategy target-weight
python -m tradepilot.etf_aw.cli backtest-kernel --strategy baseline --baseline static-inverse-vol
python -m tradepilot.etf_aw.cli backtest-report
```

如果当前 CLI 暂时不支持这些参数，实现时可以选择更小的命令边界，但必须保持两个原则：

- baseline weight 先落成 frozen artifact。
- backtest kernel 只读取 frozen weight，不在运行中生成 baseline。

## 验证要求

Artifact 测试至少覆盖：

- 每个 `rebalance_date` 输出 5 个 frozen sleeve。
- 每个权重向量 `sum(target_weight)` 在容忍误差内等于 `1`。
- `target_weight` 非负且有限。
- 缺少 sleeve 日收益或样本不足时阻断 artifact 写出，不能写出 partial 行。
- `quality_notes_json` 是合法 JSON。
- 重复业务键被拒绝。

Kernel 集成测试至少覆盖：

- 当前策略权重可以继续产生原有回测结果。
- 静态 inverse-vol baseline 可以产生独立 `strategy_name` / `strategy_version` 的 `daily_nav`、`metric`、`turnover` 和 `diagnostic` 行。
- kernel 输出保留 `weight_source_type` 和 `source_weight_dataset`。
- 同一时间范围内，策略和 baseline 的结果不会互相覆盖。
- `backtest-report` 可以按策略标识和权重来源区分输出。

## 完成标准

本阶段完成时应满足：

- `derived.etf_aw_baseline_weight` 设计和实现稳定。
- 静态 inverse-vol baseline 作为独立 artifact 可重复生成。
- `derived.etf_aw_backtest_kernel` 能对策略和 baseline 分别运行。
- CLI report 至少输出当前策略和 `static_inverse_vol_v1` 的收益、回撤、年化波动、换手、最大回撤和 diagnostics。
- CLI report 至少输出当前策略相对 `static_inverse_vol_v1` 的收益差、最大回撤差、年化波动差、换手差和异常 diagnostics 摘要。
- 尚未建设 read model、API、前端多策略对比或 rebalance plan。
