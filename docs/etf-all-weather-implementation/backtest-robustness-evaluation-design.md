# ETF 全天候回测稳健性评估设计

## 目的

本文定义 Stage M 回测稳健性评估的最小设计边界。

Stage L 已完成当前策略与 `static_inverse_vol_v1` 的 frozen baseline 对比，但正式报告只覆盖 17 个调仓周期，且未计入交易成本。当前策略累计收益仅比 baseline 高 `0.0566` 个百分点，同时年化波动和月均换手更高、Sharpe 更低。

因此下一阶段先回答两个问题：

1. 当前 point-in-time artifact 最多能支持多长、质量如何的可比历史区间。
2. 在统一、透明的成本敏感性场景下，当前策略相对 baseline 的差异是否仍然存在。

本阶段不引入新优化器，不把当前结果解释为实盘收益，也不因短样本结果自动推进 simplified ERC。

## 依赖

本设计依赖 Stage L 已落地能力：

- `derived.etf_aw_target_weight`
- `derived.etf_aw_baseline_weight`
- `derived.etf_aw_backtest_kernel`
- `weight_source_type`
- `source_weight_dataset`
- 多策略 `backtest-report`

开发分支应基于包含 Stage L 的提交或已合并主线，不应复制 baseline 生成逻辑。

## 阶段边界

### 输入

- frozen target weight artifact。
- frozen baseline weight artifact。
- 已写出的 backtest kernel `daily_nav`、`turnover`、`metric` 和 `diagnostic` 行。
- rebalance calendar。
- strategy context、risk budget 和 target weight 的状态分布，仅用于覆盖质量说明。

### 输出

- 可比历史覆盖审计。
- 固定成本场景下的净值和指标摘要。
- 当前策略相对 baseline 的 gross / net 差值。
- 输入缺失、初始建仓成本不可观测和短样本 caveat。
- 可归档的 Markdown / JSON 稳健性报告。

### 非范围

- 不修改 risk budget 或 target weight 公式。
- 不生成新的 baseline 权重。
- 不在评估过程中搜索最优成本参数。
- 不实现 simplified ERC、完整 risk parity 或协方差优化器。
- 不新增 read model、API 或前端图表。
- 不生成 rebalance plan、交易动作或订单。
- 不声称成本场景等于任何券商账户的真实费率。

## 核心原则

### Frozen input

评估层只读取已写出的 kernel 和上游 artifact。它不得重新估计 regime、risk budget、target weight 或 baseline weight。

策略和 baseline 必须使用：

- 相同日期区间。
- 相同交易日集合。
- 相同成本场景。
- 相同净值和指标公式。

### Gross kernel 保持不变

`derived.etf_aw_backtest_kernel` 继续保存未扣成本的 gross 结果。成本敏感性属于 evaluation/report 层，不覆盖或改写现有 kernel 行。

### 不隐藏不可观测量

当前 kernel 的首个 turnover 行为 `0.0`，原因是没有前一期 target weight，初始建仓换手不可观测。评估层不得把它解释为真实零成本建仓。

每条策略必须输出初始建仓成本状态。非零成本场景还必须输出净指标口径：

```text
initial_formation_cost_status = unobservable
net_metric_basis = excludes_initial_formation_cost
```

`gross` 场景不使用 `net_metric_basis`，只复现现有 gross kernel。

## Phase M1：历史覆盖审计

### 目的

先确定可以公平比较策略和 baseline 的真实重叠区间，再运行成本敏感性。不能为了拉长样本而临时补权重、回填未来可见数据或降级为未冻结策略。

### Comparable range

可比起点取以下日期的最大值：

- 当前策略第一个完整 5-sleeve 权重向量的生效日期。
- baseline 第一个完整 5-sleeve 权重向量的生效日期。
- 两条 kernel 都有可用日收益的首日。

可比终点取以下日期的最小值：

- 当前策略 kernel 最后一个可用日。
- baseline kernel 最后一个可用日。

同一可比区间内，两条策略必须拥有相同交易日集合。缺少任一方的日频行时阻断净指标比较，不做 inner join 后静默缩短样本。

### 覆盖审计字段

报告至少输出：

| 字段 | 含义 |
| --- | --- |
| `requested_start_date` | 请求起点 |
| `requested_end_date` | 请求终点 |
| `comparable_start_date` | 实际可比起点 |
| `comparable_end_date` | 实际可比终点 |
| `daily_observation_count` | 共同日频观测数 |
| `rebalance_period_count` | 共同调仓周期数 |
| `strategy_status_counts` | target weight 状态分布 |
| `risk_budget_status_counts` | risk budget 状态分布 |
| `regime_label_counts` | 可比区间状态标签分布 |
| `missing_trade_dates` | 任一策略缺少的交易日 |
| `blocking_reasons` | 阻断原因 |
| `warnings` | partial、短样本或状态集中等 caveat |

### 覆盖诊断

FAIL：

- 策略和 baseline 没有重叠区间。
- 可比区间内任一调仓周期缺少完整 5-sleeve frozen 权重。
- 两条 kernel 的日频日期集合不一致。
- 存在 blocking diagnostic。

WARN：

- target weight 或 risk budget 含 `partial`。
- 只有少量调仓周期。
- regime label 高度集中，不能代表多状态样本。
- 初始建仓成本不可观测。

V1 不为“样本足够长”设置任意自动阈值。报告必须给出精确周期数和状态分布，由后续研究评审判断是否足以支持策略结论。

## Phase M2：成本敏感性

### 成本场景

V1 固定使用研究敏感性网格：

| `cost_scenario` | `cost_bps_per_executed_notional` |
| --- | ---: |
| `gross` | 0 |
| `cost_5bps` | 5 |
| `cost_10bps` | 10 |
| `cost_20bps` | 20 |

这些数值只是压力测试网格，不代表实际佣金、买卖价差、冲击成本或任何券商报价。V1 不提供 CLI 自由参数，避免在报告阶段按结果挑选成本假设。

固定模型版本：

```text
report_version = etf_aw_backtest_robustness_report_v1
cost_model_name = half_l1_turnover_sensitivity
cost_model_version = half_l1_turnover_sensitivity_v1
```

### 换手与成本口径

kernel 当前月度换手为：

```text
monthly_turnover = 0.5 * sum(abs(new_target_weight - previous_target_weight))
```

它是 previous-target 口径，不是调仓前 drifted weight 口径。该限制必须写入每个成本场景的 diagnostics。

成本网格按双边实际成交名义金额计算：

```text
gross_traded_notional = 2 * monthly_turnover
cost_rate = cost_bps_per_executed_notional / 10000
cost_fraction = gross_traded_notional * cost_rate
```

`cost_bps_per_executed_notional` 的单位是每一单位成交名义金额的基点。ETF daily `volume` 和 `amount` 的源单位不参与 V1 成本计算，也不在本阶段推导市场冲击成本。

### 扣费时点与净收益

成本在 kernel turnover 行对应的 `observation_date` 扣除。当天净收益因子为：

```text
net_factor_t = (1 - cost_fraction_t) * (1 + gross_portfolio_return_t)
net_return_t = net_factor_t - 1
net_nav_t = previous_net_nav * net_factor_t
```

非调仓日 `cost_fraction_t = 0`。

本阶段继承现有 kernel 的权重生效日和收益时点语义，不在 evaluation 层重新解释成交时点。相关 caveat 必须写入报告。

首个调仓周期因初始建仓换手不可观测：

- `gross` 场景仍使用 `cost_fraction = 0`，并严格复现现有 kernel。
- 非零成本场景的首期 `cost_fraction` 记为 `null`，不是 `0`。
- 非零成本场景的净值计算不扣初始建仓成本。
- 非零成本场景的净指标明确标记 `excludes_initial_formation_cost`。
- 策略和 baseline 都使用同一处理方式。

### 最小报告合同

V1 不新增持久化 evaluation 数据集。CLI 从 frozen kernel 生成可归档 Markdown / JSON 报告。

建议命令：

```text
python -m tradepilot.etf_aw.cli backtest-robustness-report \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --format markdown|json
```

报告顶层：

```text
report_version
cost_model_name
cost_model_version
requested_range
comparable_range
coverage
strategies
comparisons
diagnostics
```

每条策略、每个场景至少输出：

- `gross_total_return`
- `net_total_return`
- `cost_drag`，口径为 gross total return 减 net total return。
- `net_annualized_return`
- `net_annualized_volatility`
- `net_sharpe_ratio`
- `net_max_drawdown`
- `average_turnover`
- `estimated_cost_fraction_sum`，口径为所有可观测调仓期 `cost_fraction` 之和，不等同于复利后的收益拖累。
- `initial_formation_cost_status`
- `turnover_basis`
- `diagnostics`

每个场景的 comparison 至少输出当前策略减 baseline 的：

- net total return diff。
- net annualized volatility diff。
- net Sharpe diff。
- net max drawdown diff。
- estimated cost fraction sum diff。

## Validation

### 覆盖测试

- 策略和 baseline 日期完全一致时通过。
- 任一方缺少交易日时阻断比较。
- 没有重叠日期时返回 blocking diagnostic。
- 状态分布和调仓周期数可从 deterministic fixture 重现。
- 不通过 forward-fill 或 inner join 隐藏缺失日期。

### 成本测试

- `gross` 场景的 NAV 和指标与现有 kernel 完全一致。
- 固定 gross return 和 turnover 时，更高成本场景的期末 NAV 不得更高。
- toy fixture 能精确验证 `2 * turnover * bps / 10000`。
- 成本只在 turnover 对应日期扣除一次。
- 非零成本场景的首期成本为 `null` 并产生 `excludes_initial_formation_cost` caveat；gross 场景仍精确复现 kernel。
- target 和 baseline 使用同一成本模型版本和场景集合。
- 除已显式标记的首期不可观测换手外，缺失、负数、非有限 turnover 或 `cost_fraction >= 1` 时阻断净指标。
- 多策略、多场景输出的业务键不会互相覆盖。

### 报告测试

- Markdown 和 JSON 包含相同策略、场景和 comparison。
- diagnostics 不为空时不能输出伪造的净指标。
- 所有百分比字段明确是 ratio 还是 percentage-point diff。
- report 中保留 `weight_source_type` 和 `source_weight_dataset`。

## 完成标准

Stage M 完成时应满足：

- 可比历史区间、周期数、状态分布和上游状态分布可复现。
- gross 场景严格复现 Stage L 结果。
- 固定 0 / 5 / 10 / 20 bps 场景能同时评估当前策略和 baseline。
- 初始建仓成本不可观测和 previous-target turnover caveat 不被隐藏。
- 正式 Markdown 报告作为 repo-visible artifact 归档。
- 不修改 frozen kernel、target weight 或 baseline weight。
- 不新增 API、前端或交易动作。

## 后续决策门

Stage M 报告完成后再决定是否设计 simplified ERC：

- 如果当前策略在扩展样本和多个成本场景下仍显示一致的风险调整后改善，可为 simplified ERC 单独建立 strategy version 和 frozen artifact 设计。
- 如果优势消失或只存在于少数月份，应优先检查 regime / risk-budget 映射、`partial` 状态和 confidence clamp，不通过增加优化器复杂度掩盖问题。
- 如果历史覆盖仍过短，应继续补 point-in-time 数据或积累 forward research observation，不从短样本推导模型优越性。

read model、API 和前端多策略展示仍后置到 evaluation 合同稳定之后。
