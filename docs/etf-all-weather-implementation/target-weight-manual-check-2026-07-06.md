# ETF 全天候目标权重人工检查记录

检查日期：2026-07-06

检查对象：

- `derived.etf_aw_target_weight`
- `derived.etf_aw_backtest_kernel`

相关 notebook：

- `notebooks/etf-aw-target-weight-manual-check.ipynb`

## 数据生成

基于本地已有 lakehouse 数据，运行目标权重构建：

```bash
python - <<'PY'
from datetime import date
from tradepilot.etl.service import ETLService

service = ETLService()
print(service.run_bootstrap(
    "derived.etf_aw_target_weight.build",
    start=date(2025, 1, 1),
    end=date(2026, 5, 31),
))
PY
```

生成结果：

```text
status: success
records_written: 85
rebalance range: 2025-01-20 to 2026-05-20
partitions_written: 17
```

随后运行前置 backtest kernel：

```bash
python - <<'PY'
from datetime import date
from tradepilot.etl.service import ETLService

service = ETLService()
print(service.run_bootstrap(
    "derived.etf_aw_backtest_kernel.build",
    start=date(2025, 1, 1),
    end=date(2026, 5, 31),
))
PY
```

生成结果：

```text
status: success
records_written: 349
daily_nav: 326
turnover: 17
metric: 6
strategy_name: etf_aw_v1
strategy_version: target_weight_inverse_vol_v1
```

## 结构检查

`derived.etf_aw_target_weight` 当前字段数为 23，核心字段包括：

- `calendar_name`
- `rebalance_date`
- `effective_date`
- `strategy_name`
- `strategy_version`
- `sleeve_code`
- `sleeve_role`
- `risk_budget`
- `volatility_estimate`
- `volatility_floor`
- `raw_target_weight`
- `constrained_target_weight`
- `target_weight`
- `target_weight_status`
- `optimizer_name`
- `turnover_estimate`
- `quality_notes_json`
- `source_risk_budget_rebalance_date`
- `source_sleeve_daily_max_trade_date`

每个 `rebalance_date` 均有 5 行，覆盖：

```text
bond
cash
equity_large
equity_small
gold
```

## 数值检查

对所有 17 个 rebalance date 检查：

- 每期行数为 5。
- 每期 sleeve 数为 5。
- `sum(risk_budget) = 1.0`。
- `sum(raw_target_weight) = 1.0`。
- `sum(constrained_target_weight) = 1.0`。
- `sum(target_weight) = 1.0`。
- cash 权重未超过 `0.35`。
- 非 cash sleeve 权重未超过 `0.45`。

检查结果无违反项。

最新一期 `2026-05-20`：

| Sleeve | Code | Risk budget | Vol estimate | Raw weight | Constrained | Target | Status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| bond | `511010.SH` | 0.191630 | 0.005000 | 0.322609 | 0.322609 | 0.322609 | complete |
| cash | `159001.SZ` | 0.179075 | 0.005000 | 0.301472 | 0.301472 | 0.301472 | complete |
| equity_large | `510300.SH` | 0.220925 | 0.010376 | 0.179220 | 0.179220 | 0.179220 | complete |
| equity_small | `159845.SZ` | 0.220925 | 0.015425 | 0.120562 | 0.120562 | 0.120562 | complete |
| gold | `518850.SH` | 0.187445 | 0.020723 | 0.076137 | 0.076137 | 0.076137 | complete |

最新一期合计：

```text
risk_budget                  1.0
raw_target_weight            1.0
constrained_target_weight    1.0
target_weight                1.0
```

公式抽样复核：

```text
bond score = 0.191630 / 0.005 = 38.326
raw_target_weight = score_i / sum(score)
```

抽样结果与 `raw_target_weight = 0.322609` 一致。

## 状态检查

85 行目标权重状态分布：

```text
unavailable    75
complete       10
```

按 rebalance date 看：

```text
2025-01-20 to 2026-03-20: unavailable
2026-04-20: complete
2026-05-20: complete
```

这说明当前目标权重 artifact 的工程链路已跑通，但只有最近 2 个调仓月是真正 complete。此前月份是跟随上游 risk budget 的 `unavailable` 降级输出，不应作为正式策略结果评价。

## Backtest Kernel 检查

`derived.etf_aw_backtest_kernel` 已消费 `derived.etf_aw_target_weight`，不是等权 fixture：

```text
strategy_name: etf_aw_v1
strategy_version: target_weight_inverse_vol_v1
```

输出指标：

```text
total_return             0.214078
annualized_return        0.161778
annualized_volatility    0.065533
sharpe_ratio             2.468645
max_drawdown            -0.048230
monthly_periods         17
```

这些指标只证明 target weight artifact 可以被前置 backtest kernel 稳定消费。由于 17 个月中有 15 个调仓月是 `unavailable`，当前不能把这些指标解释为正式策略表现。

## 策略语义判断

当前数据有以下特征：

- `2026-05-20` 权重偏防御：bond + cash 约 62.41%，权益合计约 29.98%，gold 约 7.61%。
- bond 和 cash 的 `volatility_estimate` 均被 `volatility_floor = 0.005` 托底。
- V1 优化器是 `budgeted_inverse_vol`，不是完整风险平价，也不考虑协方差矩阵。
- `target_weight` 不保证真实风险贡献等于 `risk_budget`。
- `source_sleeve_daily_max_trade_date == rebalance_date`，表示当前权重依赖调仓日当日可见的 sleeve daily 数据。

## 结论

工程正确性：

- 表结构正确。
- 公式链正确。
- 权重合计正确。
- cap 约束正确。
- backtest kernel 可消费。

策略可用性：

- 当前 artifact 只能视为 V1 工程验收与观察结果。
- 只有 `2026-04-20` 和 `2026-05-20` 是 complete。
- 2025-01 到 2026-03 的 unavailable 结果不应纳入正式策略评价。

## 后续动作

优先级较高：

1. 检查上游 `derived.etf_aw_risk_budget` 为什么 2025-01 到 2026-03 为 `unavailable`。
2. 明确执行时点：如果用于调仓日收盘后或次日执行，`trade_date <= rebalance_date` 可接受；如果用于调仓日盘前执行，应改为 `trade_date < rebalance_date`。
3. 等 complete 月份积累足够后，再做正式 backtest evaluation。

优先级中等：

1. 评估 bond/cash 因 volatility floor 获得较高权重是否符合策略预期。
2. 继续保留 `budgeted_inverse_vol` 命名，避免把 V1 解释为完整风险平价。
3. 如需比较 ERC、静态 inverse-vol 或等权版本，应生成独立 `strategy_version` artifact 后进入同一 backtest kernel。
