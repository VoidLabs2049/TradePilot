# ETF 全天候完整回测流程

## 目的

本文记录当前已落地的端到端回测流程,作为背景说明。它描述数据如何从原始行情一路流到可对比的回测指标,以及流程中已知的口径与 caveat。

本文只描述现状,不定义新能力。成本敏感性与稳健性评估见 `backtest-robustness-evaluation-design.md`。

## 阶段总览

整个回测是一条 CLI 驱动的冻结管线(frozen pipeline)。每一步产物写进 lakehouse(DuckDB + partitioned dataset),下游只读上游冻结产物,没有单体黑盒。

```
sync-data ──► build-risk-budget ──► build-target-weight ─┐
     │                                                     ├─► backtest-kernel ──► backtest-report
     └──────────────────────────► build-baseline-weight ──┘
```

对应 CLI(`tradepilot/etf_aw/cli.py`):

| 命令 | 产物数据集 | 作用 |
| --- | --- | --- |
| `sync-data` | `derived.etf_aw_sleeve_daily` | 5 个 sleeve 的日频调整收益面板 |
| `build-risk-budget` | `derived.etf_aw_risk_budget` | regime → 各 sleeve 风险预算 |
| `build-target-weight` | `derived.etf_aw_target_weight` | 当前策略月度权重 |
| `build-baseline-weight` | `derived.etf_aw_baseline_weight` | `static_inverse_vol_v1` 基线权重 |
| `backtest-kernel` | `derived.etf_aw_backtest_kernel` | 日频 NAV / turnover / metric / diagnostic 行 |
| `backtest-report` | (stdout) | 从 kernel 聚合出多策略对比报告 |

## 收益面板构建

`_make_etf_aw_sleeve_daily_frame`(`tradepilot/etl/service.py:1642`):

- `daily` 与 `adj`(`market.etf_adj_factor`)按 `instrument_id + trade_date` 做 **inner join**。
- `adj_close = close * adj_factor`,收益 `adj_pct_chg = groupby(instrument).pct_change() * 100`。
- 即调整后收盘价的全收益近似,分红折进 `adj_factor`。

注意:收益按**相邻可用观测**计算。单个 sleeve 缺行(停牌/数据缺失)时,跨越缺口的多日累计收益会被记到复牌当天,当成一天收益。

## Kernel 构建

`_build_etf_aw_backtest_kernel`(`service.py:2627`)是回测真正发生的地方。

### 1. 选权重来源

`--strategy target-weight` 或 `baseline`。baseline 分支按 `baseline_name/version` 过滤,并把 `baseline_name` 改写成 `strategy_name`(`service.py:2713`),让两条策略走**完全相同的下游代码路径** —— 这是可比性的基础。

### 2. 读三个冻结输入

`_read_rebalance_calendar`、`derived.etf_aw_sleeve_daily`、对应权重数据集(`service.py:2690-2725`)。任一为空直接 `_failed`,不静默降级。

### 3. 输入诊断 gate

`_backtest_input_diagnostics`(`service.py:6190`)在跑净值前做 blocking 校验:缺 sleeve 收益、调仓日不落在交易日、权重列缺失、权重行重复、缺 sleeve 权重、权重和不为 1(容差 `1e-6`)。任一命中 → 只产出 diagnostic 行,不产净值。

诊断只检查"每个交易日 5 个 sleeve 是否齐全",**不检查单个 sleeve 的日期是否连续**。

### 4. 日频 NAV 主循环

`_make_etf_aw_backtest_kernel_frame`(`service.py:6088-6163`):

- 面板 pivot 成 `date × sleeve_code` 日收益矩阵,`adj_pct_chg / 100`,NaN 填 `0.0`(`service.py:6043-6054`)。
- `effective_dates` 游标推进权重生效日:`effective_dates[i+1] <= trade_date` 才切换权重(`service.py:6097-6101`)。**权重当日生效,无 T+1 延迟。**
- 切权重那天记一条 **turnover 行**:`0.5 * Σ|new − prev|`,首期 `previous_target is None → 0.0`(`service.py:6107-6115`),basis 标注 `previous_target_weight`。
- 每个交易日记一条 **daily_nav 行**,`portfolio_return = Σ w_i · r_i`,用的是切换后的当日权重(`service.py:6136-6144`);`nav *= 1 + return`。
- `current_weight` 每个调仓日只设一次、整月复用,等价于隐含日度再平衡。

### 5. 聚合指标

`_backtest_metric_values`(`service.py:6375`)序列跑完后一次性算 5 个指标,写成 metric 行:

| 指标 | 公式 |
| --- | --- |
| `total_return` | `final_nav − 1` |
| `annualized_return` | `final_nav^(252/N) − 1`(终值年化,非日均) |
| `annualized_volatility` | `std(daily, ddof=1) · √252` |
| `sharpe_ratio` | `ann_ret / ann_vol`(rf=0;vol=0 → null) |
| `max_drawdown` | `min(nav / cummax − 1)` |

### 6. 输出校验后写盘

`_validate_backtest_kernel_frame` 全通过才写 `derived.etf_aw_backtest_kernel`(`service.py:2733-2739`)。

## 报告

`backtest-report`(`cli.py:389`)纯读 kernel,按 `(weight_source_type, strategy)` 分组产出多策略对比(`_single_backtest_report`)。**无交易成本** —— 这是 Stage L 现状,也是 robustness 评估阶段要补的缺口。

## 关键设计选择

1. **Gross-only**:kernel 全程不扣成本,turnover 只记录不作用于 NAV。成本是评估层才叠加。
2. **Previous-target turnover**:换手用"上期目标权重"而非"调仓前 drifted 权重",系统性低估真实换手;限制写进每行 `turnover_basis`。
3. **首期建仓不可观测**:第一期无前值,kernel 记 `0.0` —— 不是"零成本建仓",而是无法观测。
4. **策略与基线共用代码路径**:baseline 改写为 strategy 后走同一循环,保证可比。

## 已知 caveat

以下为已识别、当前未在代码层完全捕获的限制,评估层应显式标注:

- **sleeve 日期连续性未被诊断捕获**:单 sleeve 缺口会被合并成多日收益(见收益面板构建),而输入诊断只查每日 sleeve 齐全性。
- **净值侧隐含日度再平衡,与月度 turnover 口径不自洽**:净值假设每日拉回目标权重,turnover 只在调仓日算一次;月中 drift 收益与 drift 后调仓换手都未建模。
- **成交时点**:权重当日生效,无 T+1 成交延迟、无滑点;换仓日 gross 收益已按新权重结算。
- **`risk_free_rate = 0`**:Sharpe 绝对值受影响,策略间 diff 影响较小。
- **无外部对照基准**:只与 `static_inverse_vol_v1` 比,无买入持有 60/40 或单一宽基基准。
- **短样本**:当前正式报告仅覆盖 17 个调仓周期,regime 可能高度集中。
