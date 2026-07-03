# ETF 全天候回测评估与可视化设计

## 目的

本文定义 `derived.etf_aw_backtest_kernel` 之后的回测评估层和前端可视化边界。

回测内核已经提供给定权重序列到净值、指标和换手的最小验收夹具。评估层在此基础上解决两个问题：

- 把内核输出整理成 read endpoint 和 Dashboard 可消费的合同。
- 在 target weight 稳定后，用同一个内核生成多基线对标、换手和权重漂移评估。

## 阶段边界

### Phase 1：单策略展示

Phase 1 只读取现有内核输出，不扩展策略逻辑。

输入：

- `derived.etf_aw_backtest_kernel`
- `observation_type` 包含 `daily_nav`、`metric`、`turnover`、`diagnostic`

输出：

- 后端 read endpoint，按 `strategy_name` / `strategy_version` 聚合。
- Dashboard 或独立 ETF all-weather backtest view 的单策略净值、回撤、指标和换手展示。

Phase 1 不生成新权重、不做基线对标、不引入成本模型。

### Phase 2：基线对标与评估层

Phase 2 在 `derived.etf_aw_target_weight` 能通过内核验收后实现。

输入：

- 当前策略 target weight 序列。
- 等权基线权重序列。
- 静态 inverse-vol 基线权重序列。
- 类风险平价基线权重序列。
- `derived.etf_aw_sleeve_daily`
- `reference.rebalance_calendar.monthly_post_20`

输出：

- 多 `strategy_name` / `strategy_version` 的内核结果。
- 基线指标对比表。
- 月度换手和成本敏感性。
- 日频有效权重输出，用于权重漂移展示。

## Phase 1 数据合同

### Read Endpoint

建议新增：

```text
GET /api/workflow/etf-aw/backtest
```

返回合同建议：

| 字段 | 含义 |
| --- | --- |
| `contract_version` | `etf_aw_backtest_view_contract_v1` |
| `strategy_name` | 内核输出中的策略名称 |
| `strategy_version` | 内核输出中的策略版本 |
| `date_range` | `start_date` / `end_date` |
| `daily_nav` | 日频净值和组合收益序列 |
| `drawdown` | 后端或前端由净值计算的 underwater 序列 |
| `metrics` | 指标字典 |
| `turnover` | 月度换手序列 |
| `diagnostics` | 输入 blocked 或 degraded 时的诊断 |

`daily_nav` 行来自：

| 内核字段 | 前端字段 |
| --- | --- |
| `observation_date` | `date` |
| `net_value` | `netValue` |
| `portfolio_return` | `portfolioReturn` |

`metric` 行来自：

| `metric_name` | 展示 |
| --- | --- |
| `annualized_return` | 年化收益 |
| `annualized_volatility` | 年化波动 |
| `sharpe_ratio` | Sharpe |
| `max_drawdown` | 最大回撤 |
| `total_return` | 累计收益 |
| `monthly_periods` | 月度期数 |

`turnover` 行：

- `metric_value` 是月度换手。
- `quality_notes_json.rebalance_date` 是调仓日期。

如果存在 `diagnostic` 行，read endpoint 不应合成净值或指标，应显式返回 `diagnostics.blocking = true` 和 `reasons`。

## Phase 1 前端需求

呈现走现有 React webapp：

- 页面放在 `webapp/src/pages`。
- API 类型和请求封装放在 `webapp/src/services/api.ts`。
- 风格与 Daily Workflow / Portfolio 保持一致。
- Vite proxy 继续通过 `/api` 访问后端。

### 净值曲线和回撤带

主图：

- 使用 `daily_nav.netValue` 画日频净值折线。
- x 轴为 `date`。

回撤：

- 用 `netValue / cumulativeMax(netValue) - 1` 计算 underwater 曲线。
- 高亮最大回撤区间。
- tooltip 展示开始日期、结束日期和最大回撤。

数值处理：

- 后端可在 read model 层对展示数值做 `round(6)`。
- 如果保持原始浮点，前端图表格式化时必须容忍尾差，避免浮点噪声造成视觉锯齿。

### 指标卡片

指标卡片展示：

- 年化收益。
- 年化波动。
- Sharpe。
- 最大回撤。
- 累计收益。
- 月度换手均值。

`sharpe_ratio` 可能为 `null`。前端必须展示 `N/A`，不能把 `null` 当作 `0`。

如果存在 blocking diagnostic，应在指标区顶部展示降级横幅，内容来自 `quality_notes_json.reasons`。

### 月度换手柱

换手柱状图：

- x 轴为 `quality_notes_json.rebalance_date`。
- y 轴为 `metric_value`。

Phase 1 只展示内核已有的 `previous_target_weight` 口径换手，并在 tooltip 中展示 `turnover_basis`。

## Phase 2 后端评估层

Phase 2 使用同一个内核纯函数分别运行多条权重序列：

- 当前策略。
- 等权。
- 静态 inverse-vol。
- 类风险平价。

每条结果必须有独立的：

- `strategy_name`
- `strategy_version`
- `quality_notes_json`

前端只按 `strategy_name` / `strategy_version` 分组，不理解策略生成逻辑。

## Phase 2 新增输出

为了支持 sleeve 权重漂移图，评估层应新增日频有效权重输出。

建议 observation type：

```text
effective_weight
```

粒度：

```text
strategy_name + strategy_version + observation_date + sleeve_code
```

建议字段：

- `observation_date`
- `sleeve_code`
- `sleeve_role`
- `target_weight`
- `effective_weight`
- `drift_basis`
- `quality_notes_json`

不建议前端用 `daily_nav.portfolio_return` 和 sleeve 收益反推 drifted weight。该反推在缺失收益、四舍五入和多解场景下不稳定，应由评估层显式输出。

## Phase 2 前端需求

基线对标：

- 多条净值曲线叠加。
- 当前策略、等权、静态 inverse-vol、类风险平价使用一致图例。
- 指标对比表每条策略一列。

权重漂移：

- 使用 `effective_weight` 输出画 sleeve 权重堆叠面积图。
- x 轴为 `observation_date`。
- y 轴为 `effective_weight`。

换手：

- 月度换手柱按策略分组或可切换。
- tooltip 展示调仓日、换手口径和质量说明。

## 非范围

本文不定义：

- risk budget 数值映射。
- target weight 优化器。
- 真实交易成本模型。
- 订单生成。
- broker / QMT / XtQuant 接口。
- 自动下单。

## 验证要求

Phase 1：

- read endpoint 能在只有等权内核 fixture 时返回非空 `daily_nav`、`metrics`、`turnover`。
- `sharpe_ratio = null` 时前端展示 `N/A`。
- diagnostic 行存在时前端展示降级横幅。
- 净值和回撤计算不依赖未来数据。

Phase 2：

- 多策略结果按 `strategy_name` 分组后指标互不串行。
- 基线权重序列都复用同一内核纯函数。
- 日频有效权重每个交易日、每条策略、每个 sleeve 都有一行。
- 成本和换手假设写入 `quality_notes_json`，不能隐藏在前端。
