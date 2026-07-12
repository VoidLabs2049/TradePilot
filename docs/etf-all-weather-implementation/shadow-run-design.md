# ETF 全天候 Stage O 模拟盘观察设计

## 目的

本文定义 Stage O 的最小实现合同：基于 Stage N 已冻结的 `DRAFT` 调仓计划，记录人工确认、模拟成交和每日账户快照，生成可审计的 forward observation 与阶段复盘。

Stage O 用于验证计划、模拟执行、持仓和绩效记录能否长期一致地闭环。它不证明策略有效，不连接券商，不产生真实委托，也不从短期模拟收益推导可实盘结论。

## 阶段边界

输入：

- Stage N frozen `derived.etf_aw_rebalance_plan`，以 `plan_id` 关联。
- 人工提供的模拟成交事件，包含成交时间、数量和价格。
- 每个观察日的 frozen 5-sleeve 收盘价。
- 初始账户现金与持仓，来自 Stage N 对应账户快照，并在 Stage O 启动时冻结为账户 seed。
- 同期 baseline 日收益或净值，用于只读相对表现比较。
- frozen `sse` 交易日历，用于检查观察日期和缺失交易日。
- 人工备注和异常说明。

输出：

- append-only `derived.etf_aw_paper_decision` 人工决策记录。
- append-only `derived.etf_aw_paper_fill` 模拟成交事件。
- 单次写入的 `derived.etf_aw_shadow_account_seed` 初始账户状态。
- append-only `derived.etf_aw_shadow_observation` 每日账户观察快照。
- 按 `plan_id` 生成的 Markdown / JSON post-mortem。
- 按账户和日期区间生成只读 `shadow-performance-report` HTML / JSON 报表。
- 计划状态、成交偏差、持仓、现金、净值、权重偏离和 forward performance diagnostics。

非范围：

- 不连接券商 API，不提交、撤销或查询真实订单。
- 不自动撮合，不估计盘口、成交概率、冲击成本或滑点。
- 不自动确认 Stage N 计划，不因成交不足自动补单。
- 不修改 Stage N frozen plan，不覆盖历史成交或每日观察。
- 不重新计算目标权重，不根据 forward 表现调参。
- 不建设 API、定时任务或前端绩效界面。
- 不处理分红、拆分、基金份额折算、税费和真实佣金；出现时阻断自动净值续算，人工修正合同留待后续版本。

## 关键假设

V1 冻结以下假设：

1. 单个 `plan_id` 只属于一个模拟账户和一个 strategy version。
2. 模拟成交由人工按真实可观察价格录入；系统不自行生成成交价。
3. BUY 成交金额直接减少现金，SELL 成交金额直接增加现金；V1 费用固定为 `0`，报告必须显示该 caveat。
4. 同一 symbol 的持仓成本不参与收益计算；组合收益只由每日总资产变化计算。
5. 每个观察日使用同一收盘时点的完整 5-sleeve 价格向量。
6. 每个账户每个观察日只允许一份 observation；已冻结记录不得原地修改。
7. baseline 只用于比较，不参与账户净值、现金或持仓计算。
8. 非交易日不生成 observation；跨周累计收益使用相邻 observation，而不是假设自然日收益为零。
9. 单个模拟账户只允许一个不可变 seed；V1 不支持重置账户、追加资金或提取资金。

## 状态模型

Stage N 的 `plan_status = DRAFT` 保持不变。Stage O 根据人工决策和 append-only fill 事件派生观察状态，不回写 Stage N artifact：

```text
DRAFT
  -> CANCELLED       人工明确取消，且尚无 fill
  -> CONFIRMED       人工确认，尚无 fill
  -> PARTIALLY_FILLED 已成交数量小于计划数量
  -> FILLED          所有 BUY / SELL 行均达到计划数量
```

约束：

- `HOLD` 行不要求 fill。
- 已有任一 fill 后不能派生为 `CANCELLED`；停止剩余模拟成交时派生为 `PARTIALLY_FILLED`，并在 post-mortem 记录原因。
- 单行累计成交数量不得超过 Stage N `order_quantity`。
- fill 的 `order_side` 必须与计划行一致，symbol 必须属于该 `plan_id`。
- 状态是报告层派生值，不新增可被原地更新的订单主表。
- `FILLED` 只表示计划成交完成；该计划的目标权重持续有效，直到同账户后续 confirmed plan 接替。
- `target_plan_id` 按 `decided_at` 选择观察时点前最新的 `CONFIRMED` plan；后续 plan 只有在确认后才接替旧目标。`CANCELLED` plan 不成为目标来源，V1 也不允许取消后用相同 Stage N 幂等键重建计划。

## 输入合同

### 人工决策文件

每个 plan 使用一份 repo-visible JSON：

| 字段 | 含义 |
| --- | --- |
| `plan_id` | Stage N 稳定计划标识 |
| `decision` | `CONFIRMED` 或 `CANCELLED` |
| `decided_at` | 人工决策时间 |
| `operator` | 操作者标识，不得包含 credential |
| `note` | 决策说明，可为空 |

决策只能记录一次。`CANCELLED` plan 不接受 fill；`CONFIRMED` 不代表策略判断正确，只代表允许记录模拟成交。

决策写入 append-only `derived.etf_aw_paper_decision`，粒度为 `plan_id`，V1 schema 为：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | `etf_aw_paper_decision_v1` |
| `contract_version` | `etf_aw_shadow_run_contract_v1` |
| `plan_id` | Stage N 稳定计划标识 |
| `account_id` | 来源账户 |
| `strategy_name` | 来源策略名称 |
| `strategy_version` | 来源策略版本 |
| `decision` | `CONFIRMED` 或 `CANCELLED` |
| `decided_at` | 人工决策时间 |
| `operator` | 操作者标识 |
| `note` | 决策说明 |
| `recorded_at` | artifact 写入时间 |

### 模拟成交文件

每条 fill 最少包含：

| 字段 | 含义 |
| --- | --- |
| `fill_id` | 调用方提供的稳定唯一标识 |
| `plan_id` | 来源计划 |
| `symbol` | ETF 代码 |
| `order_side` | `BUY` 或 `SELL` |
| `fill_at` | 模拟成交时间 |
| `fill_quantity` | 本次成交份数，正整数 |
| `fill_price` | 模拟成交价，单位 CNY/份，正数 |
| `source` | 价格或人工记录来源 |
| `note` | 可选异常说明 |

V1 允许一条计划行有多次 fill，用于记录部分成交；不允许负数 fill 或冲销事件。错误记录必须以新的修正流程处理，V1 CLI 直接阻断并要求重建尚未发布的本地输入。

### 每日价格快照

每个 `observation_date` 必须恰好覆盖 frozen 5-sleeve，每个 symbol 具有唯一、有限且大于零的 `close_price`。价格必须满足：

```text
price_trade_date = observation_date
price_as_of <= observation_generated_at
```

V1 不以前值填充缺失价格，也不使用未来交易日价格。

### 初始账户 Seed

首次生成 observation 前，Stage O 必须把 Stage N 对应账户快照冻结为 `derived.etf_aw_shadow_account_seed`。粒度为 `account_id + sleeve_role`，每个账户固定 5 行，最少包含：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | `etf_aw_shadow_account_seed_v1` |
| `contract_version` | `etf_aw_shadow_run_contract_v1` |
| `account_id` | 模拟账户 |
| `seed_at` | 初始状态时间 |
| `seed_date` | 初始估值交易日 |
| `source_plan_id` | 提供初始账户快照追溯的 Stage N plan |
| `cash` | 初始现金，CNY |
| `total_asset` | 初始账户总资产，CNY |
| `sleeve_role` | frozen sleeve |
| `symbol` | ETF 代码 |
| `quantity` | 初始持仓份数 |
| `market_value` | 初始持仓市值，CNY |
| `source_snapshot_path` | Stage N 账户快照 repo-visible 路径 |
| `recorded_at` | artifact 写入时间 |

现金和总资产在每行重复。seed 必须完整覆盖 frozen universe，满足 `cash + sum(market_value) = total_asset`（容忍度 `0.01 CNY`），且 `seed_at` 不晚于首个 fill 和首个 observation。`seed_date` 必须是 frozen `sse` 交易日，快照使用该日收盘估值，首个 observation 必须晚于 `seed_date`。已有 seed 时拒绝重建或覆盖。

### Baseline 输入

baseline 必须提供相同 `observation_date` 的日收益或净值，并携带 `strategy_name`、`strategy_version` 和来源 artifact 标识。缺失 baseline 不阻断账户 observation，但相对收益字段为空并输出 `missing_baseline_observation` warning。baseline version 在账户首个 observation 时冻结；后续不得在同一累计序列中切换版本。

## 模拟成交 Artifact

数据集名称：

```text
derived.etf_aw_paper_fill
```

粒度：

```text
fill_id
```

V1 schema：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | `etf_aw_paper_fill_v1` |
| `contract_version` | `etf_aw_shadow_run_contract_v1` |
| `fill_id` | 稳定唯一标识 |
| `plan_id` | Stage N 来源计划 |
| `account_id` | 来源账户 |
| `strategy_name` | 来源策略名称 |
| `strategy_version` | 来源策略版本 |
| `sleeve_role` | 来源 sleeve |
| `symbol` | ETF 代码 |
| `order_side` | `BUY` 或 `SELL` |
| `planned_quantity` | Stage N 计划数量 |
| `fill_at` | 模拟成交时间 |
| `fill_quantity` | 本次成交份数 |
| `fill_price` | 成交价，CNY/份 |
| `fill_notional` | `fill_quantity * fill_price`，CNY |
| `source` | 输入来源 |
| `note` | 人工备注 |
| `recorded_at` | artifact 写入时间 |

禁止字段：券商委托号、真实账户密钥、真实成交回报或任何“已向市场提交”的含义。

## 每日观察 Artifact

数据集名称：

```text
derived.etf_aw_shadow_observation
```

粒度：

```text
account_id + observation_date + sleeve_role
```

每个观察日固定写 5 行，并在每行重复账户级汇总字段，保持与当前 parquet dataset 模式一致。

V1 schema：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | `etf_aw_shadow_observation_v1` |
| `contract_version` | `etf_aw_shadow_run_contract_v1` |
| `account_id` | 模拟账户 |
| `observation_date` | 观察交易日 |
| `generated_at` | 生成时间 |
| `target_plan_id` | 当日仍生效的最新 confirmed plan；尚无 confirmed plan 时为空 |
| `strategy_name` | 当前策略名称 |
| `strategy_version` | 当前策略版本 |
| `sleeve_role` | frozen sleeve |
| `symbol` | ETF 代码 |
| `close_price` | 当日收盘价，CNY/份 |
| `quantity` | 收盘模拟持仓份数 |
| `market_value` | `quantity * close_price`，CNY |
| `actual_weight` | `market_value / total_asset` |
| `target_weight` | active plan 来源目标权重；无 plan 时为空 |
| `weight_drift` | `actual_weight - target_weight`；无 target 时为空 |
| `cash` | 收盘模拟现金，CNY |
| `total_asset` | `cash + sum(market_value)`，CNY |
| `daily_return` | 相邻 observation 总资产收益率 |
| `cumulative_return` | 相对首个 observation 总资产的累计收益率 |
| `baseline_daily_return` | 同日 baseline 收益，可为空 |
| `baseline_cumulative_return` | 从 shadow 起点链式累计，可为空 |
| `relative_cumulative_return` | shadow 累计收益减 baseline 累计收益，可为空 |
| `derived_plan_status` | 当日派生的 plan 状态，可为空 |
| `warnings_json` | 非阻断 diagnostics |
| `note` | 当日人工备注 |

## 计算规则

### 成交应用顺序

首日从 frozen seed 开始，后续日期从前一交易日 observation 开始。按 `fill_at`、`fill_id` 稳定排序应用 `(previous_observation_generated_at, current_price_as_of]` 内尚未计入 observation 的 fill：

```text
BUY:  quantity += fill_quantity
      cash -= fill_quantity * fill_price

SELL: quantity -= fill_quantity
      cash += fill_quantity * fill_price
```

必须满足：

- BUY 后 `cash >= 0`。
- SELL 后 `quantity >= 0`。
- 单行累计 fill 不超过计划数量。
- 同一 `fill_id` 不得重复。

任一条件失败时整日 observation 阻断，不写 partial rows。

### 账户估值与权重

```text
market_value_i = quantity_i * close_price_i
total_asset = cash + sum(market_value_i)
actual_weight_i = market_value_i / total_asset
weight_drift_i = actual_weight_i - target_weight_i
```

现金不作为第六个 sleeve。报告单独显示 `cash_weight = cash / total_asset`，并校验：

```text
sum(actual_weight_i) + cash_weight = 1
```

容忍度固定为 `1e-6`。

### Forward performance

seed 总资产是净值基准；首个 observation 也必须计算 seed 到首日收盘的收益：

```text
daily_return_t = total_asset_t / total_asset_previous - 1
cumulative_return_t = total_asset_t / seed_total_asset - 1
baseline_cumulative_return_t = product(1 + baseline_daily_return) - 1
relative_cumulative_return_t = cumulative_return_t - baseline_cumulative_return_t
```

首日的 `total_asset_previous = seed_total_asset`，不得硬编码 `daily_return = 0`。该收益口径假设没有外部现金流；V1 出现充值、提现、分红或费用时阻断，不能把现金流误计为投资收益。

baseline 累计收益只在从首日开始连续完整的日期链上计算。任一中间交易日缺失 baseline 后，当日及后续累计与相对字段均为 `null`，直到重新生成覆盖完整区间的报告；不得跳过缺失日继续连乘。单日 baseline 恢复后仍可记录 `baseline_daily_return`。

### 成交偏差

post-mortem 对每条 BUY / SELL 计划行计算：

```text
fill_ratio = cumulative_fill_quantity / planned_quantity
volume_weighted_fill_price = sum(fill_quantity * fill_price) / cumulative_fill_quantity
price_deviation = (volume_weighted_fill_price / planned_latest_price - 1) * side_sign
```

其中 BUY `side_sign = 1`，SELL `side_sign = -1`；正值表示相对 Stage N 估值价格更不利。没有 fill 时相关字段为空，不填零。

### Sleeve 收益贡献

HTML 报表中的分类资产收益贡献必须使用前一观察日收盘后的实际权重；首日使用 seed 的 `market_value / total_asset`，避免使用当日收盘权重产生前视偏差：

```text
sleeve_return_i,t = close_price_i,t / close_price_i,t-1 - 1
sleeve_contribution_i,t = actual_weight_i,t-1 * sleeve_return_i,t
cash_contribution_t = 0
portfolio_return_from_contributions_t = sum(sleeve_contribution_i,t)
```

在无费用、外部现金流和盘中成交的完整观察日，贡献之和应与组合日收益一致。发生当日 fill 时，V1 无法仅凭收盘 observation 精确拆分盘中持仓路径；该日 contribution 标记为不可用并输出 `intraday_fill_attribution_unavailable`，不使用近似值填充。

累计贡献采用各日 contribution 算术累加，仅解释“各 sleeve 对组合日收益的贡献累计”，不等同于各资产自身累计收益，也不要求与组合几何累计收益严格相加。HTML 图例和 tooltip 必须明确该口径。

## 只读绩效报表

报表命令只读取 frozen plan、decision、fill 和 observation artifact，不生成新 observation，不修改状态，也不触发调仓。输出：

```text
shadow-performance-report-{account_id}-{start_date}-{end_date}.html
shadow-performance-report-{account_id}-{start_date}-{end_date}.json
```

HTML 必须是可离线打开的自包含文件。JSON 保存同一指标、序列和 diagnostics，作为可复核的机器可读结果。报表顶部固定显示：模拟盘、观察区间、观察交易日数、strategy version、baseline version、零费用假设、research-only 和未连接券商。

报表区间收益必须以 `start_date` 前一条 observation 的总资产为基准重新计算；若 `start_date` 是账户首日，则使用 seed。不得直接把 artifact 中“自 seed 累计”的 `cumulative_return` 当作任意子区间收益。shadow 与 baseline 必须使用同一组完整日期；baseline 缺口后的相对曲线保持为空，并在完整性区块列出缺口。

### 核心区块

参考报表对应内容冻结为：

1. 绩效指标摘要：初始/最终总资产、累计收益、年化收益、年化波动率、最大回撤、Sharpe、Calmar、正收益日占比和日收益盈亏比。
2. 净值与风险图：shadow 与 baseline 累计收益、相对累计收益、drawdown、日收益分布、60 个观察日滚动年化波动率。
3. Sleeve 收益贡献：5 个 sleeve 的累计贡献、贡献合计和组合累计收益，使用前述 lagged actual weight 口径；首日价格由 seed 的非零持仓 `market_value / quantity` 推导，零持仓贡献固定为零，无法推导时从第二个 observation 开始。
4. 数据与口径说明：样本数、缺失 baseline 日期、不可归因成交日、warnings 和公式说明。

指标公式：

```text
period_return = ending_asset / pre_start_asset - 1
annualized_return = (1 + period_return) ** (252 / return_interval_count) - 1
annualized_volatility = std(daily_return, ddof=1) * sqrt(252)
drawdown_t = total_asset_t / cumulative_max(total_asset)_t - 1
max_drawdown = min(drawdown_t)
sharpe = mean(daily_return) / std(daily_return, ddof=1) * sqrt(252)
calmar = annualized_return / abs(max_drawdown)
positive_day_ratio = count(daily_return > 0) / count(non_initial_daily_return)
daily_profit_loss_ratio = mean(daily_return where > 0) / abs(mean(daily_return where < 0))
```

无风险利率 V1 固定为 `0`。分母为零、缺少正/负收益样本或样本不足时对应指标为 `null`，不得显示为 `0` 或无穷大。年化指标少于 60 个收益区间时仍可计算，但必须显示 `short_observation_window`；少于 20 个收益区间时不计算 Sharpe、Calmar 和滚动波动率。

### 补充区块

除三张参考图外，V1 建议增加以下高价值报表：

- 权重偏离报告：每日 target/actual weight、cash weight、每个 sleeve 的 `weight_drift`、最大绝对偏离及超过 2% 阈值的日期。该区块只观察，不产生调仓建议。
- 模拟成交质量报告：计划数量、累计成交数量、fill ratio、成交均价、相对 Stage N 估值价格偏差、确认到首笔/全部成交耗时和未成交原因。
- 观察完整性报告：按 frozen `sse` 交易日历列出预期交易日、已记录日期、缺失 observation、缺失 baseline、不可归因成交日、异常备注和 reason/warning code 汇总。
- 调仓周期表：按 `plan_id` 汇总目标权重日期、确认状态、最终派生状态、计划前后总资产、周期收益、baseline 周期收益和相对收益。
- 月度收益热力表：样本覆盖完整的自然月展示 shadow、baseline 和相对月收益；不完整月份明确标记 `partial`，不与完整月份混用。

V1 不增加因子归因、Brinson 选择效应、VaR、压力测试或收益预测。这些需要额外基准权重、盘中持仓路径、统计假设或更长样本，超出当前 observation 合同。

## 阻断与提醒

### 阻断原因

| reason code | 条件 |
| --- | --- |
| `missing_plan` | `plan_id` 不存在或不是完整 frozen Stage N plan |
| `missing_or_duplicate_seed` | 账户缺少唯一、完整的 frozen seed，或尝试覆盖已有 seed |
| `duplicate_decision` | plan 已有人工决策 |
| `fill_before_confirmation` | 未确认或已取消的 plan 录入 fill |
| `invalid_fill` | fill 数量、价格、方向、symbol 或时间非法 |
| `duplicate_fill` | `fill_id` 已存在 |
| `fill_exceeds_plan` | 累计 fill 超过计划数量 |
| `insufficient_shadow_cash` | 应用 BUY fill 后现金为负 |
| `insufficient_shadow_position` | 应用 SELL fill 后持仓为负 |
| `missing_or_invalid_close_price` | 当日价格未完整覆盖 5-sleeve |
| `duplicate_observation` | 账户当日 observation 已存在 |
| `observation_date_regression` | 日期不晚于该账户最新 observation |
| `invalid_observation_date` | observation date 不是 frozen `sse` 交易日 |
| `baseline_version_mismatch` | baseline version 与账户首日冻结版本不同 |
| `unexpected_external_cash_flow` | 发现无法由 fill 解释的现金变化 |
| `unsupported_corporate_action` | 出现 V1 不支持的公司行动或份额调整 |

阻断时命令返回失败，不追加任何 fill 或 observation 行，并把结构化 diagnostics 输出到标准错误。

### 非阻断提醒

- `partial_fill`：至少一条计划行尚未完全成交。
- `no_fill_for_confirmed_plan`：已确认但尚无成交。
- `missing_baseline_observation`：当日无同口径 baseline。
- `large_weight_drift`：任一 sleeve `abs(weight_drift) > 0.02`；阈值仅用于观察，不触发交易。
- `stale_active_plan`：确认后超过 5 个交易日仍未完成模拟成交。
- `zero_fee_assumption`：V1 未计费用。
- `research_only_strategy`：短样本 forward observation 不证明策略有效。
- `intraday_fill_attribution_unavailable`：当日发生 fill，缺少盘中持仓路径，无法精确计算 sleeve contribution。
- `short_observation_window`：收益区间少于 60 个，年化指标解释力有限。

提醒不得自动生成新计划、fill 或 target weight。

## 幂等与可追溯性

- decision 幂等键为 `plan_id`，并冻结在 `derived.etf_aw_paper_decision`。
- fill 幂等键为 `fill_id`。
- observation 幂等键为 `account_id + observation_date`。
- seed 幂等键为 `account_id`。
- 所有 artifact 只追加，不原地覆盖。
- 每日 observation 必须记录价格输入路径、前一 observation 业务键、应用的 fill id 列表和 baseline 来源；这些追溯字段写入同行 JSON review metadata，不扩张 parquet V1 schema。
- post-mortem 必须列出 plan、decision、全部 fill、观察日期范围、缺失日、warnings 和人工备注。

## CLI 设计

建议新增四个独立命令：

```text
python -m tradepilot.etf_aw.cli initialize-shadow-account \
  --plan-id PLAN_ID \
  --account-snapshot path/to/account-snapshot.json

python -m tradepilot.etf_aw.cli record-paper-decision \
  --decision path/to/decision.json

python -m tradepilot.etf_aw.cli record-paper-fill \
  --fill path/to/fill.json

python -m tradepilot.etf_aw.cli build-shadow-observation \
  --account-id ACCOUNT_ID \
  --observation-date YYYY-MM-DD \
  --price-snapshot path/to/close-price.json \
  --baseline-observation path/to/baseline.json \
  --note path/to/note.txt \
  --output-dir path/to/output
```

post-mortem 使用只读命令：

```text
python -m tradepilot.etf_aw.cli shadow-post-mortem \
  --plan-id PLAN_ID \
  --output-dir path/to/output
```

只读绩效报表命令：

```text
python -m tradepilot.etf_aw.cli shadow-performance-report \
  --account-id ACCOUNT_ID \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --output-dir path/to/output
```

V1 不提供 `--broker`、`--submit`、`--auto-fill`、`--retry-order` 或按漂移自动调仓的参数。

## 实现切分

实现保持四个最小边界：

1. 输入模型与合同校验：seed、decision、fill、价格、交易日历和 baseline。
2. 纯函数成交归并：校验累计成交并派生 plan 状态。
3. 纯函数每日估值：从前一 observation、当日 fill 和完整价格生成 5 行 observation。
4. CLI 与 artifact 写出：负责读取、幂等检查、原子追加和审阅报告。

不引入通用 OMS、broker adapter、事件总线、数据库迁移框架或前端状态管理。

## 验证要求

### 单元测试

至少覆盖：

- confirmed plan 无 fill 时派生 `CONFIRMED`。
- 单次和多次 fill 正确派生 `PARTIALLY_FILLED` / `FILLED`。
- HOLD 行不要求 fill。
- fill 方向、symbol、数量、价格或时间非法时阻断。
- fill 超计划、BUY 导致负现金或 SELL 导致负持仓时阻断。
- 取消后 fill 和成交后取消均阻断。
- 5-sleeve 收盘价缺失、重复、非正或日期错误时阻断。
- 成交按稳定顺序应用，现金和数量结果确定。
- 市值、现金、总资产、实际权重和权重偏离公式正确。
- seed、首日、次日和累计收益计算正确，首日收益不被硬编码为零，且无未来数据。
- baseline 缺失时账户 observation 成功且相对字段为空。
- baseline 中间缺失或版本变化时不跨缺口续算累计相对收益。
- 相同输入重复计算得到相同业务字段和派生状态。

### CLI 与 Artifact 测试

至少覆盖：

- 合法 seed、decision 和 fill 各追加一次，重复键不覆盖。
- 合法观察日生成 schema 完整的 5 行 artifact、JSON review 和终端摘要。
- 任一阻断条件下不写 partial artifact。
- 第二个观察日只应用尚未计入历史 observation 的 fill。
- post-mortem 的累计成交、成交均价、价格偏差和观察收益与 frozen artifact 一致。
- performance report 的 HTML 与 JSON 使用相同指标和时间序列，且 HTML 可离线打开。
- 累计收益、回撤、滚动波动率、收益分布和指标摘要通过 deterministic fixture 验证。
- 无成交日 sleeve contribution 之和复现组合日收益；成交日标记不可归因，不静默近似。
- 权重偏离、成交质量、观察完整性、调仓周期和月度收益区块均可追溯到 frozen artifact。
- 短样本、零波动、无亏损日和 baseline 缺失不会产生无穷大或误导性零值。
- 输出明确显示“模拟盘、零费用假设、research-only、未连接券商”。

## 完成标准

Stage O 完成时必须同时满足：

- shadow account seed、paper decision、paper fill 和 daily observation 合同已实现并冻结版本。
- Stage N plan 保持不可变，所有执行与观察状态可由 append-only 记录重建。
- 模拟成交不会超过计划、现金或持仓约束，失败时不产生 partial artifact。
- 每个观察日可从前一快照、当日新增 fill 和完整收盘价确定性重建。
- 持仓、现金、净值、目标偏离、累计收益和 baseline 相对收益可追溯。
- post-mortem 能解释未成交、部分成交、成交价格偏差、观察缺口和人工异常。
- 只读 HTML / JSON performance report 能复核绩效、风险、收益贡献、权重偏离、成交质量和数据完整性，且不会修改 artifact。
- 没有券商连接、真实下单、自动成交、API 或前端写操作。

完成以上条件只代表 shadow portfolio 的工程观察闭环成立。进入小资金 live pilot 必须另行设计真实账户隔离、授权、费用、成交回报、公司行动、资金流、风控和紧急停止机制。
