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

## 实现假设与成功信号

V1 固定以下假设，不留给实现时临时选择：

- 评估对象是一个明确的当前策略版本和一个明确的 baseline 版本，不自动挑选历史表现最好的版本。
- 日期参数是包含首尾日的请求区间；实际计算只能使用该区间内通过覆盖审计的共同日期。
- 成本模型只对 kernel 已记录的可观测 turnover 做研究敏感性扣减，不推断首期建仓、真实佣金、滑点或冲击成本。
- Stage M 只读 frozen artifact，不写回 lakehouse；唯一新增产物是可归档报告文件。

实现成功的最小信号是：同一组 frozen 输入可重复生成字节级稳定的 JSON 业务内容，`gross` 指标与 kernel 完全一致，任一阻断条件都不会留下可被误读为有效结论的 net comparison。

## 阶段边界

本阶段只补齐“回测结果能否被可靠解释”的评估层。会议中提到的“权重转换为真实订单”属于后续 Stage N，不应塞进 Stage M 实现里，否则会把研究口径、账户状态和交易执行混在一起，降低本周交付的可验收性。

当前仓库不是从零开始：target weight、baseline weight、backtest kernel 和多策略 `backtest-report` 已存在。短期瓶颈应定义为两个缺口：

1. 缺少基于 frozen kernel 的成本敏感性和覆盖质量报告。
2. 缺少从最新权重、持仓、现金和价格生成模拟盘订单草案的模块。

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

请求区间不得自动扩展。若请求起点早于可比起点或请求终点晚于可比终点，报告保留 requested/comparable 两组日期并输出 warning；只有共同区间为空或区间内部日期集合不一致时才 FAIL。

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
| `sleeve_return_gaps` | 单个 sleeve 收益日期不连续的缺口 |
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
- 任一 sleeve 在可比区间内存在收益日期缺口。该缺口不能通过策略间 `missing_trade_dates` 检出，报告必须列出 sleeve、缺口起止日期和缺失交易日数量。

V1 不为“样本足够长”设置任意自动阈值。报告必须给出精确周期数和状态分布，由后续研究评审判断是否足以支持策略结论。

### 阻断后的输出行为

覆盖 FAIL 时仍生成 coverage 和 diagnostics，便于定位输入问题，但行为必须固定为：

- CLI 以非零状态退出。
- JSON 中 `report_status = "blocked"`。
- `strategies[*].scenarios[*]` 的 net NAV 与 net 指标均为 `null`。
- `comparisons` 为空列表，不生成部分区间或单边策略比较。
- Markdown 首屏显示阻断原因，不展示收益优劣结论。

只有 coverage 无 FAIL 时，`report_status = "complete"`，才允许生成全部场景和 comparisons。WARN 不阻断计算，但必须随报告保留。

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

同时，现有 kernel 的净值侧按当期目标权重计算每日组合收益，隐含日度再平衡；而 turnover 只按月度 previous-target 差异计算。两者不自洽，月中 drift 收益和 drift 后调仓换手均未建模。该限制必须与 turnover basis 一并写入 diagnostics。

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

本阶段继承现有 kernel 的权重生效日和收益时点语义，不在 evaluation 层重新解释成交时点。权重在生效日当日生效，不模拟 T+1 成交延迟、滑点或冲击成本；换仓日 gross 收益已按新权重结算。相关 caveat 必须写入报告。

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
  --strategy-name etf_all_weather \
  --strategy-version VERSION \
  --baseline-name static_inverse_vol_v1 \
  --baseline-version VERSION \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --format markdown|json \
  --output PATH
```

四个身份参数必须解析到唯一的 `calendar_name + strategy_name + strategy_version + weight_source_type + source_weight_dataset` 组合。零个或多个匹配都应阻断，不能默认取最新版本，也不能把多个版本聚合为一条曲线。`--output` 必填，命令不得只把正式报告打印到终端；同一命令、输入 artifact 和日期范围应覆盖生成相同业务内容。

报告顶层：

```text
report_version
report_status
generated_at
cost_model_name
cost_model_version
input_identities
requested_range
comparable_range
coverage
strategies
comparisons
diagnostics
```

`generated_at` 是唯一允许随运行变化的元数据字段，不参与报告业务内容一致性比较。`input_identities` 必须记录两条曲线的 calendar、strategy、version、weight source、source dataset，以及输入 artifact 可获得的 `ingested_at` / source date，保证报告可追溯。

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

净指标必须复用 kernel 指标口径：`net_annualized_return = final_nav ** (252 / daily_observation_count) - 1`，`net_annualized_volatility = std(net_return, ddof=1) * sqrt(252)`，`net_sharpe_ratio = net_annualized_return / net_annualized_volatility`。Sharpe 的 `risk_free_rate = 0` 必须在报告中显式标注。

每个场景的 comparison 至少输出当前策略减 baseline 的：

- gross total return diff。
- gross annualized volatility diff。
- gross Sharpe diff。
- gross max drawdown diff。
- net total return diff。
- net annualized volatility diff。
- net Sharpe diff。
- net max drawdown diff。
- estimated cost fraction sum diff。

所有 `*_diff` 固定为 `current_strategy - baseline`。收益和回撤 diff 的展示单位为 percentage point，JSON 保留 ratio；Sharpe diff 保留无量纲数值。`gross` 场景下 net 字段不得复制一套含义不清的值：`net_*` 统一等于对应 gross 值，并设置 `metric_basis = "gross"`；非零成本场景设置 `metric_basis = "net_excludes_initial_formation_cost"`。

## Validation

### 覆盖测试

- 策略和 baseline 日期完全一致时通过。
- 任一方缺少交易日时阻断比较。
- 没有重叠日期时返回 blocking diagnostic。
- 状态分布和调仓周期数可从 deterministic fixture 重现。
- 不通过 forward-fill 或 inner join 隐藏缺失日期。
- 单个 sleeve 存在收益日期缺口时输出 `sleeve_return_gaps` warning，缺口不会被静默合并进净值结论。

### 成本测试

- `gross` 场景的 NAV 和指标与现有 kernel 完全一致。
- `gross` 和 net 场景的年化收益、年化波动和 Sharpe 复用 kernel 的终值年化、`ddof=1` 波动率和 `risk_free_rate = 0` 口径。
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
- 策略身份没有匹配或匹配多个版本时阻断，不默认选择 latest。
- coverage FAIL 时 CLI 非零退出、net 指标为 `null` 且 comparisons 为空。
- 除 `generated_at` 外，同一输入重复生成的 JSON 业务内容一致。

## 完成标准

Stage M 完成时应满足：

- 可比历史区间、周期数、状态分布和上游状态分布可复现。
- gross 场景严格复现 Stage L 结果。
- 固定 0 / 5 / 10 / 20 bps 场景能同时评估当前策略和 baseline。
- 初始建仓成本不可观测、sleeve 收益日期缺口、previous-target turnover、隐含日度再平衡和成交时点 caveat 不被隐藏。
- 正式 Markdown 报告作为 repo-visible artifact 归档。
- 不修改 frozen kernel、target weight 或 baseline weight。
- 不新增 API、前端或交易动作。

## 后续决策门

Stage M 报告完成后再决定是否设计 simplified ERC：

- 如果当前策略在扩展样本和多个成本场景下仍显示一致的风险调整后改善，可为 simplified ERC 单独建立 strategy version 和 frozen artifact 设计。
- 如果优势消失或只存在于少数月份，应优先检查 regime / risk-budget 映射、`partial` 状态和 confidence clamp，不通过增加优化器复杂度掩盖问题。
- 如果历史覆盖仍过短，应继续补 point-in-time 数据或积累 forward research observation，不从短样本推导模型优越性。

read model、API 和前端多策略展示仍后置到 evaluation 合同稳定之后。

## 并行交付附录：Stage M + Stage N

本节只说明 Stage M 与订单模块的排期和接口衔接，不扩大 Stage M 的代码范围。Stage N/O 的正式 schema、状态机和测试应分别落在 `rebalance-plan-design.md` 与 `shadow-run-design.md`；两份文档未完成前，以本节的最小边界推进，不在 Stage M 模块中实现订单逻辑。

### 目标

以 2026-07-13 周一能启动模拟盘观察为目标，周末只补齐最小闭环：

```text
frozen target weight
  -> Stage M: backtest robustness report
  -> Stage N: rebalance plan
  -> paper order draft
  -> manual approval / simulated fill
  -> daily observation record
```

这里的“启动模拟盘”定义为生成可审计的订单草案和模拟成交记录，不定义为自动连接券商下单，也不作为策略持续赚钱的证明。

### 状态校准

会议小结中“回测系统尚未搭建”的表述需要修正。仓库已有 `derived.etf_aw_backtest_kernel` 和 `backtest-report`，可以支持当前策略与 baseline 的 gross 对比。真正还未补齐的是：

- Stage M：覆盖审计、成本敏感性、gross / net 统一报告。
- Stage N：目标权重到持仓订单的转换。
- Stage O：模拟盘每日记录、归因和验证报告。

因此开发优先级不应回到重写回测内核，而应在现有 kernel 之上补评估层和订单草案层。

### Stage M 交付项

Stage M 按本文前述设计实现，交付物为：

- `backtest-robustness-report` CLI。
- Markdown / JSON 稳健性报告。
- 0 / 5 / 10 / 20 bps 成本网格。
- 当前策略减 baseline 的 gross / net 差异。
- sleeve 收益缺口、短样本、首期建仓成本不可观测、previous-target turnover 和隐含日度再平衡 caveat。

验收标准：

- gross 场景严格复现现有 kernel 指标。
- 任一策略缺交易日时阻断净指标比较。
- 同一成本场景下 current 和 baseline 使用完全相同公式。
- 报告能解释为什么当前 17 个调仓周期不能直接证明策略优越。

### Stage N：订单生成最小设计

Stage N 只负责从目标权重生成模拟盘订单草案，不负责策略研究、不负责回测、不负责自动成交。

输入：

- 最新完整 5-sleeve target weight。
- 当前持仓：`symbol`、`quantity`、`available_quantity`、`market_value`、`cost_basis`。
- 当前现金和账户总资产。
- 最新可用 ETF 价格。
- ETF 最小交易单位，A 股 ETF V1 按 100 份一手处理。
- 现金缓冲比例，V1 固定为 `cash_buffer_ratio = 0.01`。

输出建议使用 repo-visible artifact：

```text
derived.etf_aw_rebalance_plan
```

每行最少字段：

| 字段 | 含义 |
| --- | --- |
| `plan_date` | 计划生成日期 |
| `target_weight_rebalance_date` | 来源权重调仓日期 |
| `strategy_version` | 策略版本 |
| `sleeve_role` | sleeve |
| `symbol` | ETF 代码 |
| `target_weight` | 目标权重 |
| `current_quantity` | 当前持仓数量 |
| `latest_price` | 最新价格 |
| `target_notional` | 目标市值 |
| `raw_delta_quantity` | 未取整的差额份额 |
| `order_side` | `BUY` / `SELL` / `HOLD` |
| `order_quantity` | 按交易单位取整后的订单数量 |
| `estimated_notional` | 预计成交名义金额 |
| `cash_buffer_ratio` | 现金缓冲比例 |
| `plan_status` | Stage N 固定为 `DRAFT`；确认、模拟成交和取消状态属于 Stage O |
| `blocking_reasons` | 阻断原因 |
| `warnings` | 非阻断提醒 |

核心公式：

```text
tradable_equity = account_total_asset * (1 - cash_buffer_ratio)
target_notional = tradable_equity * target_weight
raw_delta_quantity = (target_notional - current_market_value) / latest_price
order_quantity = floor(abs(raw_delta_quantity) / lot_size) * lot_size
```

V1 使用持仓快照中的 `market_value` 计算差额，同时校验其与 `quantity * latest_price` 的偏差并输出 warning；不在计划生成时静默覆盖用户持仓快照。所有行取整后再做账户级现金校验：

```text
estimated_sell_proceeds = sum(SELL estimated_notional)
estimated_buy_notional = sum(BUY estimated_notional)
cash_after_plan = current_cash + estimated_sell_proceeds - estimated_buy_notional
required_cash_buffer = account_total_asset * cash_buffer_ratio
```

`cash_after_plan < required_cash_buffer` 时整份计划阻断。该口径假设模拟卖出所得可用于同批买入，必须作为 warning；V1 不建模真实市场的成交先后和资金可用规则。

买入和卖出取整规则：

- BUY：向下取整，避免超买和突破现金缓冲。
- SELL：向下取整，但不得超过 `available_quantity`。
- 取整后 `order_quantity = 0` 时输出 `HOLD`，并记录 `below_lot_size` warning。

阻断条件：

- 最新 target weight 不是完整 5-sleeve。
- target weight sum 偏离 `1.0` 超过 `1e-6`。
- 缺少任一 sleeve 的 symbol、价格或持仓映射。
- 最新价格非正数或不可用。
- 账户级 `cash_after_plan` 低于固定现金缓冲。
- 卖出数量超过可用持仓。
- 同一 `plan_date + strategy_version + target_weight_rebalance_date` 已存在非 cancelled plan，避免重复生成订单。

V1 明确不做：

- 不接券商 API。
- 不自动提交真实订单。
- 不做盘口、滑点、冲击成本或成交概率估计。
- 不做税费和真实佣金估计。
- 不做调仓优化器或最小交易成本优化。

### Stage O：模拟盘观察记录

周一以后每天只补最小观测表，不急于做竞品式前端。

建议记录：

- 当日模拟成交状态。
- 成交价、成交数量、成交金额。
- 当日持仓、现金、组合净值。
- 目标权重和实际权重偏离。
- 当日收益、累计收益、相对 baseline 收益。
- 手工备注和异常说明。

竞品绩效分析界面可作为 Stage O 之后的展示参考，但不应阻塞 Stage M 和 Stage N。

### 周末执行顺序

1. 冻结本次模拟盘使用的 strategy version、target weight artifact、baseline artifact 和日期范围。
2. 实现 Stage N 的纯函数订单计划计算，并用 toy fixture 覆盖买入、卖出、低于一手、现金不足和持仓不足。
3. 增加 Stage N CLI，先输出 JSON / Markdown 或 parquet artifact，不接 API 和前端。
4. 实现 Stage M 稳健性报告，复用现有 kernel，不重写回测。
5. 跑一次端到端：target weight -> robustness report -> rebalance plan -> paper order draft。
6. 周一盘前只允许生成 `DRAFT` 订单草案，人工确认后再进入模拟成交记录。

### Go / No-Go

周一启动模拟盘前必须满足：

- 5 个 sleeve 均有完整权重，权重和通过校验。
- 最新价格和持仓快照可追溯。
- 回测报告明确列出短样本和成本 caveat。
- 订单草案不会突破现金缓冲、可用持仓和交易单位约束。
- artifact 可复现，命令和输入日期写入报告。
- 没有真实券商下单路径。

不满足任一条件时，周一只能继续生成研究报告，不能宣称进入模拟盘交易。
