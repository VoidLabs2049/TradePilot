# ETF 全天候策略提升与研究设计

## 目的

本文定义 TradePilot ETF 全天候策略下一阶段的研究框架，用于回答以下问题：

1. 当前 5-sleeve 组合是否提供了真实、稳定且可交易的风险分散。
2. 当前动态 risk budget / regime tilt 是否比简单静态配置产生可重复的样本外增益。
3. 风险估计、权重生成和调仓规则能否在扣除成本后改善回撤、恢复速度和组合稳定性。

本文是 research-only 设计，不改变当前生产数据合同、V1 strategy version、Stage N/O 人工决策边界或 live pilot 准入规则。任何候选策略必须先生成独立 frozen artifact，再进入现有 backtest kernel 和 forward evidence 流程。

## 当前基线与主要限制

当前 V1 使用 5 个 frozen sleeves：

| Sleeve | ETF | 主要风险暴露 |
| --- | --- | --- |
| `equity_large` | `510300.SH` | A 股大盘权益 |
| `equity_small` | `159845.SZ` | A 股小盘权益 |
| `bond` | `511010.SH` | 债券防御 |
| `gold` | `518850.SH` | 黄金与压力对冲 |
| `cash` | `159001.SZ` | 现金与短久期缓冲 |

当前研究链路为：

```text
market / macro context
-> regime score
-> risk budget
-> budgeted inverse-vol target weight
-> monthly frozen weight
-> common backtest kernel
-> robustness / shadow evaluation
```

当前 17 个调仓周期的已观测结果为：

| 指标 | 当前动态策略 | `static_inverse_vol_v1` |
| --- | ---: | ---: |
| 累计收益 | 21.49% | 21.43% |
| 年化波动率 | 6.65% | 6.54% |
| Sharpe | 2.44 | 2.48 |
| 月均换手 | 5.03% | 4.34% |
| 最大回撤 | -4.83% | -4.82% |

这些结果支持的当前判断是：

- 动态策略尚未表现出相对静态 inverse-vol 的可观测增益。
- 动态策略承担了更高换手，但收益、波动和回撤基本不变。
- 85 行 risk budget 中约 75 行为 `partial`，`confidence cap = 0.35` 使大部分主动 tilt 接近中性预算。
- 当前样本集中在 2025-01 至 2026-05，不能据此证明动态信号永久无效，但已不足以支持继续为现有复杂链条增加功能。

因此默认决策不是继续增强 macro/regime 模型，而是把复杂动态层降为待证明候选。任何复杂版本必须先战胜更简单、数据完整且始终可计算的规则。

开始新策略研究前必须承认以下限制：

- 正式报告历史较短，当前约 17 个调仓周期，不能覆盖足够多的宏观状态。
- 部分 ETF 上市较晚，直接使用 ETF 历史容易产生样本起点偏差。
- `static_inverse_vol_v1` 不使用相关性矩阵，只控制单资产波动率。
- 当前 kernel 隐含日度拉回目标权重，但 turnover 只在月度调仓日记录，净值和换手假设不完全一致。
- 单 sleeve 日期不连续可能把跨日累计收益压缩到一个交易日。
- 当前 Sharpe 使用 `risk_free_rate = 0`。
- 当前主要 baseline 是静态逆波动率，缺少外部简单组合基准。
- `cash` 的近零波动率会在 inverse-vol 中产生过高原始权重，当前主要依赖权重上限修正。
- `equity_large` 与 `equity_small` 可能主要暴露于同一个 A 股 beta，而非两个独立风险来源。

因此，研究顺序必须先提高证据可信度，再比较更复杂的策略。

## 研究原则

### 单变量增量

每个候选版本只改变一个主要因素，例如协方差估计、波动率窗口、regime tilt 或调仓规则。不得同时更换 universe、优化器、状态模型和成本假设后，把全部差异归因于某一个模块。

### Frozen artifact

每个候选必须拥有独立的：

- `strategy_name`
- `strategy_version`
- 参数清单和数据时点规则
- risk budget / target weight artifact
- kernel 输出和评估报告

回测循环不得根据结果重新选参、改变权重或修改 regime。

### 简单基准优先

复杂策略只有在多个简单基准之上提供稳定增益时才保留。历史累计收益最高不是充分条件。

当前 macro/regime 链条不再被视为默认主策略。研究首先验证四资产静态配置和简单二元趋势；若简单候选已经达到相同或更好的风险收益结果，则移除无信息增量的复杂层。

### 样本外优先

策略选择依据必须来自 walk-forward 或其他严格时间切分。全样本拟合结果只能用于诊断，不能作为晋级依据。

### 可交易性优先

收益改善若依赖高换手、不可实现成交价、过低流动性或精确参数，则视为无效改善。

## 研究问题

### RQ-1：数据是否足以支持策略结论

需要验证：

- 每个 sleeve 是否与交易日历连续对齐。
- 停牌、缺行和复牌后的收益是否被正确展开或明确标记。
- 复权、分红和基金份额变化是否保持一致语义。
- ETF 上市前代理数据是否 point-in-time、可追溯且不混入未来成分。
- 宏观字段是否按发布日期而不是统计期对齐。

任何确认的未来数据、代理拼接错误或无法解释的跨日收益都会阻断后续策略比较。

### RQ-2：5-sleeve universe 是否真正分散风险

研究内容：

- 滚动相关性和压力期相关性。
- 每个 sleeve 的边际风险贡献。
- 债券、黄金和现金在股市下跌期的实际防御能力。
- 大盘与小盘是否提供独立风险来源，还是重复权益 beta。
- 单一 ETF 是否稳定代表目标 sleeve，是否存在流动性或跟踪误差问题。

新增 sleeve 的理由必须是补充风险来源，而不是增加资产数量。

首个 universe 对照应测试 `cash` 是否应继续参与风险优化：

| 版本 | 风险优化对象 | Cash 处理 |
| --- | --- | --- |
| `five_sleeve_inverse_vol` | 两只权益、债券、黄金、cash | 保留当前行为作为对照 |
| `four_asset_inverse_vol` | 两只权益、债券、黄金 | 组合外固定保留 1% 至 5% 现金缓冲 |

现金缓冲用于交易取整、费用和流动性管理，不被解释为独立风险资产。若策略需要更保守，应提高债券或防御资产的明确预算，而不是依赖 cash 的低波动率扩大权重。

第二个 universe 对照应测试权益伪分散：

- 当大盘和小盘滚动相关性高于预先冻结阈值时，先合并为一个 `equity` 风险桶。
- 组合优化只决定总权益权重，再按固定比例在大盘和小盘之间分配。
- 固定比例必须提前冻结；V1 使用 50/50 作为最小对照，不根据历史收益择优。
- 与“两只权益独立参与 inverse-vol”版本比较权益总风险贡献和压力期损失。

后续才研究 sleeve 替换。候选方向包括红利低波等防御风格和创业板/科创等成长风格，但必须证明风格差异比单纯规模差异带来更稳定的风险分散。若无法证明，应诚实地把策略定义为“股债金风险预算组合”，而不是仅因资产数量称为全天候。

### RQ-3：最小相关性意识能否优于独立 inverse-vol

当前 inverse-vol 是最小可靠基线。第一轮候选不直接进入 full ERC，而是先验证权益风险桶：

| 候选 | 变化 | 目的 |
| --- | --- | --- |
| `static_inverse_vol_v1` | 当前基线 | 保留简单、可解释基准 |
| `four_asset_inverse_vol_v1` | cash 退出优化 | 消除近零波动率造成的权重失真 |
| `equity_bucket_inverse_vol_v1` | 大小盘先合并为权益风险桶 | 避免把同一 A 股 beta 计作两个风险源 |
| `multi_horizon_inverse_vol_v1` | 多窗口波动率混合 | 降低单一窗口敏感性 |
| `erc_shrinkage_v1` | 收缩协方差 + Equal Risk Contribution | 仅在简单候选仍暴露相关性问题时研究 |

V1 研究不直接引入复杂机器学习优化器、Black-Litterman、深度学习或不可解释的 alpha forecast。

### RQ-4：简单趋势规则能否替代当前动态 regime

必须比较以下冻结版本：

| 版本 | 主动规则 | 用途 |
| --- | --- | --- |
| `static_budget` | 固定中性预算 | 隔离基础配置收益 |
| `binary_equity_trend_v1` | 两只权益均高于各自 60 日均线时中性偏权益，否则降低权益、提高债券/黄金 | 验证完整、简单的价格趋势信息 |
| `current_regime_v1` | 当前 market/macro/confidence 链条 | 检验复杂链条的实际增量 |
| `reverse_trend_placebo` | 反转二元趋势的 tilt 方向 | 检验趋势规则是否优于占位解释 |

`binary_equity_trend_v1` 不使用 confidence score，不依赖 macro context，也不因宏观字段缺失降级。均线、观察时点、权重变化幅度和生效时点必须提前冻结。

若简单趋势不能战胜静态基线，优先判断 universe 和基础配置是否缺乏分散性，而不是增加更多状态分类。只有简单趋势形成稳定增益后，才恢复以下二级研究：

- `market_only_tilt`
- `macro_confirmed_tilt`
- `reverse_tilt_placebo`

若正向规则与反向 placebo 表现接近，或增益只存在于单一区间，则不能宣称主动信号有效。

### RQ-5：调仓规则能否降低无效交易

候选规则：

- 固定月度调仓。
- 固定季度调仓。
- 月度检查，但仅当单 sleeve 偏离超过 2%、3% 或 5% 时交易。
- 组合总 turnover 超过阈值时才执行。

阈值必须在训练区间冻结，并在样本外保持不变。研究重点是降低换手和执行偏差，不是寻找历史最优阈值。

### RQ-6：组合回撤保护是否改善尾部行为

单独研究组合层回撤保护，不把它与趋势、ERC 或 universe 变化同时引入。最小候选规则为：

```text
组合从历史净值高点回撤超过 5%
-> 下一可执行调仓时点将权益总权重限制在 40% 以内
```

该规则必须同时冻结再入场条件，否则容易在下跌后永久低配权益或在低点锁定损失。至少比较：

- 净值重新创出前高后解除限制。
- 净值回到距离前高 2% 以内后解除限制。
- 权益趋势重新转正并持续预设交易日后解除限制。

研究报告必须单独展示：

- 触发次数和受限交易日。
- 触发后的最大回撤改善。
- 反弹期机会成本。
- 恢复期变化。
- 因保护规则新增的 turnover 和成本。

回撤保护是风险预算上限，不是对未来收益的预测。若它只降低收益而未稳定改善尾部损失或恢复时间，则不保留。

## 基准体系

所有策略至少与以下基准比较：

1. `equal_weight_fixed_20pct_each`：5-sleeve 等权。
2. `static_inverse_vol_v1`：当前静态逆波动率。
3. `static_budget_no_tilt`：当前基础预算但不使用 regime tilt。
4. `four_asset_inverse_vol_v1`：cash 退出优化后的四资产静态组合。
5. `equity_bucket_inverse_vol_v1`：权益风险桶版本。
6. `equity_buy_and_hold`：沪深 300 或对应 ETF 买入持有。
7. `simple_stock_bond_gold`：预先冻结的简单股债金组合。

外部基准必须明确：

- 使用指数还是 ETF。
- 收益是否含分红。
- 调仓频率。
- 费用和滑点。
- 上市前代理与 ETF 实际区间的拼接边界。

## 数据研究

### 历史延伸

ETF 上市前历史可以使用对应指数或可追溯代理，但必须分层保存：

```text
proxy index history
-> transition boundary
-> tradable ETF history
```

不得把代理指数与 ETF 收益无标记拼成一条“可交易历史”。报告必须同时给出：

- 全部研究区间结果。
- 纯 ETF 可交易区间结果。
- 代理区间与 ETF 区间的指标差异。

### 连续性诊断

新增研究前应先设计以下 diagnostics：

- `missing_trade_date`
- `unexpected_suspension_gap`
- `compressed_multi_day_return`
- `invalid_adjustment_factor`
- `non_positive_adjusted_price`
- `proxy_transition_discontinuity`

blocking diagnostic 不得因评价区间裁剪而被隐藏。

### 可交易性字段

候选 ETF 研究至少记录：

- 日成交额，单位明确为 CNY。
- 成交量，保留源单位“手”。
- 估算买卖价差或可获得的最保守代理。
- 停牌和零成交日。
- 跟踪指数和跟踪误差。
- 最小交易单位和价格水平。

## 权重方法研究

### 权益风险桶

第一阶段不求解完整协方差优化，而是显式处理最明显的相关性：

1. 计算大盘与小盘收益的 point-in-time 滚动相关性。
2. 相关性超过冻结阈值时，两者作为一个 `equity` 风险桶参与 inverse-vol。
3. 先确定 equity、bond、gold 的资产桶权重。
4. 总权益权重再按冻结比例分配到大盘和小盘。

报告必须比较独立 sleeve 与风险桶版本的权益总权重、权益风险贡献、压力期损失和 turnover。

### 收缩协方差 ERC

`erc_shrinkage_v1` 是后置候选。只有权益风险桶仍无法控制相关性集中时，才使用 point-in-time 收益窗口估计收缩协方差矩阵，并求解各 sleeve 风险贡献接近目标预算的非负权重。

约束至少包括：

- 权重非负。
- 权重和为 1。
- 单 sleeve 上限。
- cash 不参与优化；只在组合外保留冻结比例的运营缓冲。
- 协方差矩阵必须有限且可用。
- 求解失败时阻断，不回退到事后选择的权重。

研究必须同时输出 raw risk contribution 和 constrained risk contribution，避免仅凭最终权重判断 ERC 是否成立。

### 多窗口波动率

候选窗口可使用短、中、长期，例如 20、60、120 或 252 个交易日，但权重组合方法必须提前冻结。研究报告需展示：

- 单窗口结果。
- 多窗口组合结果。
- 窗口轻微扰动后的敏感性。
- 权重变化速度和 turnover。

若只有一个精确窗口有效，候选不晋级。

## Regime 研究

Regime 研究分两级。一级只研究始终可计算的二元权益趋势；二级才研究 market/macro 状态分类。当前 macro pipeline 可以继续作为研究数据源，但在证明增量前不作为候选策略的硬依赖。

### 二元趋势定义

最小信号只使用两只权益 ETF 在决策时点已经完成的收盘数据：

```text
equity_large close > 60-day moving average
AND equity_small close > 60-day moving average
-> trend_on
otherwise
-> trend_off
```

必须提前冻结：

- 均线是否包含决策日收盘。
- 信号在哪个交易日生效。
- `trend_on` / `trend_off` 的目标风险预算。
- 单次权益预算变化上限。
- 均线附近是否使用缓冲带或连续确认日。

60 日不是待优化的唯一正确参数。必须同时展示相邻窗口的稳定性，不能从结果中选择最优均线。

### 状态定义

状态研究应从少量可解释维度开始：

- 权益 risk-on / risk-off。
- 利率上行 / 下行。
- 通胀压力高 / 低。
- 流动性或信用条件收紧 / 放松。

每个状态必须定义：

- 输入字段。
- 发布时间和可用时点。
- 阈值或打分公式。
- 缺失/陈旧时的降级规则。
- 最小 confidence。
- 对 risk budget 的最大允许 tilt。

### 增量验证

对每个状态分别报告：

- 状态观察数和持续期。
- 策略与基准收益。
- 最大回撤和恢复期。
- sleeve contribution。
- tilt 前后风险贡献。
- 换手和成本。

少于预设最小观察数的状态只能展示，不参与“有效/无效”的结论。

## 回测与验证设计

### Walk-forward

推荐最小流程：

```text
历史窗口估计参数
-> 冻结下一期 risk budget / target weight
-> 评价下一期收益
-> 窗口向前滚动
```

所有特征、协方差、阈值和权重只能使用决策时点之前已发布的数据。

### 参数稳定性

每个候选必须展示参数邻域，而不是只展示最优点。例如：

- 波动率窗口上下浮动。
- shrinkage 强度邻域。
- tilt cap 邻域。
- rebalance threshold 邻域。

晋级候选应在一片合理参数区域内保持结论方向一致。

### 压力区间

压力测试至少包含：

- 权益快速下跌。
- 利率快速上行、债券下跌。
- 股债同跌。
- 黄金未提供对冲。
- 流动性下降和价差扩大。

压力区间应在看结果前通过公开规则冻结，不允许事后挑选最有利区间。

### 成本情景

继续使用统一 robustness evaluation 层，至少比较：

- gross。
- 5 bps。
- 10 bps。
- 20 bps 或压力成本。

进一步研究应把月度 target turnover 改为基于 drifted pre-trade weight 的可执行 turnover，并与当前 previous-target 口径并列展示，不能静默替换历史口径。

## 评价指标

### 核心指标

- 累计收益和年化收益。
- 年化波动率。
- 最大回撤。
- Sharpe。
- Calmar。
- 最大水下持续交易日。
- 最大回撤恢复交易日。
- turnover 和成本后收益。

### 辅助指标

- Sortino。
- 日/月胜率和盈亏比。
- 最差单月。
- VaR / CVaR。
- 偏度和峰度。
- 相对基准 tracking error 和 information ratio。
- sleeve 收益贡献和风险贡献。

在加入 Sortino、Sharpe 超额收益或 information ratio 前，必须先冻结无风险利率和基准收益的 point-in-time 来源。`risk_free_rate = 0` 可以继续保留为兼容指标，但不得与使用真实无风险利率的新指标混称。

## 收益归因

候选策略必须把结果分解为：

```text
基础静态配置收益
+ risk estimation / ERC 增量
+ regime tilt 增量
+ rebalance effect
- transaction cost
= net strategy return
```

至少输出：

- 每个 sleeve 的收益贡献。
- 每个 sleeve 的平均和峰值风险贡献。
- 相对 baseline 的主动权重贡献。
- 简单趋势、regime tilt 和回撤保护分别造成的收益与 turnover。
- 无法归因的日期和原因。

无法解释主要收益来源的候选不得晋级 forward 观察。

## 候选晋级门槛

候选从历史研究进入 shadow 前必须同时满足：

### 数据门槛

- 无未来数据或发布日期错位。
- 所有 blocking diagnostics 已解决或明确阻断报告。
- 代理区间与纯 ETF 区间分别报告。
- 输入和输出可由 frozen artifact 重建。

### 稳健性门槛

- 至少在多个 walk-forward 区间方向一致。
- 不依赖单一精确参数。
- 在 10 bps 成本下不暴露结构性失效。
- 回撤、恢复期或风险稳定性至少有一项相对简单基准形成明确改善。
- 改善不能只来自一个 sleeve 或一个短状态区间。

### 复杂度门槛

- 新模块提供的增益可以解释。
- 若复杂候选与简单基准差异不显著，保留简单基准。
- 运行所需数据能够稳定、按时、point-in-time 获取。

通过历史研究不等于可以接入当前 shadow account。候选必须使用新的 strategy version 和独立 forward 序列。

## 停止条件

出现以下任一情况时停止或重新设计候选：

- 使用未来数据、事后修订值或事后选择的状态区间。
- 主要结论在轻微参数扰动后反转。
- 仅全样本有效，walk-forward 无效。
- 成本后优势消失且换手无法通过简单规则降低。
- 动态 tilt 与 reverse placebo 无明显区别。
- 回撤保护没有稳定改善尾部损失或恢复时间。
- cash 退出优化后没有降低权重失真，或运营缓冲不足以支持交易执行。
- 需要持续人工修改输入或 artifact 才能运行。
- 新 sleeve 的风险暴露与现有 sleeve 高度重复。
- 复杂模型无法解释主要权重变化或收益来源。

## 实施阶段

### Phase 0：研究可信度

交付物：

- sleeve 日期连续性与跨缺口收益诊断设计及实现。
- 代理指数历史延伸设计，明确 proxy/ETF 分层。
- 外部简单基准 artifact 设计。
- 当前 kernel 的 drift、成交时点和 turnover 假设对照报告。

完成标准：数据问题不会被策略表现掩盖，历史与可交易区间可以独立评价。

### Phase 1：简化 universe 与静态权重

交付物：

- `four_asset_inverse_vol_v1`：cash 退出优化、组合外保留固定缓冲。
- `equity_bucket_inverse_vol_v1`：大小盘合并为权益风险桶。
- `multi_horizon_inverse_vol_v1` 设计和 artifact。
- 与等权、静态 inverse-vol、static budget 的统一 walk-forward 报告。

完成标准：选出至多一个静态候选；若无稳定改善，则保留现有 inverse-vol。

### Phase 2：简单趋势与回撤保护

交付物：

- static、`binary_equity_trend_v1`、`reverse_trend_placebo` 三版本 artifact。
- 5% 回撤触发、40% 权益上限及多种冻结再入场规则的独立对比。
- 趋势与回撤保护各自的收益、回撤、恢复期、贡献、换手和成本报告。

完成标准：确认简单主动规则是否有独立增益；无增益则保留静态策略，不升级复杂 regime。

### Phase 3：相关性优化、Regime 与执行

交付物：

- 仅在需要时实现 `erc_shrinkage_v1`。
- 仅在简单趋势有效后比较 market-only、macro-confirmed 和 reverse-placebo。
- 月度、季度和 threshold rebalance 对比。
- drifted pre-trade turnover 与当前口径并列报告。
- 流动性、价差、最小交易单位和成交延迟压力测试。

完成标准：冻结可执行的 rebalance policy 和成本假设。

### Phase 4：Forward evidence

交付物：

- 新 strategy version 的独立 shadow account。
- 至少 3 个、目标 6 个完整调仓周期。
- 每周期 post-mortem 和汇总报告。

完成标准沿用 `forward-evidence-and-live-pilot-gate.md`，不得因历史回测优秀而缩短观察窗口。

## 建议的首批任务

按依赖顺序执行：

1. 增加 sleeve 日期连续性和 compressed return diagnostics。
2. 设计并冻结外部简单基准，不先实现新优化器。
3. 建立 walk-forward 研究报告合同和参数邻域输出。
4. 将 cash 移出风险优化，验证四资产静态版本。
5. 合并权益风险桶，验证相关性调整是否改善实际风险贡献。
6. 验证 `binary_equity_trend_v1` 和 `reverse_trend_placebo`。
7. 独立验证组合回撤保护及再入场规则。
8. 只有简单候选仍暴露相关性问题时才实现 `erc_shrinkage_v1`。
9. 只有简单趋势有效时才恢复 macro-confirmed regime 研究。
10. 最后研究调仓阈值、真实 turnover 和执行成本。

## 非目标

本阶段不包含：

- 自动实盘下单。
- 根据 Dashboard 交互即时调参。
- 在回测循环中网格搜索后自动采用最优参数。
- 深度学习价格预测、强化学习调仓或黑盒组合优化。
- 未解决数据时点和代理边界前扩展海外、期货或期权 universe。
- 用短期收益替代数据、运行和审计门槛。

## 完成标准

- 研究问题、候选版本、基准和停止条件均可机械复核。
- 每个候选仅改变一个主要因素，并拥有独立 frozen artifact。
- walk-forward、成本、压力区间和参数稳定性成为统一评价合同。
- 策略收益能够分解为基础配置、风险估计、regime tilt、再平衡和成本。
- 最终结果允许得出“保持现有简单策略”或“移除无增益动态模块”的结论，而不是预设必须增加复杂度。
