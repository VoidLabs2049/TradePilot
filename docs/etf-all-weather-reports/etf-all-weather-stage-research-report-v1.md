# TradePilot ETF 全天候资产配置系统：阶段性设计、实现与基线评估

报告日期：2026-07-16

报告版本：V1（冻结）

## 摘要

本文总结 TradePilot ETF 全天候资产配置项目截至 2026 年 7 月 16 日的阶段成果。项目面向 A 股个人投资辅助场景，目标是构建一套低频、可解释、可追溯的多资产风险配置系统，而非短期价格预测或自动交易系统。当前版本以 A 股大盘、小盘、债券、黄金和现金类 ETF 构成 5-sleeve universe，建立了从真实市场数据、月度快照、市场与宏观上下文、风险预算、目标权重到统一回测内核的 frozen artifact 链路。

在 2025-01 至 2026-05 的 17 个调仓周期内，当前动态策略累计收益为 21.4910%，静态 inverse-vol 基线为 21.4344%。当前策略年化波动率和月均换手较高，Sharpe 较低，最大回撤基本一致。由于样本较短、75/85 行目标权重为降级但可计算的 `partial` 状态，且现有正式对比尚未计入交易成本，当前证据不能证明动态 risk budget / regime tilt 相对简单静态基线具有稳定增益。

本阶段的主要成果是完成可审计的工程研究链路和首个同内核基线对比，而不是确认一套可直接实盘的最优策略。下一阶段将优先冻结实验合同、执行参数批量回测和 walk-forward 稳定性检验；纳斯达克 ETF、大宗商品 ETF/基金及豆粕基金先进入候选调研，不直接批量加入组合。

**关键词：** ETF 全天候；资产配置；风险预算；逆波动率；回测；可追溯研究

## 1. 研究背景与问题

全天候资产配置的核心并非预测单一资产涨跌，而是通过权益、债券、黄金和现金类资产之间的风险差异，降低组合对单一市场状态的依赖。TradePilot 的最终目标是形成可服务于真实资金配置的低频策略，并通过“历史研究、参数验证、模拟盘、小资金验证、正式运行”的分阶段路径控制模型与执行风险。当前版本先形成 point-in-time 市场和宏观上下文，再生成风险预算与目标权重，最后通过统一回测内核评价历史表现。

本阶段主要回答三个问题：

1. ETF 全天候研究链路能否从真实数据稳定生成可追溯的风险预算、目标权重和回测结果？
2. 当前动态策略是否相对简单静态 inverse-vol 基线表现出明确增益？
3. 在进入参数优化和新标的扩展前，当前证据暴露了哪些限制？

本文评估当前 V1 是否已经积累足够证据进入下一验证阶段。真实资金应用是项目最终目标，但在完成参数稳健性、交易成本、模拟盘和小资金验证前，本文不直接给出正式资金执行结论，也不把历史最优结果解释为未来收益保证。

## 2. 系统设计与研究对象

### 2.1 当前资产范围

当前 `etf_aw_v1` 使用以下 5 个 frozen sleeves：

| Sleeve | ETF | 风险暴露 |
| --- | --- | --- |
| `equity_large` | `510300.SH` | A 股大盘权益 |
| `equity_small` | `159845.SZ` | A 股小盘权益 |
| `bond` | `511010.SH` | 债券防御 |
| `gold` | `518850.SH` | 黄金与压力对冲 |
| `cash` | `159001.SZ` | 现金与短久期缓冲 |

ETF 日频 `volume` 沿用 Tushare `fund_daily` 的“手”单位，`amount` 沿用“千元人民币”单位，derived 层不做静默换算。

### 2.2 Frozen artifact 链路

当前研究链路为：

```text
真实市场、宏观和利率数据
-> 月度调仓快照
-> market regime 与 strategy context
-> frozen risk budget
-> frozen target weight
-> common backtest kernel
-> baseline comparison report
```

风险预算和目标权重在进入回测前已经写出。回测内核不在运行过程中重新判断 regime、重新估计预算或搜索参数。这一边界使每条净值曲线可以追溯到固定输入，降低研究过程中事后调参的风险。

### 2.3 当前策略方法

当前风险预算由固定中性预算、规则式 regime delta 和 confidence clamp 共同生成。宏观或利率上下文不足时，系统不再直接阻断 market-only 研究，而是输出 `partial / degraded_research`，并将主动 tilt 的 confidence cap 限制为 0.35。

目标权重使用 `budgeted_inverse_vol` V1：

```text
raw score_i = risk budget_i / max(volatility_i, volatility floor)
raw weight_i = raw score_i / sum(raw score)
```

随后应用 cash/non-cash 权重上限和 no-trade band。该方法不使用协方差矩阵，因此不是完整 Equal Risk Contribution，也不保证最终风险贡献严格等于输入风险预算。

### 2.4 对比基线

正式对比基线为 `static_inverse_vol_v1`。它与当前策略使用相同的 5-sleeve universe、63 个交易日波动率窗口、42 个最小有效观测和 0.005 volatility floor，并通过同一版本回测内核运行。主要差异是基线不消费动态 risk budget。

## 3. 数据与实验方法

### 3.1 数据范围

- 数据区间：2025-01-01 至 2026-05-31。
- 调仓日历：`etf_aw_v1_monthly_post_20`。
- 月度调仓周期：17 个。
- 日频净值观测：326 行/策略。
- 目标权重：85 行，即 5 sleeves × 17 个调仓周期。
- 当前正式基线对比未计入交易成本。

市场数据经过 adjustment-aware 处理。宏观和利率上下文保留缺失、陈旧、来源及修订 caveat。国债收益率曲线历史回补曾因 Tushare `yc_cb` 权限不足而不完整，当前设计提供 AKShare fallback，但既有评价区间仍有大量降级上下文。

### 3.2 Artifact 验证

| Artifact | 当前验证结果 |
| --- | --- |
| `derived.etf_aw_risk_budget` | 85 行；75 行 `partial`、10 行 `complete` |
| `derived.etf_aw_target_weight` | 85 行；75 行 `partial`、10 行 `complete`、0 行 `unavailable` |
| `derived.etf_aw_baseline_weight` | 85 行；健康检查通过 |
| 当前策略 backtest kernel | 349 行；合同校验通过 |
| baseline backtest kernel | 349 行；合同校验通过 |
| 两条 kernel diagnostics | 均为空 |

早期人工检查曾记录 75 行目标权重为 `unavailable`。后续已修复两类问题：一是 macro/rates 缺失导致 market-only 场景被过度硬降级；二是 `2025-03-20` 风险预算舍入后略超 `1e-6` 容忍线。重建后，目标权重状态为 75 行 `partial`、10 行 `complete`、0 行 `unavailable`。因此本文采用修复后的正式基线报告口径。

### 3.3 评价指标

本阶段比较累计收益、年化收益、年化波动率、最大回撤、Sharpe 和月均换手。差值均定义为“当前动态策略减静态 inverse-vol 基线”。由于尚未实现正式成本稳健性报告，结果均为 gross 口径。

## 4. 实验结果

### 4.1 基线对比

| 指标 | 当前动态策略 | 静态 inverse-vol | 差值 |
| --- | ---: | ---: | ---: |
| 累计收益 | 21.4910% | 21.4344% | +0.0566 个百分点 |
| 年化收益 | 16.2393% | 16.1975% | +0.0418 个百分点 |
| 年化波动率 | 6.6479% | 6.5432% | +0.1047 个百分点 |
| 最大回撤 | -4.8348% | -4.8230% | -0.0118 个百分点 |
| Sharpe | 2.4428 | 2.4755 | -0.0327 |
| 月均换手 | 5.0258% | 4.3387% | +0.6871 个百分点 |

当前动态策略的累计和年化收益仅略高于基线，但同时承担了更高波动率和换手，Sharpe 更低，最大回撤几乎没有改善。0.0566 个百分点的累计收益差异在计入额外交易摩擦后能否保留尚待验证，当前结果不足以证明动态层具有稳定经济价值。

### 4.2 状态完整性

17 个调仓周期中，只有最近 2 个周期的 10 行风险预算和目标权重为 `complete`，其余 15 个周期的 75 行为 `partial`。`partial` 表示链路可以生成受限且可审计的权重，但宏观/利率上下文不完整，主动 tilt 受到较低 confidence cap 限制。

因此，当前对比更接近“受限动态预算与静态 inverse-vol 的工程增量测试”，不能代表完整宏观状态模型的长期评价。

### 4.3 工程正确性

当前阶段已确认：

- 每个调仓周期均包含完整 5-sleeve 权重。
- risk budget、raw target weight、constrained target weight 和最终 target weight 每期合计为 1。
- cash 与 non-cash 权重上限检查无违反项。
- 回测内核消费 frozen target weight，不静默回退等权。
- 当前策略和静态基线均由同一内核运行，并保留权重来源字段。
- 当前正式对比的两条 kernel 均无 blocking diagnostic。

这些结果证明研究链路已具备可重复的工程基础，但不等价于证明策略具有样本外超额收益。

## 5. 讨论

### 5.1 动态层尚未证明增益

当前动态策略相对静态基线的 gross 收益差异很小，且尚未计入交易成本；该噪声级差异在计入成本后可能消失，风险调整后指标反而略弱。最直接的解释不是立即增加模型复杂度，而是先验证动态层是否在更长样本、成本情景和参数邻域中持续提供信息。

### 5.2 当前 universe 可能存在结构性问题

`cash` 和债券波动率可能同时受到 volatility floor 影响，使 inverse-vol 权重偏向低波动资产。大盘和小盘 ETF 也可能主要代表同一个 A 股权益 beta，而非两个独立风险来源。这两点应先通过参数和风险桶对照检验，再决定是否引入更复杂优化器。

### 5.3 新标的扩展的合理位置

纳斯达克 ETF、大宗商品 ETF/基金和豆粕基金可能补充海外成长、通胀或农产品风险暴露，但本阶段尚无正式候选表或组合实验结果。尤其场内 ETF 与场外基金不能共用成交假设：前者涉及市场价格、价差和折溢价，后者涉及未知净值、申赎确认时滞和持有期费用。

因此，新标的目前只能作为待调研候选，不能在本文中评价其组合改善效果。

## 6. 研究局限

本阶段存在以下主要限制：

1. 样本仅覆盖 17 个调仓周期，不能充分覆盖多种宏观和压力状态。
2. 15/17 个周期依赖 `partial` 上下文，完整动态策略证据不足。
3. 正式基线对比尚未计入佣金、滑点或其他交易成本。
4. 当前 turnover 使用 previous-target 口径，与真实 drifted pre-trade weight 仍有差异。
5. 当前 Sharpe 使用 `risk_free_rate = 0`。
6. 当前 inverse-vol 不使用相关性矩阵，无法直接控制资产间共同风险。
7. 尚未完成参数矩阵、walk-forward、参数邻域和压力区间实验。
8. 尚未完成纳斯达克、大宗商品和豆粕基金候选调研。
9. 当前结果为历史研究，不包含 shadow portfolio 或真实资金执行证据。

## 7. 结论

截至 2026 年 7 月 16 日，TradePilot 已完成 ETF 全天候 V1 从真实数据到 frozen risk budget、target weight、双权重源回测和基线报告的主要工程链路。其核心价值是建立了可追溯、可检查、避免回测时动态改参的研究基础。

现有 17 个调仓周期的证据显示，当前动态 `target_weight_inverse_vol_v1` 尚未相对 `static_inverse_vol_v1` 提供明确的风险调整后增益。动态策略的 gross 收益仅略高，且该未计成本的噪声级差异在计入成本后可能消失；同时其波动率和换手更高、Sharpe 更低、最大回撤几乎相同。基于当前证据，V1 尚未达到直接使用真实资金的验证门槛，应继续完成参数、成本和 forward evidence 验证，而不是无条件增加模型复杂度。

当前合理决策是保留现有版本作为工程研究候选，同时把静态 inverse-vol 保留为强制对照。下一阶段先完成参数优化和稳健性验证；只有在形成可重复的参数结论后，再对新标的进行短名单调查和单变量组合对照。

## 8. 后续工作

后续按以下顺序推进：

1. 冻结当前基线的数据区间、回测内核、成本和 turnover 口径。
2. 整理首轮参数清单，定义批量实验的最小输入与结果字段。
3. 跑通小规模参数组合，再执行完整参数矩阵和 walk-forward 回测。
4. 输出参数邻域、成本敏感性和是否保留当前基线的判断。
5. 参数阶段完成后，建立纳斯达克 ETF、大宗商品 ETF/基金和豆粕基金候选表。
6. 只对通过初筛的标的逐个执行单变量组合对照。
7. 基于后续实际实验结果另行生成新的版本化阶段报告，不回写本 V1 报告。

## 参考项目材料

1. [ETF 全天候当前设计](../etf-all-weather-implementation/current-design.md)
2. [ETF 全天候回测基线对比报告（2026-07-10）](../etf-all-weather-implementation/backtest-baseline-comparison-report-2026-07-10.md)
3. [ETF 全天候风险预算人工检查记录](../etf-all-weather-implementation/risk-budget-manual-check-2026-07-06.md)
4. [ETF 全天候目标权重人工检查记录](../etf-all-weather-implementation/target-weight-manual-check-2026-07-06.md)
5. [ETF 全天候策略优化阶段计划](../etf-all-weather-implementation/strategy-improvement-research-design.md)
6. [ETF 全天候回测稳健性评估设计](../etf-all-weather-implementation/backtest-robustness-evaluation-design.md)

## 附录：结果复现命令

现有正式基线对比使用以下命令链：

```bash
python -m tradepilot.etf_aw.cli build-baseline-weight \
  --baseline static-inverse-vol \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli health-check baseline-weight \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli backtest-kernel \
  --strategy target-weight \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli backtest-kernel \
  --strategy baseline \
  --baseline static-inverse-vol \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli backtest-report \
  --start-date 2025-01-01 \
  --end-date 2026-05-31 \
  --format markdown
```
