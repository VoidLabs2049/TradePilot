# TradePilot 商品期货阶段 3：P.DCE 单品种质量卡

Generated at: `2026-07-24T06:09:49.321699+00:00`
Code version: `ea10455f0834aba1fb62a317d96d77f6306bcfcd-dirty`
Snapshot id: `9fd0f1dc278f37d0`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| P.DCE | 4551 | 2007-10-29 .. 2026-07-20 | 4550 | 1 | 0.0220% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 56 | 0 | 79.8421 | 526226 | 413547 | 0 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| 1.5491% | 24.2764% | -80.8367% | 8.1808% | -7.2574% | 38 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P2609.DCE | 9389 | 10 | 吨 | 人民币元/吨 | 93890 | 50000 | 1 | 43890 | 87.7800% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.3397 |
| CU.SHF | 0.4220 |
| I.DCE | 0.2286 |
| M.DCE | 0.4612 |
| RB.SHF | 0.2737 |
| SC.INE | 0.3955 |
| TA.ZCE | 0.4104 |

## Stage 3 Decision

结论：`observe`。
- max drawdown is -80.8367%

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
