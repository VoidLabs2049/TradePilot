# TradePilot 商品期货阶段 3：TA.ZCE 单品种质量卡

Generated at: `2026-07-24T07:47:07.015577+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `486766836384bedc`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## Input Caveats

- Stage 2 sample starts at 2008-09-16 after excluded unauditable rolls

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| TA.ZCE | 4332 | 2008-09-16 .. 2026-07-20 | 4331 | 1 | 0.0231% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 59 | 1 | 72.2 | 927276 | 966598 | 0 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| -1.2669% | 24.0518% | -69.6248% | 7.6971% | -8.0586% | 46 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TA2609.ZCE | 5890 | 5 | 吨 | 人民币元/吨 | 29450 | 50000 | 2 | 8900 | 17.8000% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.3234 |
| CU.SHF | 0.4053 |
| I.DCE | 0.3034 |
| M.DCE | 0.2388 |
| P.DCE | 0.4104 |
| RB.SHF | 0.3273 |
| SC.INE | 0.6317 |

## Stage 3 Decision

结论：`observe`。
- abnormal roll return days: 1

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
