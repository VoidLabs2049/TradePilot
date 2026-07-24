# TradePilot 商品期货阶段 3：AL.SHF 单品种质量卡

Generated at: `2026-07-24T07:46:42.247613+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `288fd6d553195f16`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| AL.SHF | 5000 | 2005-12-20 .. 2026-07-20 | 4999 | 1 | 0.0200% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 244 | 1 | 20.4082 | 113527 | 138608 | 1 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| 0.0331% | 17.1178% | -69.2236% | 5.5064% | -6.3065% | 17 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| AL2609.SHF | 23010 | 5 | 吨 | 人民币元/吨 | 115050 | 50000 | 1 | 65050 | 130.1000% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| CU.SHF | 0.5809 |
| I.DCE | 0.3148 |
| M.DCE | 0.2612 |
| P.DCE | 0.3397 |
| RB.SHF | 0.3615 |
| SC.INE | 0.2431 |
| TA.ZCE | 0.3234 |

## Stage 3 Decision

结论：`observe`。
- abnormal roll return days: 1
- zero volume days: 1

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
