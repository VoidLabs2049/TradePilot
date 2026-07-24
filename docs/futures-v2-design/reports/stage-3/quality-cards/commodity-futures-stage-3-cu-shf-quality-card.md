# TradePilot 商品期货阶段 3：CU.SHF 单品种质量卡

Generated at: `2026-07-24T07:46:48.309086+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `b3ae4b7b7b7dca30`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| CU.SHF | 5000 | 2005-12-20 .. 2026-07-20 | 4999 | 1 | 0.0200% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 247 | 2 | 20.1613 | 160605 | 176098 | 2 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| 6.7914% | 22.7735% | -68.5182% | 6.3539% | -6.6193% | 47 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CU2609.SHF | 103790 | 5 | 吨 | 人民币元/吨 | 518950 | 50000 | 1 | 468950 | 937.9000% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.5809 |
| I.DCE | 0.3988 |
| M.DCE | 0.3165 |
| P.DCE | 0.4220 |
| RB.SHF | 0.4044 |
| SC.INE | 0.2508 |
| TA.ZCE | 0.4053 |

## Stage 3 Decision

结论：`observe`。
- abnormal roll return days: 2
- zero volume days: 2

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
