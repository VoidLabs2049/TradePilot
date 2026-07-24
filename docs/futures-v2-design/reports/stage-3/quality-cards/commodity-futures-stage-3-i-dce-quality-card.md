# TradePilot 商品期货阶段 3：I.DCE 单品种质量卡

Generated at: `2026-07-24T07:46:54.074230+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `1d1397180a0c5f87`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| I.DCE | 3102 | 2013-10-18 .. 2026-07-20 | 3101 | 1 | 0.0322% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 37 | 2 | 81.6316 | 769944 | 705473 | 0 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| 16.1615% | 34.5326% | -60.5983% | 9.5333% | -9.9807% | 119 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| I2609.DCE | 758 | 100 | 吨 | 人民币元/吨 | 75800 | 50000 | 1 | 25800 | 51.6000% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.3148 |
| CU.SHF | 0.3988 |
| M.DCE | 0.1566 |
| P.DCE | 0.2286 |
| RB.SHF | 0.7341 |
| SC.INE | 0.1806 |
| TA.ZCE | 0.3034 |

## Stage 3 Decision

结论：`observe`。
- abnormal roll return days: 2

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
