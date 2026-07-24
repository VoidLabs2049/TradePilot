# TradePilot 商品期货阶段 3：RB.SHF 单品种质量卡

Generated at: `2026-07-24T06:09:40.659383+00:00`
Code version: `ea10455f0834aba1fb62a317d96d77f6306bcfcd-dirty`
Snapshot id: `5e1611d154f96616`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| RB.SHF | 4206 | 2009-03-27 .. 2026-07-20 | 4205 | 1 | 0.0238% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 55 | 0 | 75.1071 | 1.89161e+06 | 1.71359e+06 | 0 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| -0.1368% | 22.0318% | -79.1771% | 6.8790% | -8.1032% | 37 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RB2610.SHF | 3096 | 10 | 吨 | 人民币元/吨 | 30960 | 50000 | 2 | 11920 | 23.8400% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.3615 |
| CU.SHF | 0.4044 |
| I.DCE | 0.7341 |
| M.DCE | 0.1960 |
| P.DCE | 0.2737 |
| SC.INE | 0.1527 |
| TA.ZCE | 0.3273 |

## Stage 3 Decision

结论：`accept`。
- meets fixed Stage 3 quality-card thresholds

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
