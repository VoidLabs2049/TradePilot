# TradePilot 商品期货阶段 3：M.DCE 单品种质量卡

Generated at: `2026-07-24T07:46:57.224211+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `2cf7e2d03c3488e5`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| M.DCE | 5000 | 2005-12-20 .. 2026-07-20 | 4999 | 1 | 0.0200% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 62 | 0 | 79.3651 | 969250 | 1386513 | 0 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| 7.3066% | 19.9699% | -47.1680% | 7.6377% | -5.6959% | 13 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| M2609.DCE | 3129 | 10 | 吨 | 人民币元/吨 | 31290 | 50000 | 2 | 12580 | 25.1600% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.2612 |
| CU.SHF | 0.3165 |
| I.DCE | 0.1566 |
| P.DCE | 0.4612 |
| RB.SHF | 0.1960 |
| SC.INE | 0.1710 |
| TA.ZCE | 0.2388 |

## Stage 3 Decision

结论：`accept`。
- meets fixed Stage 3 quality-card thresholds

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
