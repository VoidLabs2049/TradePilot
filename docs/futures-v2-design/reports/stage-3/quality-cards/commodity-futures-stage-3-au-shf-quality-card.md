# TradePilot 商品期货阶段 3：AU.SHF 单品种质量卡

Generated at: `2026-07-24T08:19:36.256720+00:00`
Code version: `9bec58bc509e96f91ccf4aa82c530c77d4d1f798-dirty`
Snapshot id: `ad1acb11454a9c58`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| AU.SHF | 4501 | 2008-01-09 .. 2026-07-20 | 4500 | 1 | 0.0222% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 54 | 0 | 81.8364 | 122413 | 157040 | 0 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| 6.4016% | 17.6486% | -47.4913% | 8.4454% | -13.1580% | 17 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| AU2608.SHF | 874.24 | 1000 | 克 | 人民币元/克 | 874240 | 50000 | 1 | 824240 | 1648.4800% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.1971 |
| CU.SHF | 0.2993 |
| I.DCE | 0.0439 |
| M.DCE | 0.1423 |
| P.DCE | 0.1628 |
| RB.SHF | 0.0758 |
| SC.INE | 0.0082 |
| TA.ZCE | 0.1330 |

## Stage 3 Decision

结论：`accept`。
- meets fixed Stage 3 quality-card thresholds

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
