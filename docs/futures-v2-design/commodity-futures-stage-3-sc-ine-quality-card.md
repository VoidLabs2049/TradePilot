# TradePilot 商品期货阶段 3：SC.INE 单品种质量卡

Generated at: `2026-07-24T06:09:52.898666+00:00`
Code version: `ea10455f0834aba1fb62a317d96d77f6306bcfcd-dirty`
Snapshot id: `326b8d2f61d3af6f`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。

## History And Continuity

| Root | Rows | Window | Return rows | Missing returns | Missing rate | Duplicate dates |
| --- | --- | --- | --- | --- | --- | --- |
| SC.INE | 2018 | 2018-03-26 .. 2026-07-20 | 2017 | 1 | 0.0496% | 0 |

## Roll And Liquidity

| Rolls | Abnormal roll returns | Avg holding days | Median volume | Median OI | Zero volume days | Zero OI days |
| --- | --- | --- | --- | --- | --- | --- |
| 93 | 2 | 21.4681 | 131400 | 32225.5 | 0 | 0 |

## Return And Drawdown

| Ann return | Ann volatility | Max drawdown | Max daily gain | Max daily loss | Extreme days |
| --- | --- | --- | --- | --- | --- |
| 1.3914% | 38.0835% | -74.8639% | 16.0951% | -13.6693% | 93 |

## Integer-Lot Sizing Hint

| Latest contract | Latest close | Multiplier | Trade unit | Quote unit | One-lot notional | Target notional | Nearest lots | Lot error | Lot error % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SC2609.INE | 548.4 | 1000 | 桶 | 人民币元/桶 | 548400 | 50000 | 1 | 498400 | 996.8000% |

## Peer Correlation

| Peer | Correlation |
| --- | --- |
| AL.SHF | 0.2431 |
| CU.SHF | 0.2508 |
| I.DCE | 0.1806 |
| M.DCE | 0.1710 |
| P.DCE | 0.3955 |
| RB.SHF | 0.1527 |
| TA.ZCE | 0.6317 |

## Stage 3 Decision

结论：`observe`。
- abnormal roll return days: 2

该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选逐一质量卡完成后冻结权重规则。
