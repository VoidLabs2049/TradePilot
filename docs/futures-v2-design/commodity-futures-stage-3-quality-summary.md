# TradePilot 商品期货阶段 3：非黄金候选质量汇总

Generated at: `2026-07-24T07:47:07.096787+00:00`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本汇总覆盖 Stage 4 第一轮非黄金候选：`AL.SHF / CU.SHF / RB.SHF / I.DCE / M.DCE / P.DCE / SC.INE / TA.ZCE`。它只汇总 Stage 2 连续合约构建和 Stage 3 单品种质量筛选结果，不构建商品篮子。

## Candidate Decisions

| Root | Stage 2 | Stage 3 | Rows | Window | Rolls | Ann return | Ann volatility | Max drawdown | Reasons |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| AL.SHF | pass | observe | 5000 | 2005-12-20 .. 2026-07-20 | 244 | 0.0331% | 17.1178% | -69.2236% | abnormal roll return days: 1<br>zero volume days: 1 |
| CU.SHF | pass | observe | 5000 | 2005-12-20 .. 2026-07-20 | 247 | 6.7914% | 22.7735% | -68.5182% | abnormal roll return days: 2<br>zero volume days: 2 |
| RB.SHF | pass | accept | 4206 | 2009-03-27 .. 2026-07-20 | 55 | -0.1368% | 22.0318% | -79.1771% | meets fixed Stage 3 quality-card thresholds |
| I.DCE | pass | observe | 3102 | 2013-10-18 .. 2026-07-20 | 37 | 16.1615% | 34.5326% | -60.5983% | abnormal roll return days: 2 |
| M.DCE | pass | accept | 5000 | 2005-12-20 .. 2026-07-20 | 62 | 7.3066% | 19.9699% | -47.1680% | meets fixed Stage 3 quality-card thresholds |
| P.DCE | pass | observe | 4551 | 2007-10-29 .. 2026-07-20 | 56 | 1.5491% | 24.2764% | -80.8367% | max drawdown is -80.8367% |
| SC.INE | pass | observe | 2018 | 2018-03-26 .. 2026-07-20 | 93 | 1.3914% | 38.0835% | -74.8639% | abnormal roll return days: 2 |
| TA.ZCE | pass | observe | 4332 | 2008-09-16 .. 2026-07-20 | 59 | -1.2669% | 24.0518% | -69.6248% | Stage 2 sample starts at 2008-09-16 after excluded unauditable rolls<br>abnormal roll return days: 1 |

## Stage 4 Readiness

结论：`ready_for_stage4_rule_freeze`。所有非黄金候选均已有 Stage 2 连续合约产物和 Stage 3 质量卡。
