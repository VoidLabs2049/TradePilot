# TradePilot 商品期货阶段 2：P.DCE 连续合约构建报告

Generated at: `2026-07-24T07:46:59.380001+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `16050566c6d55962`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/P.DCE/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`P.DCE`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

## Frozen Method

- 主力选择：逐日遵循冻结的 `market.futures_mapping`。
- 复权公式：比值法后向复权。
- `adjusted_close`：每个换月日用 `new_close / old_close` 调整所有更早历史段。
- `adjusted_settle`：每个换月日用 `new_settle / old_settle` 调整所有更早历史段。
- 绝对 `roll_gap` / `settle_roll_gap` 仍保留为换月价差审计字段。
- 绩效主口径：`continuous_return = adjusted_close.pct_change()`。
- 审计对照口径：`settle_return_audit = adjusted_settle.pct_change()`。

## Summary

| Root | Rows | Window | Rolls | Max roll close gap | Max roll settle gap | Min adjusted close | Min adjusted settle | Max abs roll return |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P.DCE | 4551 | 2007-10-29 .. 2026-07-20 | 56 | 1262 | 1346 | 2024.67 | 2022.21 | 4.1768% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2008-04-18 | P0805.DCE | P0809.DCE | 134 | 154 | 1.01257 | 1.0674% | -0.1873% |
| 2008-08-07 | P0809.DCE | P0901.DCE | 218 | 94 | 1.02812 | 2.1009% | -0.6918% |
| 2008-11-27 | P0901.DCE | P0905.DCE | 234 | 252 | 1.05154 | 4.3725% | -0.7433% |
| 2009-03-04 | P0905.DCE | P0909.DCE | 0 | 2 | 1 | 1.1058% | 1.1058% |
| 2009-06-09 | P0909.DCE | P1001.DCE | -208 | -212 | 0.969176 | -3.0824% | 0.0000% |
| 2009-08-10 | P1001.DCE | P1005.DCE | 300 | 302 | 1.04727 | 8.9866% | 4.0669% |
| 2009-11-27 | P1005.DCE | P1009.DCE | 56 | 70 | 1.00866 | -2.3639% | -3.2017% |
| 2010-04-28 | P1009.DCE | P1101.DCE | 12 | 34 | 1.00173 | -0.8540% | -1.0248% |
| 2010-09-08 | P1101.DCE | P1105.DCE | 204 | 214 | 1.02836 | 2.5229% | -0.3050% |
| 2010-11-05 | P1105.DCE | P1109.DCE | 282 | 276 | 1.03133 | 6.7879% | 3.5435% |

## Stage 2 Decision

结论：`pass`。`P.DCE` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
