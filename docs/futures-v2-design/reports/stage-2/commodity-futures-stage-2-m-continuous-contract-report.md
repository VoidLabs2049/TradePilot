# TradePilot 商品期货阶段 2：M.DCE 连续合约构建报告

Generated at: `2026-07-24T07:46:56.306338+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `e12fae76019a003a`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/M.DCE/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`M.DCE`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

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
| M.DCE | 5000 | 2005-12-20 .. 2026-07-20 | 62 | 626 | 636 | 587.035 | 589.063 | 4.5591% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2006-02-20 | M0605.DCE | M0609.DCE | 100 | 99 | 1.0432 | 3.7371% | -0.5584% |
| 2006-05-12 | M0609.DCE | M0611.DCE | 91 | 82 | 1.03907 | 4.5356% | 0.6048% |
| 2006-07-31 | M0611.DCE | M0701.DCE | 76 | 74 | 1.03396 | 4.9433% | 1.4966% |
| 2006-10-20 | M0701.DCE | M0705.DCE | 106 | 109 | 1.04668 | 4.3459% | -0.3073% |
| 2007-01-30 | M0705.DCE | M0709.DCE | 130 | 131 | 1.05367 | 4.6760% | -0.6563% |
| 2007-07-04 | M0709.DCE | M0801.DCE | 119 | 114 | 1.04777 | 3.8599% | -0.8754% |
| 2007-08-24 | M0801.DCE | M0805.DCE | 96 | 87 | 1.03427 | 2.8034% | -0.6033% |
| 2007-12-13 | M0805.DCE | M0809.DCE | -106 | -100 | 0.969435 | -1.1758% | 1.9400% |
| 2008-06-26 | M0809.DCE | M0901.DCE | -177 | -207 | 0.957656 | -6.0108% | -1.8549% |
| 2008-10-31 | M0901.DCE | M0905.DCE | -123 | -123 | 0.956024 | -7.1528% | -2.8819% |

## Stage 2 Decision

结论：`pass`。`M.DCE` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
