# TradePilot 商品期货阶段 2：SC.INE 连续合约构建报告

Generated at: `2026-07-24T07:47:02.891035+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `1876043416ca96f0`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/SC.INE/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`SC.INE`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

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
| SC.INE | 2018 | 2018-03-26 .. 2026-07-20 | 93 | 51.3 | 49.8 | 173.441 | 178.963 | 6.3008% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2018-08-09 | SC1809.INE | SC1812.INE | -5.9 | -5.5 | 0.988634 | -1.9113% | -0.7836% |
| 2018-11-19 | SC1812.INE | SC1901.INE | 6.6 | 7.6 | 1.01417 | 1.2213% | -0.1928% |
| 2018-12-18 | SC1901.INE | SC1903.INE | 11.1 | 8.7 | 1.02743 | -0.4787% | -3.1355% |
| 2019-02-18 | SC1903.INE | SC1904.INE | 8.1 | 7.9 | 1.01788 | 3.7812% | 1.9581% |
| 2019-03-18 | SC1904.INE | SC1905.INE | -1.6 | -0.4 | 0.996466 | -0.8352% | -0.4835% |
| 2019-04-17 | SC1905.INE | SC1906.INE | 4.2 | 3.7 | 1.0089 | 2.2981% | 1.3960% |
| 2019-05-21 | SC1906.INE | SC1907.INE | -0.7 | 0.7 | 0.998626 | -2.2286% | -2.0941% |
| 2019-06-18 | SC1907.INE | SC1908.INE | 5.4 | -0.5 | 1.01323 | -2.9343% | -4.2019% |
| 2019-07-16 | SC1908.INE | SC1909.INE | 3.3 | 1.7 | 1.00731 | 0.0880% | -0.6381% |
| 2019-08-15 | SC1909.INE | SC1910.INE | -2.4 | -2.1 | 0.994318 | -2.6877% | -2.1316% |

## Stage 2 Decision

结论：`pass`。`SC.INE` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
