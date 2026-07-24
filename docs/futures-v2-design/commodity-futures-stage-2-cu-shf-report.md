# TradePilot 商品期货阶段 2：CU.SHF 连续合约构建报告

Generated at: `2026-07-24T06:09:36.800182+00:00`
Code version: `ea10455f0834aba1fb62a317d96d77f6306bcfcd-dirty`
Snapshot id: `11354d751701f7f4`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/CU.SHF/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`CU.SHF`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

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
| CU.SHF | 5000 | 2005-12-20 .. 2026-07-20 | 247 | 1380 | 1360 | 19246.6 | 20072.4 | 6.3514% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2005-12-26 | CU0602.SHF | CU0603.SHF | -520 | -400 | 0.987678 | 1.8075% | 3.0777% |
| 2006-01-24 | CU0603.SHF | CU0604.SHF | -510 | -530 | 0.988493 | -0.7701% | 0.3851% |
| 2006-02-24 | CU0604.SHF | CU0605.SHF | -470 | -430 | 0.989962 | 0.0000% | 1.0140% |
| 2006-03-23 | CU0605.SHF | CU0606.SHF | -260 | -380 | 0.994651 | 0.1035% | 0.6418% |
| 2006-04-19 | CU0606.SHF | CU0607.SHF | 20 | 40 | 1.00032 | 2.6789% | 2.6456% |
| 2006-05-16 | CU0607.SHF | CU0608.SHF | 190 | 200 | 1.00237 | -3.6864% | -3.9145% |
| 2006-06-16 | CU0608.SHF | CU0609.SHF | 420 | 420 | 1.007 | 1.7514% | 1.0441% |
| 2006-07-21 | CU0609.SHF | CU0610.SHF | 280 | 280 | 1.0044 | -2.9045% | -3.3303% |
| 2006-09-07 | CU0610.SHF | CU0611.SHF | 10 | -30 | 1.00013 | 0.9739% | 0.9604% |
| 2006-10-18 | CU0611.SHF | CU0612.SHF | -10 | -30 | 0.99986 | -1.2707% | -1.2569% |

## Stage 2 Decision

结论：`pass`。`CU.SHF` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
