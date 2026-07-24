# TradePilot 商品期货阶段 2：I.DCE 连续合约构建报告

Generated at: `2026-07-24T06:09:42.454098+00:00`
Code version: `ea10455f0834aba1fb62a317d96d77f6306bcfcd-dirty`
Snapshot id: `442326ee286c000a`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/I.DCE/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`I.DCE`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

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
| I.DCE | 3102 | 2013-10-18 .. 2026-07-20 | 37 | 134.5 | 129 | 47.2669 | 47.8973 | 5.6232% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2014-02-27 | I1405.DCE | I1409.DCE | -6 | -5 | 0.992718 | -0.2439% | 0.4878% |
| 2014-07-23 | I1409.DCE | I1501.DCE | -12 | -16 | 0.98243 | -2.1866% | -0.4373% |
| 2014-10-23 | I1501.DCE | I1505.DCE | -42 | -44 | 0.926445 | -7.8397% | -0.5226% |
| 2015-03-09 | I1505.DCE | I1509.DCE | -8 | -9 | 0.982684 | -0.4386% | 1.3158% |
| 2015-07-21 | I1509.DCE | I1601.DCE | -28 | -24.5 | 0.927835 | -5.6356% | 1.7038% |
| 2015-11-23 | I1601.DCE | I1605.DCE | -35 | -32.5 | 0.895522 | -10.0450% | 0.4498% |
| 2016-03-21 | I1605.DCE | I1609.DCE | -33.5 | -33.5 | 0.926535 | -6.0067% | 1.4461% |
| 2016-08-11 | I1609.DCE | I1701.DCE | -56 | -54 | 0.88501 | -12.7530% | -1.4170% |
| 2016-11-25 | I1701.DCE | I1705.DCE | -40.5 | -42.5 | 0.938026 | -1.4469% | 5.0643% |
| 2017-03-22 | I1705.DCE | I1709.DCE | -88.5 | -85.5 | 0.867017 | -16.0727% | -3.2000% |

## Stage 2 Decision

结论：`pass`。`I.DCE` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
