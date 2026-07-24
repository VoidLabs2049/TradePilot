# TradePilot 商品期货阶段 2：AU.SHF 连续合约构建报告

Generated at: `2026-07-24T08:19:27.417643+00:00`
Code version: `9bec58bc509e96f91ccf4aa82c530c77d4d1f798-dirty`
Snapshot id: `3674da77ae69e7cd`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/AU.SHF/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`AU.SHF`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

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
| AU.SHF | 4501 | 2008-01-09 .. 2026-07-20 | 54 | 4.5 | 5.38 | 193.895 | 201.237 | 4.7193% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2008-04-22 | AU0806.SHF | AU0812.SHF | -1.47 | -1.28 | 0.992786 | -1.0758% | -0.3570% |
| 2008-10-28 | AU0812.SHF | AU0906.SHF | 0.32 | 0.43 | 1.00195 | 4.9237% | 4.7193% |
| 2009-04-17 | AU0906.SHF | AU0912.SHF | 0.01 | 0 | 1.00005 | -2.2107% | -2.2158% |
| 2009-10-13 | AU0912.SHF | AU1006.SHF | 0.88 | 1.22 | 1.00381 | 0.8009% | 0.4178% |
| 2010-04-14 | AU1006.SHF | AU1012.SHF | 0.5 | 0.55 | 1.00197 | 0.3946% | 0.1973% |
| 2010-10-25 | AU1012.SHF | AU1106.SHF | 0.5 | 1.45 | 1.00172 | 2.4929% | 2.3171% |
| 2011-04-18 | AU1106.SHF | AU1112.SHF | 2.34 | 2.39 | 1.00755 | 1.2649% | 0.5060% |
| 2011-10-26 | AU1112.SHF | AU1206.SHF | -0.26 | 0.37 | 0.999266 | 3.8229% | 3.8991% |
| 2012-04-12 | AU1206.SHF | AU1212.SHF | 0.18 | 0.16 | 1.00053 | -0.1118% | -0.1648% |
| 2012-11-01 | AU1212.SHF | AU1306.SHF | 3.6 | 3.47 | 1.01033 | 1.3621% | 0.3254% |

## Stage 2 Decision

结论：`pass`。`AU.SHF` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
