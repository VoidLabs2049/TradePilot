# TradePilot 商品期货阶段 2：AL.SHF 连续合约构建报告

Generated at: `2026-07-24T07:46:41.101208+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `ba8cd989e24898a9`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/AL.SHF/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`AL.SHF`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

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
| AL.SHF | 5000 | 2005-12-20 .. 2026-07-20 | 244 | 470 | 450 | 8677.96 | 8853 | 6.0976% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2006-01-11 | AL0603.SHF | AL0604.SHF | 110 | 110 | 1.00561 | 0.3561% | -0.2035% |
| 2006-01-27 | AL0604.SHF | AL0605.SHF | 170 | 170 | 1.00797 | 2.9201% | 2.1063% |
| 2006-03-20 | AL0605.SHF | AL0606.SHF | 160 | 150 | 1.00797 | 2.7933% | 1.9807% |
| 2006-04-12 | AL0606.SHF | AL0607.SHF | 150 | 150 | 1.0074 | -0.8742% | -1.6027% |
| 2006-05-11 | AL0607.SHF | AL0608.SHF | 200 | 200 | 1.00852 | 4.2731% | 3.3921% |
| 2006-06-15 | AL0608.SHF | AL0609.SHF | 80 | 80 | 1.00401 | 1.8302% | 1.4235% |
| 2006-07-14 | AL0609.SHF | AL0610.SHF | 60 | 50 | 1.00307 | -0.1527% | -0.4580% |
| 2006-08-21 | AL0610.SHF | AL0611.SHF | -210 | -200 | 0.988758 | -0.8056% | 0.3222% |
| 2006-09-22 | AL0611.SHF | AL0612.SHF | -470 | -450 | 0.976687 | 0.0508% | 2.4390% |
| 2006-10-20 | AL0612.SHF | AL0701.SHF | -170 | -110 | 0.991916 | -0.1914% | 0.6220% |

## Stage 2 Decision

结论：`pass`。`AL.SHF` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
