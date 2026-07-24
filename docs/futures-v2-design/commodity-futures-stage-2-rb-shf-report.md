# TradePilot 商品期货阶段 2：RB.SHF 连续合约构建报告

Generated at: `2026-07-24T07:46:50.462742+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `4a56eebb7e5f6ed4`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/RB.SHF/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`RB.SHF`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

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
| RB.SHF | 4206 | 2009-03-27 .. 2026-07-20 | 55 | 441 | 436 | 898.449 | 896.542 | 3.3400% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2009-07-14 | RB0909.SHF | RB0910.SHF | 26 | 28 | 1.00666 | 1.0551% | 0.3860% |
| 2009-08-05 | RB0910.SHF | RB0911.SHF | -8 | 3 | 0.998362 | 0.5569% | 0.7219% |
| 2009-09-01 | RB0911.SHF | RB0912.SHF | -11 | -7 | 0.997308 | -1.4033% | -1.1372% |
| 2009-10-12 | RB0912.SHF | RB1001.SHF | 58 | 48 | 1.01595 | 1.3996% | -0.1921% |
| 2009-11-09 | RB1001.SHF | RB1002.SHF | 183 | 172 | 1.0466 | 5.6555% | 0.9512% |
| 2009-11-30 | RB1002.SHF | RB1005.SHF | 417 | 399 | 1.10536 | 11.0688% | 0.4824% |
| 2010-03-09 | RB1005.SHF | RB1010.SHF | 441 | 436 | 1.10513 | 10.6708% | 0.1432% |
| 2010-07-16 | RB1010.SHF | RB1101.SHF | 150 | 146 | 1.0383 | 4.0164% | 0.1791% |
| 2010-10-14 | RB1101.SHF | RB1105.SHF | 183 | 182 | 1.04265 | 3.4690% | -0.7632% |
| 2011-02-09 | RB1105.SHF | RB1110.SHF | 111 | 116 | 1.02214 | 2.0311% | -0.1792% |

## Stage 2 Decision

结论：`pass`。`RB.SHF` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
