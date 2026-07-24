# TradePilot 商品期货阶段 2：TA.ZCE 连续合约构建报告

Generated at: `2026-07-24T07:47:06.028268+00:00`
Code version: `0e899f570a8796f015f3ab0f1980928681390da2-dirty`
Snapshot id: `78798cc9d13580dd`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_continuous_contract/TA.ZCE/part-00000.parquet`

## Scope

本报告只覆盖 Stage 2 的单品种主力连续合约：`TA.ZCE`。不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。

## Frozen Method

- 主力选择：逐日遵循冻结的 `market.futures_mapping`。
- 复权公式：比值法后向复权。
- 样本截断：因截断日前存在无法同日定位新旧合约价格的换月，本报告只使用 `2008-09-16` 起的映射行。
- `adjusted_close`：每个换月日用 `new_close / old_close` 调整所有更早历史段。
- `adjusted_settle`：每个换月日用 `new_settle / old_settle` 调整所有更早历史段。
- 绝对 `roll_gap` / `settle_roll_gap` 仍保留为换月价差审计字段。
- 绩效主口径：`continuous_return = adjusted_close.pct_change()`。
- 审计对照口径：`settle_return_audit = adjusted_settle.pct_change()`。

## Summary

| Root | Rows | Window | Rolls | Max roll close gap | Max roll settle gap | Min adjusted close | Min adjusted settle | Max abs roll return |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TA.ZCE | 4332 | 2008-09-16 .. 2026-07-20 | 59 | 448 | 466 | 3211.8 | 3178.65 | 5.7550% |

## Roll Sample

| Trade date | From | To | Roll gap | Settle roll gap | Roll ratio | Naive return | Continuous return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2008-10-16 | TA0811.ZCE | TA0901.ZCE | -266 | -260 | 0.956336 | -9.8700% | -5.7550% |
| 2008-12-08 | TA0901.ZCE | TA0905.ZCE | 326 | 304 | 1.0682 | 9.2426% | 2.2679% |
| 2009-04-08 | TA0905.ZCE | TA0909.ZCE | -222 | -206 | 0.967401 | -5.7511% | -2.5751% |
| 2009-08-12 | TA0909.ZCE | TA0910.ZCE | 52 | 22 | 1.00634 | 0.0727% | -0.5577% |
| 2009-09-07 | TA0910.ZCE | TA0911.ZCE | -2 | 16 | 0.999726 | 1.3626% | 1.3904% |
| 2009-10-09 | TA0911.ZCE | TA0912.ZCE | -18 | -22 | 0.997313 | 0.1800% | 0.4499% |
| 2009-10-27 | TA0912.ZCE | TA1001.ZCE | 32 | 42 | 1.00428 | 0.0267% | -0.4001% |
| 2009-12-03 | TA1001.ZCE | TA1002.ZCE | 110 | 108 | 1.0137 | 1.9544% | 0.5763% |
| 2009-12-30 | TA1002.ZCE | TA1003.ZCE | 120 | 130 | 1.01505 | 1.4791% | -0.0251% |
| 2010-01-21 | TA1003.ZCE | TA1005.ZCE | 208 | 196 | 1.02501 | 3.0955% | 0.5804% |

## Stage 2 Decision

结论：`pass`。`TA.ZCE` 连续序列已按冻结的比值法后向复权生成，换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。

该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，也不代表商品篮子或 ETF 基线增量回测可以直接开始。
