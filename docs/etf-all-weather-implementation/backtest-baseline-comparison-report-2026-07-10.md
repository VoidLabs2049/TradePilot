# ETF 全天候回测基线对比报告（2026-07-10）

## 目的

本报告归档 `etf_aw_v1 / target_weight_inverse_vol_v1` 与 `static_inverse_vol_v1` 在同一版本回测内核下的正式对比结果。

baseline 权重先写入 `derived.etf_aw_baseline_weight`，策略和 baseline 再分别由 `derived.etf_aw_backtest_kernel` 消费。回测过程中不重新估计风险预算、目标权重或 baseline 权重。

## 运行范围

- 代码版本：本报告所在 commit。
- 分支：`feat/etf-aw-baseline-evaluation-design`。
- 数据区间：`2025-01-01` 至 `2026-05-31`。
- 调仓日历：`etf_aw_v1_monthly_post_20`。
- 当前策略：`etf_aw_v1 / target_weight_inverse_vol_v1`。
- 对比基线：`static_inverse_vol / static_inverse_vol_v1`。
- 日频净值观测：326 行/策略。
- 月度调仓周期：17 个。
- 交易成本：未计入。

当前策略上游 target weight 共 85 行，其中 10 行为 `complete`、75 行为 `partial`。`partial` 主要来自 market-only risk budget 降级，因此本报告只用于工程基线和增量诊断，不作为长期策略优劣结论。

## 运行命令

```bash
python -m tradepilot.etf_aw.cli build-baseline-weight \
  --baseline static-inverse-vol \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli health-check baseline-weight \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli backtest-kernel \
  --strategy target-weight \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli backtest-kernel \
  --strategy baseline \
  --baseline static-inverse-vol \
  --start-date 2025-01-01 \
  --end-date 2026-05-31

python -m tradepilot.etf_aw.cli backtest-report \
  --start-date 2025-01-01 \
  --end-date 2026-05-31 \
  --format markdown
```

## Artifact 验证

| Artifact | 结果 |
| --- | --- |
| `derived.etf_aw_baseline_weight` | 85 行；5 sleeves × 17 个调仓周期；健康检查通过 |
| 当前策略 backtest kernel | 349 行；合同校验通过 |
| baseline backtest kernel | 349 行；合同校验通过 |
| `weight_source_type` | 当前策略为 `target_weight`，基线为 `baseline` |
| `source_weight_dataset` | 分别指向 target weight 和 baseline weight artifact |
| diagnostics | 当前策略与 baseline 均为空 |

旧 backtest kernel 分区缺少 `weight_source_type` 和 `source_weight_dataset` 时，写入边界会按旧合同语义补为 `target_weight` / `derived.etf_aw_target_weight`，再追加 baseline 结果。正式重建不需要删除历史分区。

## 指标对比

差值口径为“当前策略减 baseline”。

| 指标 | 当前策略 | static inverse-vol | 差值 |
| --- | ---: | ---: | ---: |
| 累计收益 | 21.4910% | 21.4344% | +0.0566 个百分点 |
| 年化收益 | 16.2393% | 16.1975% | +0.0418 个百分点 |
| 年化波动 | 6.6479% | 6.5432% | +0.1047 个百分点 |
| 最大回撤 | -4.8348% | -4.8230% | -0.0118 个百分点 |
| Sharpe | 2.4428 | 2.4755 | -0.0327 |
| 月均换手 | 5.0258% | 4.3387% | +0.6871 个百分点 |

## 结论与限制

- 当前策略累计收益仅略高于静态 inverse-vol baseline。
- 当前策略的年化波动和月均换手更高，Sharpe 略低，尚未显示明确的风险调整后优势。
- 两条回测均无 blocking diagnostic。
- 样本只覆盖 17 个调仓周期，且未计入交易成本；更高换手可能进一步削弱当前策略的净收益优势。
- 下一阶段如继续评估，应优先延长 point-in-time 样本并增加成本敏感性，而不是直接引入更复杂优化器。
