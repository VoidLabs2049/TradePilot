# ETF 全天候风险预算人工检查记录

检查日期：2026-07-06

检查对象：

- `derived.etf_aw_strategy_context`
- `derived.etf_aw_risk_budget`
- `derived.etf_aw_target_weight`
- `derived.etf_aw_backtest_kernel`

## 原始问题

`derived.etf_aw_risk_budget` 共有 85 行，覆盖 17 个调仓月。初始状态为：

```text
unavailable    75
complete       10
```

对应日期：

```text
2025-01-20 to 2026-03-20: unavailable
2026-04-20: complete
2026-05-20: complete
```

`quality_notes_json.reasons` 显示早期月份的直接原因均为：

```text
strategy_context_unavailable
```

## 根因链路

进一步检查 `derived.etf_aw_strategy_context` 后确认：

```text
macro/rates 基础数据历史覆盖不足
-> macro_rates_context_status = unavailable
-> strategy_context.context_status = unavailable
-> risk_budget.budget_status = unavailable
-> target_weight.target_weight_status = unavailable
```

市场侧并不是主阻塞。早期月份的 `market_regime_label` 已存在，且 regime score 可用；阻塞来自 Stage G 对 macro/rates 缺失的硬降级。

旧规则为：

```python
if macro_rates_context_status == "unavailable":
    return "unavailable", "not_ready"
```

这会导致 market-only 场景无法继续生成可审计的 `partial` risk budget。

## 修复策略

按 V1 边界，宏观/利率字段用于上下文和置信度校正，在未完成充分验证前不应单独触发大幅仓位切换，也不应阻断 market-only risk budget。

已将 Stage G 规则调整为：

```python
if macro_rates_context_status == "unavailable":
    return "partial", "degraded_research"
```

下游 `derived.etf_aw_risk_budget` 已有降级规则：

```text
context_status = partial 或 readiness_level = degraded_research
-> budget_status = partial
-> confidence cap = 0.35
```

因此 market-only 历史月份会输出受限主动 tilt 的 `partial` 风险预算，而不是 `unavailable`。

## 历史数据补数

同时尝试补齐 2025-01 到 2026-05 的 macro/rates 历史数据：

```python
from datetime import date
from tradepilot.etl.models import IngestionRequest, TriggerMode
from tradepilot.etl.service import ETLService

service = ETLService()
request = IngestionRequest(
    request_start=date(2025, 1, 1),
    request_end=date(2026, 5, 31),
    trigger_mode=TriggerMode.MANUAL,
    context={},
)
for dataset in [
    "macro.slow_fields",
    "rates.daily_rates",
    "rates.lpr",
    "rates.gov_curve_points",
]:
    result = service.run_dataset_sync(dataset, request)
    print(dataset, result.status.value, result.records_written, result.error_message)
```

结果：

```text
macro.slow_fields       success  17
rates.daily_rates       success  694
rates.lpr               success  30
rates.gov_curve_points  failed   0
```

`rates.gov_curve_points` 失败原因：

```text
Tushare yc_cb 接口无访问权限
```

当前本地基础数据覆盖：

| Dataset | Rows | Date range |
| --- | ---: | --- |
| `macro.slow_fields` | 17 | 2025-02-05 to 2026-06-01 |
| `rates.daily_rates` | 704 | 2025-01-02 to 2026-06-05 |
| `rates.lpr` | 30 | 2025-01-20 to 2026-03-20 |
| `rates.gov_curve_points` | 16 | 2026-04-17 to 2026-06-05 |

## 重建结果

重跑：

```python
from datetime import date
from tradepilot.etl.service import ETLService

service = ETLService()
for profile in [
    "derived.etf_aw_strategy_context.build",
    "derived.etf_aw_risk_budget.build",
    "derived.etf_aw_target_weight.build",
    "derived.etf_aw_backtest_kernel.build",
]:
    print(service.run_bootstrap(
        profile,
        start=date(2025, 1, 1),
        end=date(2026, 5, 31),
    ))
```

`derived.etf_aw_strategy_context`：

```text
partial     15
complete     2
```

`derived.etf_aw_risk_budget`：

```text
partial     75
complete    10
```

`derived.etf_aw_target_weight`：

```text
partial        70
complete       10
unavailable     5
```

`derived.etf_aw_backtest_kernel`：

```text
daily_nav    326
turnover      17
metric         6
```

## 风险预算检查

检查结果：

- 每个 rebalance date 有 5 个 sleeve。
- 每期 `base_budget` 合计为 1。
- 每期 `tilted_budget` 合计为 1。
- `partial` 月份使用受限 confidence，最大 `effective_confidence_score` 为 0.35。
- `complete` 月份可使用完整 confidence cap，最大为 0.70。
- 早期月份不再是 `unavailable_neutral_budget`，而是 market-only 降级后的 `partial` risk budget。
- 国债曲线仍缺历史数据，因此早期月份不能升级为 `complete`。

## 后续复核结果

`2025-03-20` 的 5 行 `derived.etf_aw_target_weight` 曾为 `unavailable`。复核后确认：

- 目标权重窗口有足够 sleeve daily 样本。
- 对应 risk budget 已是 `partial`，不是旧的 `unavailable` 状态。
- 真正阻断点是 risk budget 的 5 个 `tilted_budget` 四舍五入后合计为 `1.0000010000000001`，略超 target weight 上游校验的 `1e-6` 容忍线。

现已修复 risk budget rounding drift，并重建 2025-03 artifact。`2025-03-20` target weight 已降级输出为 `partial`，不再是 `unavailable`。

## 结论

两个方向都已推进：

1. 代码语义修复：market-only 且市场上下文完整时，Stage G 不再因为 macro/rates 缺失硬阻断，改为 `partial / degraded_research`。
2. 历史数据补齐：PMI、SHIBOR、LPR 已补到本地；国债曲线因 Tushare 权限不足仍缺历史区间。

当前 risk budget 已从 75 行 `unavailable` 改善为 75 行 `partial`，10 行 `complete`。target weight 当前为 75 行 `partial`、10 行 `complete`、0 行 `unavailable`。这使 target weight 和 backtest kernel 可以消费更多历史月份，但正式策略评价仍应区分 `partial` 与 `complete`。
