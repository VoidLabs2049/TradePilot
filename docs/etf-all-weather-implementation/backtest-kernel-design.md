# ETF 全天候回测内核设计

## 目的

本文定义在 `derived.etf_aw_risk_budget` 和 `derived.etf_aw_target_weight` 之前应先实现的小型回测内核。

该内核是验收夹具，不是完整评估层。它的职责是在后续策略层开发期间，为基础收益路径、回撤和换手行为提供客观检查。

## 范围

内核输入：

- 月度 sleeve 权重。
- 来自 `derived.etf_aw_sleeve_daily` 的日频复权 sleeve 收益。
- 来自 `reference.rebalance_calendar.monthly_post_20` 的调仓日期。

内核输出：

- 日频净值曲线。
- 年化收益。
- 年化波动。
- Sharpe ratio。
- 最大回撤。
- 月度换手。
- 缺失收益、缺失权重和调仓日对齐诊断。

## 非范围

该内核不得生成策略决策。

它不包含：

- regime 评分。
- risk budget 映射。
- target weight 优化。
- baseline comparison pack。
- 交易成本模型。
- 参数扰动。
- Dashboard 展示。
- 交易建议生成。

这些内容分别属于 `risk-budget-design.md`、`target-weight-design.md` 和后续的 `backtest-evaluation-design.md`。

## 接口边界

实现应遵循现有 derived dataset 风格：

```text
_build_*() -> _make_*_frame(df) -> _write_*()
```

核心净值和指标计算应放在纯函数中，确保测试不依赖网络或外部服务。

## 最小 Fixture

第一个 fixture 应对 frozen v1 sleeves 使用月度等权：

- `510300.SH`
- `159845.SZ`
- `511010.SH`
- `518850.SH`
- `159001.SZ`

成功标准是该 fixture 能确定性地产出：

- 非空净值曲线。
- 每个被测 strategy 一行指标。
- 月度换手行。
- 不包含未来函数式 join。

## 计算规则

日频组合收益是在应用最新生效月度权重后，对 sleeve 日频收益做加权求和。

权重在 `reference.rebalance_calendar.monthly_post_20` 定义的调仓日生效。如果后续实现选择下一交易日执行，必须在 schema 和测试中显式写清楚该 effective-date 规则。

换手在调仓日按以下公式度量：

```text
0.5 * sum(abs(new_weight - drifted_weight_before_rebalance))
```

如果第一版实现暂时没有 drifted weights，初始 MVP 可以使用：

```text
0.5 * sum(abs(new_weight - previous_target_weight))
```

但必须把该限制写入 diagnostics。

## 验证要求

测试应覆盖：

- 收益和权重完整。
- 某个交易日缺失 sleeve 收益。
- 某个调仓日缺失单个 sleeve 权重。
- 重复权重行。
- 调仓日没有匹配交易日。
- 小型 fixture 的指标输出确定性。

内核不得在缺失调仓行时静默 forward-fill strategy 权重。缺失权重应根据实现时选定的数据集合同，产生 degraded diagnostic 或 validation failure。

## 验收用途

`target-weight-design.md` 应使用该内核验证 target-weight 序列：

- 能产生确定性收益路径。
- 不会因为浮点噪声产生异常换手。
- 开发期间至少可以和等权 fixture 对比。

完整 baseline 对比、成本假设和参数扰动仍属于后续评估层。
