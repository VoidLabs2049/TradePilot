# ETF 全天候 CLI 设计

## 目的

本文定义 ETF 全天候后端命令行主线。

CLI 的职责是把已经设计好的数据集构建、健康检查和回测步骤串成可重复执行的工程入口。它不是新的策略层，不在命令运行时做研究选参，也不替代后续 Dashboard / workflow read model。

设计借鉴 TradeSimulator 的命令边界：

```text
data -> train-suite -> backtest
```

ETF 全天候对应为：

```text
sync-data
-> build-risk-budget
-> health-check risk-budget
-> build-target-weight
-> health-check target-weight
-> backtest-kernel
-> backtest-report
```

## 总原则

- 每个命令只做一件事，并写出明确 artifact。
- 回测命令只消费 frozen artifact，不重新生成风险预算或目标权重。
- 命令使用 `click` 实现，遵循 TradePilot 现有 Python 规则。
- 命令通过 `python -m ...` 运行，不引入额外包管理工具。
- 复杂参数优先进入配置或数据合同，不把 CLI 做成临时实验参数集合。
- 所有命令应能在无网络测试 fixture 中验证核心逻辑。

## 建议入口

建议新增模块：

```text
tradepilot/etf_aw/cli.py
```

建议运行方式：

```bash
python -m tradepilot.etf_aw.cli sync-data
python -m tradepilot.etf_aw.cli build-risk-budget
python -m tradepilot.etf_aw.cli health-check risk-budget
python -m tradepilot.etf_aw.cli build-target-weight
python -m tradepilot.etf_aw.cli health-check target-weight
python -m tradepilot.etf_aw.cli backtest-kernel
python -m tradepilot.etf_aw.cli backtest-report
```

如果实现时不新增 `tradepilot/etf_aw/` 包，也可以挂到现有 workflow / derived dataset CLI 下，但命令边界应保持不变。

## 命令设计

### `sync-data`

职责：

- 运行 ETF 全天候依赖的数据同步或本地构建入口。
- 确保 `reference.*`、`market.*`、`macro.*`、`rates.*` 和已落地 derived context 可用。

输入：

- 项目配置。
- 可选 `--start-date` / `--end-date`，仅作为同步窗口。

输出：

- 已有 lakehouse / DuckDB 数据集。
- ingestion run 或等价诊断记录。

非范围：

- 不生成风险预算。
- 不生成目标权重。
- 不运行回测。

### `build-risk-budget`

职责：

- 从 `derived.etf_aw_strategy_context` 和 `derived.etf_aw_regime_score` 生成 `derived.etf_aw_risk_budget`。
- 写出前执行 risk budget 健康检查。

输入：

- `strategy_name`，默认 `etf_aw_v1`。
- 可选 `--start-date` / `--end-date`。

输出：

- `derived.etf_aw_risk_budget`
- health findings，写入日志或后续健康检查 artifact。

失败条件：

- 来源 strategy context 缺失且无法按设计降级。
- 健康检查出现必须阻断的 FAIL。
- business key 重复。

非范围：

- 不估计协方差。
- 不生成 ETF 权重。
- 不运行回测。

### `health-check risk-budget`

职责：

- 对已写出的 `derived.etf_aw_risk_budget` 运行健康检查。
- 输出 FAIL / WARN 摘要。

输入：

- `strategy_name`
- 可选 `--as-of-date` 或日期窗口。

输出：

- 终端摘要。
- 可选 health check artifact，例如 `derived.etf_aw_health_check`。

失败条件：

- 任一 FAIL finding。

### `build-target-weight`

职责：

- 消费已通过健康检查的 `derived.etf_aw_risk_budget`。
- 读取 `derived.etf_aw_sleeve_daily` 估计波动率或协方差。
- 生成 `derived.etf_aw_target_weight`。
- 写出前执行 target weight 健康检查。

输入：

- `strategy_name`。
- `strategy_version`。
- 可选 `--start-date` / `--end-date`。

输出：

- `derived.etf_aw_target_weight`
- explainability / quality notes
- health findings

失败条件：

- 来源 risk budget 缺失或未通过健康检查。
- 协方差样本不足且无法按设计降级。
- 权重健康检查出现必须阻断的 FAIL。

非范围：

- 不读取当前持仓。
- 不生成订单。
- 不运行回测。

### `health-check target-weight`

职责：

- 对已写出的 `derived.etf_aw_target_weight` 运行健康检查。
- 输出 FAIL / WARN 摘要。

输入：

- `strategy_name`
- `strategy_version`
- 可选 `--as-of-date` 或日期窗口。

输出：

- 终端摘要。
- 可选 health check artifact。

失败条件：

- 任一 FAIL finding。

### `backtest-kernel`

职责：

- 消费 frozen target weight artifact。
- 调用前置回测内核生成净值、指标、换手和 diagnostics。

输入：

- `strategy_name`
- `strategy_version`
- 已冻结的 `derived.etf_aw_target_weight`
- `derived.etf_aw_sleeve_daily`
- `reference.rebalance_calendar.monthly_post_20`

输出：

- `derived.etf_aw_backtest_kernel`

硬边界：

- 不重新生成 risk budget。
- 不重新生成 target weight。
- 不按回测表现动态搜索参数。
- 不做 baseline comparison pack。

### `backtest-report`

职责：

- 读取 `derived.etf_aw_backtest_kernel`。
- 生成命令行或本地文件报表。

输入：

- `strategy_name`
- `strategy_version`
- 可选 `--format markdown|json`

输出：

- 净值、回撤、指标、换手和 diagnostics 摘要。
- Phase 0 可先输出 markdown / JSON，本地 HTML 或前端展示后置。

非范围：

- 不生成新权重。
- 不修改策略 artifact。
- 不启动自动交易。

## Frozen Artifact 规则

CLI 主线必须保持以下顺序：

```text
build-risk-budget
-> health-check risk-budget
-> build-target-weight
-> health-check target-weight
-> backtest-kernel
-> backtest-report
```

如果要比较不同规则或参数，必须生成不同 `strategy_name` / `strategy_version` 的 artifact，再分别运行 `backtest-kernel`。不得通过 `backtest-kernel` 的参数临时覆盖风险预算、协方差窗口或权重约束。

## 最小测试要求

CLI 层测试不需要真实 Tushare 网络数据，应使用临时 DuckDB / 临时 lakehouse fixture。

最小覆盖：

- `build-risk-budget` 能写出 5 sleeve risk budget。
- `health-check risk-budget` 能在 FAIL finding 时返回非 0 或抛出命令异常。
- `build-target-weight` 拒绝消费未通过健康检查的 risk budget。
- `health-check target-weight` 能发现权重合计错误、负权重和缺失 sleeve。
- `backtest-kernel` 只读取 frozen target weight，不调用 risk budget 或 target weight builder。
- `backtest-report` 能在只有内核 fixture 时输出非空指标摘要。

## 实施顺序

1. 先实现 `build-risk-budget` 和 `health-check risk-budget`。
2. 再实现 `build-target-weight` 和 `health-check target-weight`。
3. 接入已有前置回测内核为 `backtest-kernel`。
4. 最后实现 `backtest-report` 的 markdown / JSON Phase 0 输出。

`sync-data` 可以先复用现有 ingestion / bootstrap 命令，不必为了 CLI 设计重复实现数据同步。
