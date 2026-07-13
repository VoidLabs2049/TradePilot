# ETF 全天候 Stage N 模拟盘订单草案设计

## 目的

本文定义 Stage N 的最小实现合同：把已冻结的 ETF 全天候目标权重、账户快照和最新可用价格转换为可审计的模拟盘调仓计划与 `DRAFT` 订单草案。

Stage N 用于验证“目标权重能否落到可执行数量”的工程闭环，不证明策略有效，不连接券商，也不产生真实订单或模拟成交。

## 阶段边界

输入：

- 最新完整的 `derived.etf_aw_target_weight` 权重向量。
- 单一模拟账户的持仓、可用持仓、现金和总资产快照。
- 5 个 frozen sleeve 对应 ETF 的最新可用价格。
- A 股 ETF 交易单位；V1 固定为每手 `100` 份。
- 现金缓冲比例；V1 固定为 `cash_buffer_ratio = 0.01`。

输出：

- `derived.etf_aw_rebalance_plan` frozen artifact。
- 同一计划的 JSON 和 Markdown 人工审阅文件。
- 账户级校验结果、阻断原因和非阻断提醒。

非范围：

- 不重新计算或修改目标权重。
- 不运行回测或根据 Stage M 结果临时调参。
- 不连接券商 API，不提交、确认、撤销或撮合订单。
- 不模拟成交、更新持仓或计算 forward performance；这些属于 Stage O。
- 不估计盘口、滑点、冲击成本、税费、佣金或成交概率。
- 不建设 API、定时任务或前端操作入口。

## 关键假设

V1 采用以下明确假设：

1. 账户总资产、现金、持仓和价格来自同一计划时点的人工提供快照。
2. 计划使用价格快照估算名义金额，不把它解释为可成交价格。
3. 同批卖出预计所得可用于同批买入；该假设必须写入 `warnings`。
4. 所有 BUY 和 SELL 数量均按 `lot_size` 向下取整。
5. Stage N 只生成整份计划。任一阻断条件成立时，不输出部分可执行订单。
6. frozen universe 当前固定为 5 个 sleeve；symbol 映射必须显式提供，不能按持仓缺失推断为零持仓。

## 输入合同

### 目标权重

从 `derived.etf_aw_target_weight` 选择最新可用且完整的单个向量：

```text
calendar_name + rebalance_date + strategy_name + strategy_version
```

必须满足：

- 恰好覆盖 frozen 5-sleeve universe，且无重复 sleeve。
- 每个 `target_weight` 有限且非负。
- `abs(sum(target_weight) - 1.0) <= 1e-6`。
- 所有行的 `rebalance_date`、`strategy_name`、`strategy_version` 一致。

Stage N 不允许用旧权重补齐缺失 sleeve，也不允许在读取后重新归一化。

### 账户快照

V1 通过 repo-visible JSON 输入单一模拟账户快照，最少字段为：

| 字段 | 含义 |
| --- | --- |
| `account_id` | 模拟账户标识 |
| `snapshot_at` | 账户快照时间 |
| `cash` | 可用于计划估算的现金，单位 CNY |
| `total_asset` | 账户总资产，单位 CNY |
| `positions` | frozen symbol 的持仓列表 |
| `positions[].symbol` | ETF 代码 |
| `positions[].quantity` | 总持仓份数 |
| `positions[].available_quantity` | 可卖持仓份数 |
| `positions[].market_value` | 快照市值，单位 CNY |
| `positions[].cost_basis` | 可选的持仓成本信息；仅展示，不参与订单计算 |

账户快照必须满足：

- `cash >= 0`，`total_asset > 0`。
- 数量为非负整数，且 `available_quantity <= quantity`。
- 每个 frozen symbol 恰好一条持仓记录；零持仓必须显式写为 `0`。
- `market_value` 有限且非负。

### 价格快照

V1 通过 repo-visible JSON 输入价格快照，最少字段为：

| 字段 | 含义 |
| --- | --- |
| `price_as_of` | 价格快照时间 |
| `symbol` | ETF 代码 |
| `latest_price` | 最新可用价格，单位 CNY/份 |
| `source` | 价格来源标识 |

每个 frozen symbol 必须恰好一条正数、有限价格。V1 不自动回退到更早价格；调用方必须明确准备本次计划使用的价格快照。

## 输出 Artifact

数据集名称：

```text
derived.etf_aw_rebalance_plan
```

粒度：

```text
plan_id + sleeve_role + symbol
```

V1 schema：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定为 `etf_aw_rebalance_plan_v1` |
| `contract_version` | 固定为 `etf_aw_rebalance_plan_contract_v1` |
| `plan_id` | 由业务幂等键确定的稳定计划标识 |
| `plan_date` | 计划日期 |
| `generated_at` | 生成时间 |
| `account_id` | 模拟账户标识 |
| `account_snapshot_at` | 账户快照时间 |
| `price_as_of` | 价格快照时间 |
| `target_weight_rebalance_date` | 来源目标权重调仓日期 |
| `strategy_name` | 来源策略名称 |
| `strategy_version` | 来源策略版本 |
| `sleeve_role` | frozen sleeve role |
| `symbol` | ETF 代码 |
| `target_weight` | 目标权重 |
| `current_quantity` | 当前持仓份数 |
| `available_quantity` | 可卖持仓份数 |
| `current_market_value` | 持仓快照市值，单位 CNY |
| `latest_price` | 计划估值价格，单位 CNY/份 |
| `target_notional` | 目标市值，单位 CNY |
| `raw_delta_quantity` | 取整前目标与当前持仓的差额份数 |
| `lot_size` | 交易单位，V1 固定为 100 份 |
| `order_side` | `BUY`、`SELL` 或 `HOLD` |
| `order_quantity` | 按交易单位取整后的非负订单份数 |
| `estimated_notional` | 预计订单名义金额，单位 CNY |
| `cash_buffer_ratio` | V1 固定为 `0.01` |
| `plan_status` | Stage N 固定为 `DRAFT` |
| `blocking_reasons_json` | 账户级和行级阻断原因 |
| `warnings_json` | 非阻断提醒 |

禁止字段：

- 券商订单号、委托状态或成交状态。
- 成交价格、成交数量或成交时间。
- `APPROVED`、`SUBMITTED`、`FILLED` 等执行状态。
- API credential、账户密钥或真实券商账户信息。

## 计算规则

### 目标市值与原始差额

```text
required_cash_buffer = total_asset * cash_buffer_ratio
tradable_equity = total_asset - required_cash_buffer
target_notional = tradable_equity * target_weight
raw_delta_quantity = (target_notional - current_market_value) / latest_price
```

V1 使用持仓快照中的 `current_market_value` 计算差额，同时计算：

```text
repriced_market_value = current_quantity * latest_price
```

当 `current_market_value` 与 `repriced_market_value` 不一致时保留原始快照值，并输出 `market_value_price_mismatch` warning，不静默覆盖输入。实现阶段应冻结一个简单的偏差阈值并写入测试，不能在运行时自动调整。

### 交易方向与手数

```text
rounded_quantity = floor(abs(raw_delta_quantity) / lot_size) * lot_size
```

- `raw_delta_quantity > 0` 且 `rounded_quantity > 0`：`BUY`。
- `raw_delta_quantity < 0` 且 `rounded_quantity > 0`：`SELL`。
- `rounded_quantity = 0`：`HOLD`，并记录 `below_lot_size` warning。
- SELL 的 `order_quantity` 不得超过 `available_quantity`；不允许静默截断为可卖数量。
- `estimated_notional = order_quantity * latest_price`。

BUY 和 SELL 都向下取整，使订单草案不因手数取整扩大原始调仓绝对值。

### 账户级现金校验

所有行完成取整后统一计算：

```text
estimated_sell_proceeds = sum(SELL estimated_notional)
estimated_buy_notional = sum(BUY estimated_notional)
cash_after_plan = cash + estimated_sell_proceeds - estimated_buy_notional
```

必须满足：

```text
cash_after_plan >= required_cash_buffer
```

不满足时整份计划阻断，不按比例缩减买单，也不按任意优先级删除买单。自动资金分配会引入新的组合优化语义，不属于 V1。

## 阻断与提醒

### 阻断原因

至少定义以下稳定 reason code：

| reason code | 条件 |
| --- | --- |
| `incomplete_target_weight` | 目标权重未完整覆盖 frozen 5-sleeve |
| `invalid_target_weight_sum` | 权重和超出 `1e-6` 容忍度 |
| `invalid_target_weight` | 权重为负数、非有限值或重复 |
| `invalid_account_snapshot` | 现金、总资产或持仓数量合同不合法 |
| `missing_symbol_mapping` | sleeve 无唯一 symbol 映射 |
| `missing_position` | frozen symbol 无显式持仓记录 |
| `missing_or_invalid_price` | 缺少唯一正数价格 |
| `insufficient_available_quantity` | SELL 数量超过可卖持仓 |
| `insufficient_cash_buffer` | 计划后现金低于固定缓冲 |
| `duplicate_active_plan` | 同一幂等键已存在非 cancelled 计划 |

阻断时命令返回失败，不写 `derived.etf_aw_rebalance_plan`，但应把 diagnostics 输出到标准错误，便于人工修正输入。

### 非阻断提醒

至少定义：

- `below_lot_size`：原始差额不足一手，订单为 `HOLD`。
- `market_value_price_mismatch`：持仓市值与数量乘最新价格存在显著偏差。
- `same_batch_sell_proceeds_assumed`：现金校验假设同批卖出所得可用于买入。
- `research_only_strategy`：Stage M 尚不能支持策略有效性结论；订单草案仅验证工程闭环。

## 幂等与可追溯性

业务幂等键固定为：

```text
account_id + plan_date + strategy_version + target_weight_rebalance_date
```

同一幂等键已存在计划时，Stage N 必须拒绝再次生成。Stage N 不定义取消状态，也不覆盖旧 artifact；如需取消后重建，由 Stage O 设计明确状态和审计规则后实现。

每份 JSON 和 Markdown 文件必须包含：

- 输入文件路径或 artifact 标识。
- 目标权重业务键。
- 账户与价格快照时间。
- 生成参数和合同版本。
- 账户级计算汇总。
- 每个 sleeve 的订单草案、阻断原因和提醒。

## CLI 设计

建议新增独立命令：

```text
python -m tradepilot.etf_aw.cli build-rebalance-plan \
  --account-snapshot path/to/account-snapshot.json \
  --price-snapshot path/to/price-snapshot.json \
  --plan-date YYYY-MM-DD \
  --output-dir path/to/output
```

命令职责：

1. 只读最新 frozen target weight 和两个显式快照文件。
2. 完成纯函数校验与订单计算。
3. 校验通过后写 frozen artifact、JSON 和 Markdown。
4. 输出 `plan_id`、BUY/SELL/HOLD 数量、预计买卖金额、计划后现金和 warnings 摘要。

V1 不提供 `--submit`、`--approve`、`--broker` 或自动修正现金不足的参数。

## 实现切分

实现应保持三层最小边界：

1. 输入模型与合同校验：解析 frozen target weight、账户快照和价格快照。
2. 纯函数计划计算：输入已校验模型，返回完整计划或结构化 diagnostics，不访问文件系统。
3. CLI 与 artifact 写出：负责读取、幂等检查、输出文件和终端摘要。

不为单次用途引入通用订单管理框架、broker adapter 或状态机。

## 验证要求

### 单元测试

至少覆盖：

- 5-sleeve 正常输入生成确定性的 BUY、SELL 和 HOLD。
- BUY 与 SELL 均按 100 份向下取整。
- 差额不足一手时输出 `HOLD + below_lot_size`。
- SELL 超过 `available_quantity` 时整份计划阻断。
- 计划后现金低于 1% 缓冲时整份计划阻断。
- 权重缺 sleeve、重复、为负或权重和错误时阻断。
- 持仓、symbol 映射或价格缺失时阻断。
- 非正数价格、负现金和非法持仓数量时阻断。
- 持仓市值与最新价格不一致时保留输入并输出 warning。
- 相同输入重复计算得到相同订单行和稳定 `plan_id`。

### CLI 与 Artifact 测试

至少覆盖：

- 合法 fixture 生成 schema 完整的 5 行 artifact、JSON 和 Markdown。
- `plan_status` 全部为 `DRAFT`，且不存在成交或券商字段。
- JSON 数值与 frozen artifact 一致。
- Markdown 明确显示“模拟盘草案、需人工判断、未提交订单”。
- 重复业务键不会覆盖已有计划。
- 任一阻断条件下不产生 partial artifact。

## 完成标准

Stage N 完成时必须同时满足：

- 独立的 `derived.etf_aw_rebalance_plan` 合同已实现并冻结版本。
- 纯函数可从完整 5-sleeve 权重和显式账户/价格快照生成确定性订单草案。
- 手数、可卖持仓、现金缓冲、完整性和幂等校验均有测试。
- CLI 可生成可人工审阅的 JSON 和 Markdown，且所有行固定为 `DRAFT`。
- 失败时不写 partial plan，成功时可追溯到全部输入快照和目标权重版本。
- 没有券商连接、自动提交、模拟成交、API 或前端写操作。

完成以上条件后，Stage O 才能基于 frozen `plan_id` 设计人工确认、模拟成交、每日持仓与 forward performance 记录。
