# ETF 全天候目标权重设计

## 目的

本文冻结 `derived.etf_aw_target_weight` 的 V1 设计。

目标权重层位于 `derived.etf_aw_risk_budget` 之后、前置回测内核之前。它只回答一个问题：在已冻结风险预算和历史 sleeve 收益可用的前提下，每个 ETF sleeve 应生成多少纸面目标权重。

该层不生成交易建议、不读取当前持仓、不触发回测、不做参数搜索。

## 范围

输入：

- `derived.etf_aw_risk_budget`
- `derived.etf_aw_sleeve_daily`
- `reference.rebalance_calendar.monthly_post_20`

输出：

- 每个 rebalance date、每个 frozen sleeve 一行目标权重。
- 输入风险预算、波动率估计、原始权重、约束后权重、最终目标权重。
- 优化器状态、降级原因和 explainability 信息。

非范围：

- 生成或修改 risk budget。
- 重新估计 market regime。
- simplified ERC / full ERC 优化器。
- 当前持仓、订单、交易计划。
- 自动下单或 broker / QMT / XtQuant 接口。
- Dashboard 或 `/etf-aw` 前端扩展。
- baseline comparison pack。

## Frozen Artifact 规则

目标权重只能消费已经写出的 frozen artifact：

```text
derived.etf_aw_strategy_context
-> derived.etf_aw_risk_budget
-> health check
-> derived.etf_aw_target_weight
-> health check
-> backtest kernel / evaluation report
```

目标权重构建过程中不得：

- 重新生成 `derived.etf_aw_risk_budget`。
- 修改 risk budget。
- 按回测表现搜索窗口、阈值、vol floor 或权重上限。
- 使用 rebalance date 之后才可见的数据。

如果要比较不同窗口、约束或优化器，必须生成不同 `strategy_name` / `strategy_version` 的目标权重 artifact，再进入同一个回测内核。

## 数据集

数据集名称：

```text
derived.etf_aw_target_weight
```

粒度：

```text
calendar_name + rebalance_date + strategy_name + strategy_version + sleeve_code
```

V1 schema：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定为 `etf_aw_target_weight_v1` |
| `contract_version` | 固定为 `etf_aw_target_weight_contract_v1` |
| `calendar_name` | 调仓日历名称 |
| `rebalance_date` | 月度调仓日期 |
| `effective_date` | 权重生效日期，V1 等于 `rebalance_date` |
| `strategy_name` | 策略名称，V1 使用 `etf_aw_v1` |
| `strategy_version` | 策略版本，V1 使用 `target_weight_inverse_vol_v1` |
| `sleeve_code` | frozen ETF 代码 |
| `sleeve_role` | frozen sleeve role |
| `risk_budget` | 来源 `tilted_budget` |
| `volatility_estimate` | 用于 inverse-vol 的波动率估计 |
| `volatility_floor` | V1 使用的波动率下限 |
| `raw_target_weight` | 约束前目标权重 |
| `constrained_target_weight` | 单 sleeve 上限等硬约束后的权重，不包含 no-trade band |
| `target_weight` | 最终目标权重，应用 no-trade band 后再次归一化 |
| `target_weight_status` | `complete` / `partial` / `stale` / `missing` / `unavailable` |
| `optimizer_name` | V1 固定为 `budgeted_inverse_vol` |
| `optimizer_basis` | 权重生成规则说明 |
| `turnover_estimate` | 相对上一期目标权重的纸面换手估计 |
| `quality_notes_json` | 降级原因、来源 caveat 和数值诊断 |
| `source_risk_budget_rebalance_date` | 来源 risk budget 日期 |
| `source_sleeve_daily_max_trade_date` | 用于估计波动率的最大交易日 |
| `ingested_at` | 写入时间 |

禁止字段：

- `trade_action`
- `order_instruction`
- `rebalance_instruction`
- `order_quantity`
- `order_amount`
- `broker_account`

目标权重不是交易计划。后续如果需要根据持仓生成 paper rebalance plan，必须使用独立数据集，例如 `derived.etf_aw_rebalance_plan`。

## Sleeve 顺序

所有目标权重向量按以下顺序定义：

```text
equity_large, equity_small, bond, gold, cash
```

V1 frozen sleeves：

| Sleeve role | Code |
| --- | --- |
| `equity_large` | `510300.SH` |
| `equity_small` | `159845.SZ` |
| `bond` | `511010.SH` |
| `gold` | `518850.SH` |
| `cash` | `159001.SZ` |

每个 rebalance date 必须输出 5 行。缺少任一 frozen sleeve 不允许写出 `complete` 状态。

实现时必须复用 ETL 层共享的 frozen sleeve 顺序和 rank helper，不在 target weight builder、read model 或 API 中重复手写 role order / rank map。写出顺序、read model 返回顺序和测试断言必须保持一致。

## V1 优化器

V1 使用 budgeted inverse-vol approximation，不引入 ERC 求解器。

核心公式：

```text
inverse_vol_score_i = risk_budget_i / max(volatility_i, volatility_floor)
raw_target_weight_i = inverse_vol_score_i / sum(inverse_vol_score)
```

设计理由：

- 已有 Stage E/G 的 regime 和宏观上下文仍是 V1 规则式基座，不适合直接引入复杂优化器。
- inverse-vol 计算可解释、稳定、便于 fixture 验证。
- risk budget 已表达状态方向，target weight 层只负责把预算映射到权重。

## 逐步计算流程

对每个 `rebalance_date`，V1 按 frozen sleeve 顺序执行以下步骤。

### 1. 读取风险预算

从 `derived.etf_aw_risk_budget` 读取同一 `calendar_name + rebalance_date + strategy_name + strategy_version` 下 5 个 sleeve 的 `tilted_budget`：

```text
risk_budget_i = tilted_budget_i
```

前置条件：

- 5 个 frozen sleeve 必须完整。
- `risk_budget` 合计必须为 `1`。
- 来源 risk budget 不得包含 `FAIL` finding。
- 来源 risk budget 日期不得晚于当前 `rebalance_date`。

### 2. 计算日收益

对每个 sleeve，使用 `derived.etf_aw_sleeve_daily` 中 `trade_date <= rebalance_date` 的 adjusted close 计算日收益：

```text
return_t = adj_close_t / adj_close_{t-1} - 1
```

如果实现里已有 `daily_return` 字段，可以直接使用该字段；否则必须从 adjustment-aware `adj_close` 计算，不能使用 raw close。

### 3. 估计波动率

取 rebalance date 之前可见的最近 `63` 个交易日收益，至少需要 `42` 个有效观测：

```text
volatility_i = std(return_i over trailing window)
effective_volatility_i = max(volatility_i, 0.005)
```

如果某个 sleeve 样本不足但整体仍可降级输出：

- `effective_volatility_i = 0.005`
- `target_weight_status = partial`
- `quality_notes_json.reasons` 包含 `insufficient_volatility_observations` 和 `volatility_floor_applied`

如果多个 sleeve 样本不足导致无法形成 5 sleeve 向量，健康检查应输出 FAIL 并阻断写出。

### 4. 生成 raw target weight

风险预算越高、波动率越低，目标权重越高：

```text
inverse_vol_score_i = risk_budget_i / effective_volatility_i
raw_target_weight_i = inverse_vol_score_i / sum(inverse_vol_score)
```

`raw_target_weight` 是纯公式输出，不包含单 sleeve 上限、cash 上限或 no-trade band。

### 5. 应用硬约束

先处理单 sleeve 上限，再处理 cash 上限：

```text
capped_weight_i = min(raw_target_weight_i, sleeve_cap_i)
excess = sum(raw_target_weight) - sum(capped_weight)
```

其中：

```text
sleeve_cap_i = 0.35 for cash
sleeve_cap_i = 0.45 for all other sleeves
```

被 cap 切出的 `excess` 只分配给尚未触顶的 sleeve，按其当前权重比例重新分配。重复执行直到：

- 无剩余 `excess`；或
- 所有 sleeve 都触顶。

然后归一化：

```text
constrained_target_weight_i = capped_weight_i / sum(capped_weight)
```

如果所有 sleeve 都触顶且无法归一化到 `1`，健康检查应输出 FAIL。

### 6. 应用 no-trade band

如果存在上一期 target weight，比较本期 `constrained_target_weight` 与上一期最终 `target_weight`：

```text
diff_i = constrained_target_weight_i - previous_target_weight_i
```

若 `abs(diff_i) < 0.0025`：

```text
target_weight_i = previous_target_weight_i
```

否则：

```text
target_weight_i = constrained_target_weight_i
```

应用 no-trade band 后必须再次归一化，确保：

```text
sum(target_weight) = 1
```

首期没有上一期权重时，不应用 no-trade band：

```text
target_weight_i = constrained_target_weight_i
```

### 7. 计算换手估计

V1 不读取当前持仓，因此换手估计只比较相邻 target weight：

```text
turnover_estimate = 0.5 * sum(abs(target_weight_i - previous_target_weight_i))
```

首期 `turnover_estimate` 可为 `null` 或 `0.0`，但必须在 `quality_notes_json` 中说明首期无法估算真实换手。

### 8. 写出字段

每个 sleeve 行至少写出：

- `risk_budget`
- `volatility_estimate`
- `volatility_floor`
- `raw_target_weight`
- `constrained_target_weight`
- `target_weight`
- `turnover_estimate`
- `target_weight_status`
- `optimizer_name`
- `optimizer_basis`
- `quality_notes_json`

所有权重字段保留 6 位小数；健康检查使用 `1e-6` 容忍度校验合计。

## 数值例子

假设某个 rebalance date 的 risk budget 与有效日波动率如下：

| Sleeve role | `risk_budget` | `effective_volatility` |
| --- | ---: | ---: |
| `equity_large` | `0.235` | `0.012` |
| `equity_small` | `0.235` | `0.016` |
| `bond` | `0.186` | `0.006` |
| `gold` | `0.179` | `0.010` |
| `cash` | `0.165` | `0.005` |

先计算 inverse-vol score：

| Sleeve role | 计算 | Score |
| --- | --- | ---: |
| `equity_large` | `0.235 / 0.012` | `19.5833` |
| `equity_small` | `0.235 / 0.016` | `14.6875` |
| `bond` | `0.186 / 0.006` | `31.0000` |
| `gold` | `0.179 / 0.010` | `17.9000` |
| `cash` | `0.165 / 0.005` | `33.0000` |

score 合计为 `116.1708`，得到 raw target weight：

| Sleeve role | `raw_target_weight` |
| --- | ---: |
| `equity_large` | `0.1686` |
| `equity_small` | `0.1264` |
| `bond` | `0.2668` |
| `gold` | `0.1541` |
| `cash` | `0.2841` |

此例中没有 sleeve 超过上限，且 cash 未超过 `0.35`，因此：

```text
constrained_target_weight = raw_target_weight
```

如果没有上一期权重：

```text
target_weight = constrained_target_weight
turnover_estimate = null
```

如果上一期 `target_weight` 为：

| Sleeve role | Previous |
| --- | ---: |
| `equity_large` | `0.1690` |
| `equity_small` | `0.1260` |
| `bond` | `0.2670` |
| `gold` | `0.1540` |
| `cash` | `0.2840` |

所有差异均小于 `0.0025`，则 no-trade band 允许沿用上一期权重，最终：

```text
target_weight = previous_target_weight
turnover_estimate = 0.0
```

## 正确性判断

实现完成后，不能只看输出非空来判断正确。至少需要同时满足以下检查。

### 数学恒等式

每个 rebalance date 都必须满足：

```text
sum(risk_budget) = 1
effective_volatility_i = max(volatility_i, 0.005)
inverse_vol_score_i = risk_budget_i / effective_volatility_i
raw_target_weight_i = inverse_vol_score_i / sum(inverse_vol_score)
sum(raw_target_weight) = 1
sum(constrained_target_weight) = 1
sum(target_weight) = 1
```

合计检查容忍度统一使用 `1e-6`。任何权重为负数、非数值或超过对应 sleeve 上限，都不能被视为正确输出。

### 方向性检查

在其他输入不变时：

- `risk_budget` 增加，目标权重应增加。
- `volatility_estimate` 增加，目标权重应下降。
- `volatility_estimate < volatility_floor` 时，必须使用 floor 后的有效波动率。
- cash 因低波动获得过高 raw weight 时，必须受 `0.35` 上限约束。
- 非 cash sleeve 不得超过 `0.45`。
- no-trade band 只能消除小于 `0.0025` 的尾差，不能吞掉明确的大幅权重变化。

### 示例回归

本文数值例子应进入单元测试 fixture。给定同样的 `risk_budget` 和 `effective_volatility`，实现应输出近似：

| Sleeve role | Expected raw target weight |
| --- | ---: |
| `equity_large` | `0.1686` |
| `equity_small` | `0.1264` |
| `bond` | `0.2668` |
| `gold` | `0.1541` |
| `cash` | `0.2841` |

该 fixture 的目的不是证明策略有效，而是锁定公式解释、浮点精度和 sleeve 顺序。

### 约束回归

还需要构造至少以下边界用例：

- cash raw weight 超过 `0.35` 时被 cap，并把 excess 分配给未触顶 sleeve。
- 非 cash raw weight 超过 `0.45` 时被 cap。
- 多个 sleeve 同时触顶时仍能归一化；无法归一化时输出 FAIL。
- 单个 sleeve 波动率缺失时进入 `partial` 降级。
- 多个 sleeve 样本不足导致无法形成 5 sleeve 向量时阻断写出。
- no-trade band 小差异沿用上一期，大差异使用本期 constrained 权重。

### Point-in-time 检查

任一实现或测试都必须证明：

- risk budget 来源日期不晚于 `rebalance_date`。
- sleeve daily 收益窗口只包含 `trade_date <= rebalance_date` 的行。
- 不为了补齐 63 日窗口向后读取未来交易日。
- stale 或缺失的 sleeve daily 会进入降级或 FAIL，而不是静默 forward-fill。

### 回测内核检查

target weight 通过写出健康检查后，还必须被前置回测内核消费一次：

- 能生成确定性净值路径。
- 月度换手不因 `1e-6` 级尾差异常放大。
- diagnostics 能指出缺失收益、缺失权重和调仓日对齐问题。

这一步只验证 artifact 可消费和数值稳定，不做收益优劣判断。

## 波动率估计

输入使用 `derived.etf_aw_sleeve_daily` 的 adjusted return。

V1 固定参数：

| 参数 | 值 |
| --- | ---: |
| trailing window | `63` 个交易日 |
| minimum observations | `42` 个交易日 |
| max missing ratio | `0.20` |
| volatility floor | `0.005` 日波动率 |
| single sleeve max weight | `0.45` |
| cash sleeve max weight | `0.35` |
| no-trade band | `0.0025` |
| turnover warn threshold | `0.25` |

波动率计算：

```text
volatility_i = std(adjusted_daily_return_i over trailing 63 observations)
```

V1 不使用协方差矩阵做优化。协方差摘要可以写入 diagnostics，但不得驱动权重求解。

## Cash Sleeve 低波动规则

现金 sleeve 可能因为低波动导致 inverse-vol 权重发散。

V1 规则：

- cash 使用同一 `volatility_floor`。
- cash 权重仍受 `cash sleeve max weight = 0.35` 限制。
- 如果 cash 原始权重超过 `0.35`，必须约束并重新归一化其他 sleeve。
- 如果 cash 因 vol floor 触发约束，`quality_notes_json.reasons` 必须包含 `cash_weight_capped` 或 `volatility_floor_applied`。

cash 低波动不能静默放大成主导仓位。

## 权重约束

约束顺序：

```text
raw_target_weight
-> cap single sleeve
-> cap cash sleeve
-> redistribute excess weight
-> normalize
-> constrained_target_weight
-> no-trade band handling
-> target_weight
```

V1 不读取当前持仓，因此 no-trade band 只用于相邻目标权重之间的尾差处理：

- 如果某 sleeve 相比上一期 `constrained_target_weight` 的变化绝对值小于 `0.0025`，允许沿用上一期权重。
- 应用 no-trade band 后必须再次 normalize。
- no-trade band 阈值必须明显大于 `1e-6` 浮点容差，避免尾差触发伪调仓。

如果没有上一期目标权重，首期不应用 no-trade band。

字段语义：

- `raw_target_weight` 只表达 inverse-vol 公式输出。
- `constrained_target_weight` 表达硬约束后的权重。
- `target_weight` 表达最终写出的权重；如果 no-trade band 被触发，它可以不同于 `constrained_target_weight`。
- `turnover_estimate` 使用最终 `target_weight` 计算，避免把被 no-trade band 消除的尾差计入换手。

## 状态映射

| 输入情况 | 输出行为 |
| --- | --- |
| risk budget `budget_status = complete`，5 sleeve 完整，波动率样本充足 | `target_weight_status = complete`，按 budgeted inverse-vol 输出 |
| risk budget `budget_status = partial`，波动率样本充足 | `target_weight_status = partial`，按预算输出但保留降级原因 |
| risk budget `budget_status = stale` | `target_weight_status = stale`，输出中性 inverse-vol 或上一期可用权重，必须写明 basis |
| risk budget `budget_status = missing` / `unavailable` | `target_weight_status = unavailable`，不输出主动权重 |
| 单个 sleeve 波动率样本不足 | `target_weight_status = partial`，该 sleeve 使用 volatility floor 并写入原因 |
| 多个 sleeve 波动率样本不足，无法形成 5 sleeve 向量 | FAIL，阻断写出 |
| 来源 risk budget 未通过健康检查 | FAIL，阻断写出 |

降级原则：

- 缺失数据只能降低主动性，不能放大权重。
- 任一降级必须进入 `quality_notes_json.reasons`。
- `complete` 状态不得依赖 partial、missing、stale 或 unavailable 来源。

## Point-in-time 规则

目标权重不得使用 rebalance date 之后才可见的数据。

约束：

- `source_risk_budget_rebalance_date <= rebalance_date`。
- 波动率估计只使用 `trade_date <= rebalance_date` 的 sleeve daily。
- `source_sleeve_daily_max_trade_date <= rebalance_date`。
- 不允许为了填满窗口向后读取未来交易日。
- 如果某 sleeve 的最新交易日早于 rebalance date，应标记 stale 或 partial，并写入诊断。

## Optimizer Basis

`optimizer_basis` 使用短字符串，便于 read model 和后续 CLI 报表展示。

允许值：

| `optimizer_basis` | 含义 |
| --- | --- |
| `budgeted_inverse_vol` | 风险预算除以 trailing volatility 后归一化 |
| `degraded_inverse_vol` | 输入或样本降级后仍可输出权重 |
| `neutral_inverse_vol` | 风险预算不可用时回落到中性预算的 inverse-vol |
| `unavailable` | 来源或样本不足，不能生成有效权重 |

## Quality Notes

`quality_notes_json` 必须是 JSON object。

建议字段：

```json
{
  "reasons": [],
  "source_risk_budget_status": "complete",
  "risk_budget_sum": 1.0,
  "window_observations": {
    "equity_large": 63,
    "equity_small": 63,
    "bond": 63,
    "gold": 63,
    "cash": 63
  },
  "volatility_floor_applied_roles": [],
  "raw_weight_sum": 1.0,
  "constrained_weight_sum": 1.0,
  "target_weight_sum": 1.0,
  "turnover_estimate": 0.08,
  "capped_roles": [],
  "no_trade_band_roles": []
}
```

常见 `reasons`：

- `risk_budget_missing`
- `risk_budget_failed_health_check`
- `risk_budget_partial`
- `risk_budget_stale`
- `sleeve_daily_missing`
- `sleeve_daily_stale`
- `insufficient_volatility_observations`
- `volatility_floor_applied`
- `cash_weight_capped`
- `single_sleeve_weight_capped`
- `no_trade_band_applied`
- `high_turnover`
- `source_after_rebalance_date`

## Read Model 合同

V1 先只新增 latest 合同：

- `get_latest_etf_aw_target_weight`

返回合同：

- `schema_version = etf_aw_target_weight_v1`
- `contract_version = etf_aw_target_weight_contract_v1`
- 每个 rebalance date 必须包含 5 个 sleeve。
- 返回 `target_weight_sum`、`raw_target_weight_sum`、`constrained_target_weight_sum`。
- 返回 `turnover_estimate` 和顶层 `quality_notes`。
- 不返回 `trade_action`、`order_instruction` 或 `rebalance_instruction`。

如果最新数据缺失，应返回 `None`，不能合成权重。

列表 read model、API endpoint 和前端展示等到 backtest kernel 消费合同稳定后再加。

如果后续新增 API endpoint，必须声明 Pydantic `response_model`，并同步前端 typed contract。读取失败和无数据应在消费方区分，不能把接口错误伪装成空 artifact。

## 健康检查清单

目标权重写出前必须运行纯函数健康检查。健康检查不读盘、不触发风险预算或权重重算。

健康检查 findings 是 target weight validation 的唯一事实来源。`_validate_target_weight_frame` 应从 `_target_weight_health_findings` 派生布尔结果，避免维护两套会漂移的不变量实现。构建流程只因 `FAIL` finding 阻断写出；`WARN` 必须保留在返回值或后续健康检查输出中，不能被静默丢弃。

### FAIL

出现以下情况必须阻断写出：

- 每个 rebalance date 不是 5 个 frozen sleeve。
- 同一 business key 出现重复行。
- `raw_target_weight`、`constrained_target_weight` 或 `target_weight` 合计不等于 `1`，容忍浮点误差不超过 `1e-6`。
- 任一权重为负数、非数值或超过 V1 单 sleeve 上限。
- cash sleeve 权重超过 `0.35`。
- 来源 `derived.etf_aw_risk_budget` 缺失、未通过健康检查或 business key 不唯一。
- `risk_budget` 合计不等于 `1`，容忍浮点误差不超过 `1e-6`。
- 波动率样本不足且未降级。
- `volatility_estimate` 非数值且未使用 floor 降级。
- 输出字段包含交易动作、订单或执行指令。
- 来源 risk budget 或 sleeve daily 日期晚于当前 `rebalance_date`。

### WARN

出现以下情况允许写出，但必须进入 finding 或 `quality_notes_json.reasons`：

- 月度 `turnover_estimate > 0.25`。
- no-trade band 内尾差被归零或沿用上一期权重。
- 单 sleeve 因 vol floor 或缺失数据被降级。
- cash sleeve 因低波动触发上限。
- 连续多个 rebalance date 的 target weight 完全不变。
- risk budget 为 `partial` 或 `stale`，但仍可生成降级权重。

V1 必须至少产出一类真实 `WARN` finding，例如 `high_turnover`、`no_trade_band_applied`、`volatility_floor_applied` 或 `cash_weight_capped`。如果实现阶段决定暂不支持 WARN，则不应保留形同虚设的 level 分级。

### Finding 字段

健康检查 finding 建议包含：

- `level`: `FAIL` / `WARN`
- `check_name`
- `rebalance_date`
- `sleeve_role`，如适用
- `message`
- `details`

FAIL 不能被 backtest kernel、Dashboard 或 workflow insight 静默忽略。

## 测试要求

最小测试集：

- 完整 5 sleeve risk budget 和 63 日收益生成非空目标权重。
- 输出 5 行，顺序为 frozen sleeve 顺序。
- `raw_target_weight`、`constrained_target_weight`、`target_weight` 合计为 `1`。
- cash 低波动触发 `cash_weight_capped`。
- 单 sleeve 波动率缺失触发 partial 降级。
- 多 sleeve 样本不足时阻断写出。
- 来源 risk budget 缺失时阻断写出。
- 来源 risk budget 日期晚于 rebalance date 时阻断写出。
- 重复 business key 被健康检查拦截。
- 权重超过单 sleeve 上限被约束或阻断。
- no-trade band 不因 `1e-6` 级别尾差产生伪换手。
- 前置回测内核能消费生成的 target weight fixture，并输出确定性净值、指标和换手。

测试应使用临时 DuckDB 和临时 lakehouse fixture，不依赖网络。

## 实现边界

后续实现应遵循现有 derived dataset 风格：

```text
_build_*() -> _make_*_frame(df) -> _write_*()
```

建议拆分：

- `_build_etf_aw_target_weight(start, end)`
- `_make_etf_aw_target_weight_frame(risk_budget, sleeve_daily)`
- `_write_etf_aw_target_weight(frame)`
- `_target_weight_health_findings(frame)`
- `_validate_target_weight_frame(frame)`

`_make_etf_aw_target_weight_frame` 应保持纯函数，便于构造降级测试。

## 与回测内核衔接

前置回测内核只能消费已经写出的 `derived.etf_aw_target_weight`。

验收要求：

- target weight 序列能生成确定性收益路径。
- 月度换手不因浮点尾差异常爆发。
- diagnostics 能指出缺失收益、缺失权重和调仓日对齐问题。
- 开发期至少能与等权 fixture 对比。

完整 baseline 对比、成本假设、参数扰动和前端净值展示仍属于后续 `backtest-evaluation-design.md` Phase 2。

## simplified ERC 引入标准

V1 不实现 simplified ERC。

只有同时满足以下条件，才允许新增 `target_weight_erc_v1`：

- budgeted inverse-vol 已通过前置回测内核验收。
- target weight 健康检查稳定，无频繁人工豁免。
- baseline evaluation 能比较等权、静态 inverse-vol、当前 V1 和 ERC 版本。
- ERC 求解失败、奇异协方差、权重上限和 cash 低波动都有 deterministic fixture。

ERC 必须作为新的 `strategy_version` artifact，不得替换 V1 合同。

## 验收标准

本文档冻结后，进入实现前必须满足：

- schema 和 business key 已固定。
- inverse-vol 参数、vol floor、权重上限和 no-trade band 已固定。
- 降级规则和健康检查清单已固定。
- 明确不输出交易建议和订单。
- 明确前端不扩展，先由 backtest kernel 消费合同。

实现完成的最低验收：

- `derived.etf_aw_target_weight` dataset 注册完成。
- bootstrap profile 可运行。
- latest read model 可读取最新目标权重。
- 单元测试覆盖有效、降级、缺失、低波动、禁止字段和 point-in-time。
- 前置回测内核可消费 target weight fixture。
- 不改变 Stage B-I 既有合同。
