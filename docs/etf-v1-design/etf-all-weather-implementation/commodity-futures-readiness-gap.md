# 商品期货真实落地缺口清单

本文件盘点从[商品期货 Sleeve 设计方案](./commodity-futures-sleeve-design.md)（已覆盖标的映射、regime 取舍、分阶段计划）与[主力连续拼接管线](../etf-all-weather-data-sources/futures-continuous-contract-pipeline.md)（已覆盖数据构造）到**真实落地**之间，除数据接入外还欠缺什么。所有条目均经代码核查，带 `file:line`。

## 0. 一个改变问题性质的前提

核查中最重要的发现：**现有 ETF 全天候平台本身还是一个"零成本 + 现货全额持有 + 禁止做空"的回测/模拟模型。** 具体证据：

- 回测净值只滚乘组合日收益，**没有任何成本扣减**：`service.py:6271` `nav *= 1.0 + portfolio_return`。turnover 被算出来了（`service.py:6233`），但只作为一条观测行写出，从不进入 NAV。
- 模拟盘显式声明零费用假设：`shadow_run.py:475` `"zero_fee_assumption"`。
- 模拟持仓**禁止负数量**（不能做空）：`shadow_run.py:611` `insufficient_shadow_position`。
- 下单/持仓一律现货口径：`notional = quantity * price`，无合约乘数（`rebalance_plan.py:276`、`shadow_run.py:447`）；买入扣**全额** notional 而非保证金（`rebalance_plan.py:334`、`shadow_run.py:603`）。

**结论**：期货落地需要的成本引擎、保证金、做空、盯市、合约乘数——**现有 ETF 平台一个都还没有**。因此期货不是"在成熟执行框架上加一类标的"，而是会**第一个把这些留白逼出来**。这决定了排期：其中一部分是"期货专属"，另一部分其实是"平台共性债务"，值得分开对待（见 §9）。

设计方案已明确把实盘/模拟下单、保证金、杠杆排除在第一阶段外（见主方案 §1、§4.3），即执行侧目前是**有意留白**，不是遗漏。本文件是把这份"待补清单"显式化，供后续立项。

## 1. 数据接入

| 缺口 | 位置 | 说明 |
| --- | --- | --- |
| 代码正则拒绝期货 | `validators.py:125` `^\d{6}\.(SH\|SZ)$` | `M.DCE`/`CU.SHF` 会被拒 |
| 交易所白名单 | `validators.py:84` `["SH","SZ"]` | 需加 `SHF/DCE/ZCE/INE`（实测三字母后缀） |
| 类型白名单 | `validators.py:212` `.isin(["etf","index"])` | 需加 `future` |
| 连续合约拼接管线 | 无 | 后向复权拼接替代 ETF 的 `adj_factor` 路径（`service.py:1690`）；设计已成文，代码未落地 |

## 2. 回测内核

| 缺口 | 位置 | 说明 |
| --- | --- | --- |
| 假设现货全额持有 | `service.py:6265-6271` | 净值 = Σ权重×日收益，无名义价值/乘数/保证金/杠杆概念。期货喂入的必须是**连续合约后向复权收益率**，而非 ETF `adj_factor` 收益（`service.py:1690`） |
| 硬校验"恰好 6 sleeve" | `service.py:6337/6372` | panel 与 weights 的 code 集合必须**恰好等于** `_ETF_AW_SLEEVE_CODES`；加第 7 个 sleeve 前会判 blocking |
| 权重和必须为 1 | `service.py:6376` `weight_sum_not_one` | 与"名义价值权重之和为 1"口径一致，但需确认期货名义口径下成立 |

## 3. Regime 打分（策略层）

| 缺口 | 位置 | 说明 |
| --- | --- | --- |
| 只聚 equity/bond/gold/cash 四组 | `service.py:3633-3649` | commodity 会被完全忽略；`market_score` 系数写死，商品与权益相关性方向随宏观切换，不能照抄（详见主方案 §5.1） |
| market feature scope 只有 4 组 | `service.py:196` | 需加 `commodity_score` |
| 权重上限 caps 按 6 role 写死 | `service.py:118` `_ETF_AW_TARGET_WEIGHT_CAPS` | 需补 commodity 档；tilt 表同理（`service.py:251` 起） |

## 4. 成本引擎（平台共性债务 + 期货专属）

**现状：回测与模拟盘均零成本**（§0）。落地期货需要一个把 turnover 真正折算成 NAV 拖累的成本引擎，且能按期货口径计费：

- 期货手续费：每手固定 + 按名义价值比例，两种并存。
- **平今仓单独费率**：日内平仓费率通常高于开仓/隔夜平仓，需按开平标记计费。
- 冲击成本：按流动性/下单量估计。
- ETF 侧的佣金/冲击此前也没建模——成本引擎应同时补上 ETF，避免两套口径。

## 5. 下单执行 / 模拟盘

| 缺口 | 位置 | 现货现状 → 期货需求 |
| --- | --- | --- |
| 下单单位 | `rebalance_plan.py:35` `DEFAULT_LOT_SIZE=100`；前端 `index.tsx:478` 写死 100 | 整百股 → **整数手（1 张）** |
| 保证金占用 | `rebalance_plan.py:334`、`shadow_run.py:603` 扣全额 notional | 全额 → **保证金 = notional × 乘数 × 保证金率** |
| 合约乘数 | `rebalance_plan.py:276`、`shadow_run.py:447` `notional=qty*price` | 无乘数 → `notional = qty * price * multiplier` |
| 做空 | `shadow_run.py:611` 禁止负持仓 | 禁止 → **允许负持仓（空头）** |
| 盈亏口径 | `shadow_run.py:447` 持仓×价市值法 | 市值法 → **保证金账户逐日盯市浮盈浮亏** |
| 换月移仓 | 无 | 主力换月时模拟盘需生成移仓单（平旧开新），并与连续序列的换月日对齐 |

## 6. 持仓承载结构

全天候持仓/下单草稿落在 **parquet dataset**（非 db.py 表）：`derived.etf_aw_rebalance_plan`（`rebalance_plan.py:31`）、shadow 系列（`shadow_run.py:28-31`）。

- parquet 行 schema（`rebalance_plan.py:300-331`、`shadow_run.py:292-308`）为现货语义，**缺 `contract_multiplier / margin / contract_month / roll_date` 列**。
- 参考表 `canonical_sleeves`（`db.py:317`）、`canonical_instruments`（`db.py:288`）有 `instrument_type` 但无期货专属列；`sleeve_code` 单一稳定代码假设与主力换月冲突。
- （db.py 的 `portfolio/trades/trade_plan` 是旧 A 股个股系统，与全天候期货落地无关，不必动。）

## 7. 前端

| 缺口 | 位置 | 说明 |
| --- | --- | --- |
| role 硬编码 6 个 | `index.tsx:53-78` `ROLE_LABELS/COLORS/ORDER` | 需加 commodity |
| role union 类型 | `api.ts:90-96` `EtfAwSleeveRole` | 6 个字面量 union，需加 commodity |
| 前端自复刻现货下单数学 | `index.tsx:470-506` | 写死 100 手数、无乘数、无保证金、买入扣全额现金；对期货给出的名义价值/现金占用会算错，需按期货口径重写 |

## 8. 测试

测试全为 mock/fixture（不打真实 Tushare），可控，但大量硬编码"恰好 6 sleeve / 固定行数"：

- 内联 6-sleeve 清单：`test_stage_h_backtest_kernel.py:522/563`、`test_stage_n_rebalance_plan.py:281`。
- **写死行数断言会直接 fail**：`test_stage_o_shadow_run.py:202/369/446` `len==6`、`:384/418` `len==12`（2 天×6 sleeve）。加第 7 个 sleeve 后必须改为按 sleeve 数动态。

## 9. 落地建议：区分"期货专属"与"平台共性债务"

把缺口分两类立项，避免把平台级改造全压到"引入期货"这一件事上：

**A. 平台共性债务**（现有 ETF 也缺，建议独立于期货先补，或至少显式承认）：
- 成本引擎（§4）——回测/模拟盘零成本是现有平台的既有状态，不是期货引入的。
- 做空、盯市（§5）——ETF 全天候用不到，但成本引擎和执行框架的重构会顺带触及。

**B. 期货专属**（只有期货需要）：
- 连续合约拼接（§1）、合约乘数/保证金/换月字段（§5/§6）、regime 的 commodity 组（§3）、代码/交易所/类型白名单（§1）、前端 commodity role（§7）。

**分阶段落地顺序建议**（与主方案的 Phase 对齐、并细化执行侧）：

1. **数据接入 + 连续拼接**（主方案 Phase 1）：让期货能入链、连续序列可重建。**不碰执行/成本。**
2. **回测口径打通**（主方案 Phase 2）：解除"恰好 6 sleeve/权重和=1"硬校验，让商品篮子作为第 7 个 sleeve 进回测。此时回测仍零成本——**先验证有无独立风险来源，成本敏感性用主方案既有的成本敏感性方法事后叠加**。
3. **成本引擎**（平台共性，可与 2 并行）：给回测补成本，ETF + 期货一起。这是判断"成本后是否仍有增量"的前提（对照主方案停止条件"成本后优势消失即否决"）。
4. **执行/模拟盘改造**（主方案 Phase 3 之后）：整数手、保证金、做空、盯市、换月移仓、前端下单数学重写。**只有走到模拟盘/实盘才需要**，回测阶段不阻塞。

## 10. 验收锚点

真实落地（走到模拟盘）前，至少确认：

- 期货能入链且连续序列通过[拼接管线验收标准](../etf-all-weather-data-sources/futures-continuous-contract-pipeline.md)第 7 节。
- 回测对 7-sleeve（含商品篮子）宇宙通过，且成本敏感性显示成本后仍有增量，否则按停止条件保留 6-sleeve 基线。
- 模拟盘能表达整数手 + 保证金占用 + 逐日盯市 + 换月移仓，且与连续序列换月日对齐。
- 前端 7 个 sleeve 展示正确，期货名义价值/现金占用口径正确。
