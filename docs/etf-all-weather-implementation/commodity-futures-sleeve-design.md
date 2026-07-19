# 全天候引入商品期货 Sleeve 设计方案

本文件是设计与影响评估，不是实现。评审通过后再按分阶段计划编码。相关背景见
[ETF 全天候策略优化阶段计划](./strategy-improvement-research-design.md)（其中第 2.2、6.3 节已列出大宗商品候选调研，但当时定位为**商品 ETF/基金**）与
[数据源地图](../etf-all-weather-data-sources/data-source-map.md)（其中已注明 `continuous-contract logic may be needed later`，即真期货的连续合约逻辑被显式推迟）。

## 1. 结论先行

本方案要做的：把商品作为**一个新的独立资产类别（`commodity` sleeve role）**引入全天候，标的为**真商品期货主力连续合约**，初期候选池为：

| 品种 | 代码含义 | 交易所 | 板块归类 |
| --- | --- | --- | --- |
| AU | 黄金 | SHFE | 贵金属 |
| AL | 铝 | SHFE | 有色 |
| CU | 铜 | SHFE | 有色 |
| I | 铁矿石 | DCE | 黑色 |
| M | 豆粕 | DCE | 农产品 |
| RB | 螺纹钢 | SHFE | 黑色 |
| SC | 原油 | INE | 能化 |
| P | 棕榈油 | DCE | 农产品 |
| TA | PTA | CZCE | 能化 |

本方案**不做**（与既有研究纪律一致）：

- 不在本阶段直接进入实盘或模拟盘下单。
- 不一次性把 9 个品种全部加入组合权重。
- 不在第一阶段就改动 regime 打分与权重分配——先把数据接入和单变量回测跑通。
- 不用真期货的杠杆特性去放大组合暴露；期货用于**获取商品价格暴露**，不用于加杠杆（详见 §4.3）。
- 不把 AU 期货与现有 `gold` sleeve（黄金 ETF `518850.SH`）重复计入同一风险暴露（详见 §5.2）。

**关键判断**：现有全天候的整条 ETL 链路——代码格式、交易所白名单、`instrument_type`、regime 四分组打分、"每个调仓日必须正好 N 个 sleeve"的校验——全部围绕**沪深两市 ETF** 写死。加纳指 ETF（同交易所、同代码格式、同类型）的 PR #33 已改动 25+ 后端文件、几乎每个 Stage 的测试。真商品期货在此之上还额外撞上**代码格式、交易所、类型白名单、regime 建模、主力换月拼接、保证金**六个新问题，改动量显著大于 #33。因此本方案强烈建议**先出文档、分阶段落地**，不一步到位。

## 2. 当前基线与冻结假设

当前 V2 使用 6 个 frozen sleeves（`equity_large / equity_small / equity_overseas / bond / gold / cash`），标的池定义在 `tradepilot/etl/etf_aw_universe.py:10`（`ETF_AW_SLEEVES`）。链路为：

```text
market / macro context
-> regime score（equity/bond/gold/cash 四分组）
-> risk budget（按角色写死的 tilt 表）
-> budgeted inverse-vol target weight（按角色写死的 cap）
-> monthly frozen weight
-> common backtest kernel
-> robustness / shadow evaluation
```

引入 commodity 会撞上的**冻结假设清单**（这是本方案影响评估的核心，逐项都会导致校验失败或语义失真，必须一并处理）：

| 冻结点 | 位置 | 现状假设 | 期货触发的问题 |
| --- | --- | --- | --- |
| 标的代码格式 | `tradepilot/etl/validators.py:125` `_ID_RE = ^\d{6}\.(SH\|SZ)$` | 六位数字 + SH/SZ | 期货代码为 `M.DCE`、`CU.SHFE`、`AU.INE` 等，会被拒 |
| 交易所白名单 | `tradepilot/etl/validators.py:84` | `["SH","SZ"]` | 期货在 SHFE/DCE/CZCE/INE，会被拒 |
| instrument_type 白名单 | `tradepilot/etl/validators.py:212` `.isin(["etf","index"])` | 只允许 etf/index | 需加 `future` |
| regime 四分组打分 | `tradepilot/etl/service.py:3624` `_regime_score_row`、`3643` `market_score` 公式 | 只聚成 equity/bond/gold/cash 四组，公式系数写死 | commodity 会被完全忽略；且商品与权益/黄金相关性方向不一致，不能简单塞入现公式 |
| market feature scope | `tradepilot/etl/service.py:196` | group 只有 equity/bond/gold/cash_score | 需加 commodity_score |
| 权重上限 cap | `tradepilot/etl/service.py:118` `_ETF_AW_TARGET_WEIGHT_CAPS` | 按角色写死 | 需补 commodity 档 |
| regime tilt 表 | `tradepilot/etl/service.py:251` 起多组 dict | 按角色写死 | 需为 commodity 补全每张表 |
| "正好 N 个 sleeve" 校验 | `service.py:3609/3762/5948/6055/6109`，及 `datasets.py` 中 `*.five_roles_per_rebalance_date` 等规则名 | 标的数量被验证器锁死 | 加品种即触发失败，须同步更新 |
| 下载代码清单（重复源） | `tradepilot/etl/update_etf_aw_data.py:23` `_ETF_AW_CODES` | 与 universe 重复硬编码 | 需同步 |
| 角色顺序常量 | `etf_aw_universe.py:74`、`constants.py:5` | 六角色写死 | 需加 commodity |

## 3. 标的映射与 sleeve 归类

真期货是**单品种、无内在权重**的合约，与"一个 sleeve = 一个 ETF"的既有模型不同。9 个品种需要一个明确的聚合模型，本方案给出两个候选，评审时二选一：

### 方案 A：单一 commodity sleeve（推荐用于第一阶段）

把 9 个品种聚合为**一个** `commodity` sleeve，内部按等风险或等权合成一条商品篮子净值曲线，对上层 regime/权重逻辑只暴露"一个 sleeve"。

- 优点：完全复用"一个 sleeve = 一条价格序列"的既有抽象，对 §2 的"正好 N 个 sleeve"校验冲击最小（sleeve 数从 6 变 7，而非变 15）。
- 缺点：黑色/有色/农产品/能化/贵金属的驱动因子差异被抹平；篮子内部权重成为一个隐藏设计选择。

### 方案 B：按板块分多个 sleeve

按 §1 表的板块归类拆成 `commodity_metals` / `commodity_black` / `commodity_energy` / `commodity_agri` 等多个 sleeve role。

- 优点：保留板块间的独立风险来源，regime tilt 可分板块。
- 缺点：sleeve 数量与角色表、tilt 表、cap 表全部膨胀；每个板块历史样本更短，统计稳定性更差；对既有校验冲击最大。

**建议**：第一阶段采用**方案 A（单一 commodity sleeve）**，把 9 品种合成一条商品篮子，先验证"商品作为一个资产类别是否给全天候带来独立风险来源"这一根本问题；确认有增量后，再评估是否值得拆板块（方案 B）。这与既有研究纪律"增加标的是为了补充独立风险来源，不是扩大数量"一致。

## 4. 真期货相对商品 ETF 多出来的三个新问题

这是"真期货"相对"商品 ETF 代理"多出来的、既有 ETL 完全没有处理过的复杂度。文档需正面回应，否则回测口径不可信。

### 4.1 主力合约换月与连续拼接

期货没有"一只永续证券"，主力合约每隔几周/几月换月。回测必须用**连续合约**（主力连续或指数连续），并对换月缺口做价格调整（后向复权拼接 back-adjustment），否则换月当日会出现虚假跳空收益。

- 需新增：`fut_mapping`（主力映射）→ 连续序列构造逻辑。这是 ETF 复权因子逻辑之外的**新数据处理管线**。构造规则与验收标准已单独成文：[主力连续合约拼接管线](../etf-all-weather-data-sources/futures-continuous-contract-pipeline.md)。
- point-in-time 纪律：换月映射本身必须 point-in-time，不能用未来才知道的主力信息回填历史。

### 4.2 期货无复权因子，但有近远月基差

ETF 走 `fund_adj` 复权因子；期货没有对应物，取而代之的是**基差/展期收益（roll yield）**。连续合约的收益里天然包含展期损益，回测的收益语义必须显式说明"是否已含 roll yield"，并与 ETF 的 total-return 语义区分（对照
[ETF 收益语义说明](../etf-all-weather-data-sources/etf-return-semantics-note.md)）。

### 4.3 保证金与杠杆——本方案的处理原则

期货天然带杠杆（保证金交易）。全天候是**风险平价/资产配置**框架，不是杠杆策略。处理原则：

- 期货仓位按**名义价值（notional）**参与权重分配，与 ETF 的市值口径对齐；权重之和仍为 1，商品 sleeve 的权重就是其名义暴露占比。
- 保证金只影响资金占用与现金管理，不改变"目标暴露 = 权重 × 组合净值"这一口径。
- 第一阶段回测**不引入杠杆放大**；商品 sleeve 的作用是提供一个新的价格暴露，不是加杠杆。杠杆化的资金效率话题（用期货保证金释放现金）留待独立评估，不混入本阶段。

## 5. Regime 与相关性建模问题（需评审决策）

### 5.1 market_score 公式需要重新推导，不能照抄

现公式（`service.py:3643`）：

```text
market_score = clamp(0.70*equity - 0.15*max(bond,0) - 0.15*max(gold,0), -100, 100)
```

它假设 bond/gold 是权益的对冲（系数为负）。商品的行为**不是**单一方向：

- 商品与权益在再通胀期正相关、在滞胀期与权益负相关——方向随宏观状态切换。
- 简单把 commodity_score 以固定系数塞进上式会引入错误的先验。

因此加 commodity 不是"补一个系数"，而是要**重新设计 regime 打分如何吸收商品信号**。第一阶段的保守做法：commodity 先只进入**风险预算/权重**层（提供独立暴露），**暂不进入 market_score 的方向性打分**，把打分公式的重新推导作为第二阶段单独课题。评审需确认这一取舍。

### 5.2 AU 期货与 gold ETF 的重复暴露

现有 `gold` sleeve 是黄金 ETF（`518850.SH`），候选池里的 AU 又是黄金期货。二者高度相关。处理选项（评审二选一）：

- 选项一：commodity 篮子**剔除 AU**，黄金暴露继续由现有 gold sleeve 承担，商品 sleeve 只含工业品/农产品/能化。
- 选项二：保留 AU 于商品篮子，但在组合层显式约束"gold + commodity 中的贵金属"合计暴露上限，避免集中押注黄金。

**建议选项一**：语义最干净，商品 sleeve 定位为"非黄金商品的独立暴露"，与既有 gold sleeve 正交。

## 6. 数据源（已实测）

已用项目现有 Tushare token（`tushare==1.4.24`，走 `TUSHARE_TOKEN`）对 9 个候选品种实测三类接口，结论如下。**关键：候选品种全部可获取，无 token 权限门槛，但连续序列必须自行拼接。**

### 6.1 三条硬结论

1. **交易所后缀是三字母，不是全称**。Tushare 期货 `ts_code` 用 `.SHF / .DCE / .ZCE / .INE`（郑商所是 `.ZCE`，不是 `CZCE`；上期所是 `.SHF`，不是 `SHFE`）。这直接影响 §2 的 validators 交易所白名单要放开成什么值——是 `SHF/DCE/ZCE/INE`，不是交易所全称。9 品种的连续根代码为：`AU.SHF / AL.SHF / CU.SHF / RB.SHF`（上期所）、`I.DCE / M.DCE / P.DCE`（大商所）、`SC.INE`（能源中心）、`TA.ZCE`（郑商所）。第一版用错后缀会全部返回空且不报错——这是最容易踩的坑。

2. **主力映射（`fut_mapping`）历史很深，主力连续日线（`fut_daily` 连续根）历史很浅**。实测：

   | 接口 | 覆盖 | 历史深度（示例） |
   | --- | --- | --- |
   | `fut_mapping`（主力映射） | 9 品种全部有 | M 到 2005-12、CU/AL 到 2005-12、AU 到 2008-01、TA 到 2006-12、P 到 2007-10、RB 到 2009-03、I 到 2013-10、SC 到 2018-03 |
   | `fut_daily`（连续根，如 `M.DCE`） | 9 品种全部有 | 一律仅 **2024-01 起（约 614 个交易日）** |
   | `fut_daily`（单合约，如 `M2609.DCE`） | 全部有 | 完整逐合约历史 |
   | `fut_basic`（合约元数据） | 4 交易所全部有 | SHFE 3552 / DCE 3230 / CZCE 2846 / INE 465 行合约，含 `multiplier`/`per_unit`/`trade_unit` |

   **含义**：Tushare 直接给的"连续日线"只有 ~2 年，不足以做全天候的长历史回测。要拿到 2005–2013 起的深历史，必须**自己拼**：用 `fut_mapping`（point-in-time 主力映射，深历史）+ `fut_daily` 逐合约日线（深历史）+ 后向复权换月拼接，构造连续序列。这实证了 §4.1 说的"新数据处理管线"不是可选项而是必需。

3. **无权限门槛**。9 品种、4 交易所的 `fut_basic/fut_daily/fut_mapping` 均正常返回，未触发积分/权限报错。项目现有 token 即可支撑本方案的数据接入。

### 6.2 数据源表（更新后）

| 数据项 | 主源 | 校验/fallback | 实测结论 |
| --- | --- | --- | --- |
| 合约元数据（`fut_basic`） | Tushare | 各交易所官网合约表 | ✅ 可用，含乘数/交易单位，用于名义价值换算 |
| 主力映射（`fut_mapping`） | Tushare | AKShare | ✅ 可用，深历史（2005–2018 起），拼接的锚点，须 point-in-time |
| 逐合约日线（`fut_daily` 单合约） | Tushare | AKShare | ✅ 可用，深历史，拼接的价格来源 |
| 主力连续日线（`fut_daily` 连续根） | Tushare | — | ⚠️ 仅 2024-01 起；**不能**作为长历史回测的唯一来源 |
| 保证金/乘数 | `fut_basic` + 交易所官网 | — | ✅ `fut_basic` 已含乘数 |

`tradepilot/data/tushare_client.py` 目前**无任何期货方法**（只有 `get_etf_daily`/`get_etf_adj_factor`），需新增 `get_futures_basic`/`get_futures_daily`/`get_futures_mapping`；连续拼接逻辑不属于 client，放在新的连续合约构造管线里。

数据可靠性验证沿用既有测试计划模板（对照
[数据可靠性测试计划](../etf-all-weather-data-sources/data-reliability-test-plan.md)），额外必查项：交易所后缀正确性（`.SHF/.ZCE` 易错）、换月边界、连续拼接跳空与后向复权、时区/交易日历（期货夜盘）、名义价值换算正确性。

## 7. 分阶段执行计划

### Phase 0：冻结实验合同

冻结候选品种清单、连续合约构造规则（主力连续 vs 指数连续、后向复权）、名义价值口径、数据区间、成本口径。产出连续合约构造规则说明文档。

### Phase 1：数据接入（不动策略）

- 新增 `future` instrument_type，放开 validators 的代码格式/交易所/类型白名单（§2 前三行）。
- 新增 tushare source adapter 的 futures dataset 与 `tushare_client` 期货方法。
- 实现主力连续拼接管线，把 9 品种连续日线落到 lakehouse（`market.futures_daily` 或类似 dataset）。
- 验收：9 品种连续序列可重建、通过换月边界与 point-in-time 检查、在看板可视化。**此阶段策略权重不变。**

### Phase 2：单变量回测对照

- 按方案 A 合成商品篮子净值，作为**单一 commodity sleeve** 接入。
- 逐个/整体加入商品 sleeve，与现有 6-sleeve 基线做单变量对照（每次只加一个新暴露）。
- 处理 §2 的"正好 N 个 sleeve"校验、cap 表、tilt 表、角色顺序常量（sleeve 从 6 → 7）。
- regime 打分层按 §5.1 保守处理：commodity 暂不进 market_score，只进权重层。
- 验收：成本后是否带来独立风险来源与增量，能否用收益/风险贡献解释；证据不足则**保留 6-sleeve 基线**是有效结论。

### Phase 3：regime 建模（条件触发）

仅当 Phase 2 证明商品有增量，才启动 market_score 公式的重新推导（§5.1），把商品的状态依赖相关性纳入 regime 打分。

### Phase 4：阶段总结报告

汇总数据接入、连续合约验证、单变量回测、失败项与限制，按既有五问模板（研究了什么 / 数据与方法 / 结果 / 保留还是拒绝 / 限制与下一步）产出可追溯报告。

## 8. 停止条件

沿用既有研究纪律，并针对期货补充：

- 连续合约拼接引入无法解释的换月跳空，或换月映射用到未来信息。
- 期货收益语义（是否含 roll yield）无法与 ETF 口径对齐说明。
- 商品 sleeve 与现有 gold sleeve 高度重复，改善只来自集中押注黄金。
- 成本后（含展期成本）优势消失。
- 结论仅全样本有效、walk-forward 无效，或轻微参数扰动后反转。
- 结果无法从连续合约构造规则、数据版本和 frozen artifact 重建。

## 9. 立即待办（评审通过后）

1. 评审确认：sleeve 归类方案 A/B（§3）、AU 重复暴露处理选项（§5.2）、regime 第一阶段保守取舍（§5.1）。
2. 冻结候选品种与连续合约构造规则（Phase 0）。
3. ~~核实 Tushare 期货接口的可用字段、历史深度与 token 门槛~~ —— 已实测完成，见 §6（9 品种全部可用、无门槛、连续序列须自拼）。
4. 定义 `future` instrument_type 与 validators 放开的最小改动清单。
5. 编写主力连续拼接管线并先跑通一个品种的可重建验证。

Phase 2-4 不属于立即待办，在 Phase 1 数据可信重建后依次启动。
