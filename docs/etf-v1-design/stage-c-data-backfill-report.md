# Stage C 数据回补报告

日期：2026-05-01

## 范围

本文档记录本次在本地 TradePilot 数据库和 lakehouse 上执行的 Stage C ETF 全天候数据回补。

本次回补路径：

- `reference.trading_calendar.full_history`
- `reference.rebalance_calendar.monthly_post_20`
- `reference.etf_aw_sleeves.frozen_v1`
- `market.etf_daily`
- `market.etf_adj_factor`
- `derived.etf_aw_sleeve_daily`

## 环境

- 分支：`data-ingestion-stage-c-design`
- 数据库：`data/tradepilot.duckdb`
- Lakehouse 根目录：`data/lakehouse`
- 数据源：Tushare
- Tushare 配置：`.env` 中包含 `TUSHARE_TOKEN`；token 值未打印、未提交。
- 最终测试命令：`python -m unittest -v tests/etl/test_stage_b.py tests/etl/test_stage_c.py`
- 最终自动化测试结果：`31 tests passed`

## 摘要

Stage C ETF 全天候 v1 数据基座已完成本地回补。

首先完成 reference 基座：

1. SH/SZ 交易日历全历史，范围为 `2016-01-01` 到 `2026-05-01`。
2. 基于 SH/SZ 共同开市日生成每月 20 日后调仓日历。
3. 物化 ETF 全天候 v1 frozen sleeve universe。

随后为 frozen sleeve universe 回补 market 和 derived 数据：

- `510300.SH`
- `159845.SZ`
- `511010.SH`
- `518850.SH`
- `159001.SZ`

`derived.etf_aw_sleeve_daily` 是 adjustment-aware 日频面板，只包含日线数据和复权因子同时可用的观测行。

单位口径沿用 Tushare `fund_daily` 源字段：`volume` 为手，`amount` 为千元人民币；Stage C derived parquet 未做单位换算。

## 数据结果

| 数据集 | 范围 | 行数 | 说明 |
| --- | --- | ---: | --- |
| `canonical_trading_calendar` `SH` | `2016-01-01` 到 `2026-05-01` | 3,774 | 自然日覆盖 |
| `canonical_trading_calendar` `SZ` | `2016-01-01` 到 `2026-05-01` | 3,774 | 自然日覆盖 |
| `canonical_rebalance_calendar` | `2016-01-20` 到 `2026-04-20` | 124 | 每月 20 日后共同开市日规则 |
| `canonical_sleeves` | n/a | 5 | ETF 全天候 v1 frozen sleeves |
| `market.etf_daily` | 按标的不同 | 10,183 | normalized parquet |
| `market.etf_adj_factor` | 按标的不同 | 10,182 | normalized parquet |
| `derived.etf_aw_sleeve_daily` | 按标的不同 | 10,181 | derived parquet，124 个按月分区 |

分 sleeve 覆盖情况：

| Sleeve | `market.etf_daily` | `market.etf_adj_factor` | `derived.etf_aw_sleeve_daily` |
| --- | ---: | ---: | ---: |
| `159001.SZ` | 2,506 | 2,507 | 2,506 |
| `159845.SZ` | 1,232 | 1,232 | 1,232 |
| `510300.SH` | 2,507 | 2,507 | 2,507 |
| `511010.SH` | 2,507 | 2,506 | 2,506 |
| `518850.SH` | 1,431 | 1,430 | 1,430 |

最终 watermark：

| 数据集 | 数据源 | latest fetched date |
| --- | --- | --- |
| `reference.trading_calendar` | `tushare` | `2026-05-01` |
| `market.etf_daily` | `tushare` | `2026-04-30` |
| `market.etf_adj_factor` | `tushare` | `2026-04-30` |

## 执行记录

### 交易日历

执行路径：

- `ETLService().run_bootstrap("reference.trading_calendar.full_history")`

结果：

- 状态：`success`
- 请求范围：`2016-01-01` 到 `2026-05-01`
- 总窗口数：`125`
- 已处理窗口数：`125`
- 跳过窗口数：`0`
- 最终 SH/SZ 覆盖：完整
- 重复 `(exchange, trade_date)` 业务键：`0`
- 最终 validation：通过

### 调仓日历

执行路径：

- `ETLService().run_bootstrap("reference.rebalance_calendar.monthly_post_20")`

结果：

- 状态：`success`
- 请求范围：`2016-01-01` 到 `2026-05-01`
- 总月份数：`124`
- 已处理月份数：`124`
- 写入记录数：`124`

### Frozen sleeves

执行路径：

- `ETLService().run_bootstrap("reference.etf_aw_sleeves.frozen_v1")`

结果：

- 状态：`success`
- 写入记录数：`5`
- validation 通过：
  - frozen codes 完全匹配
  - sleeve role 受支持
  - listing exchange 与代码后缀匹配
  - exposure note 存在
  - canonical instruments 可用

### ETF 日线

执行路径：

- `ETLService().run_dataset_sync("market.etf_daily", request)`
- 请求窗口：`2016-01-01` 到 `2026-05-01`
- 请求标的：5 个 ETF 全天候 frozen sleeves
- trigger mode：`backfill`

结果：

- 状态：`success`
- 发现记录数：`10,183`
- 写入记录数：`10,183`
- watermark 已更新

### ETF 复权因子

执行路径：

- 先执行一次全窗口 `ETLService().run_dataset_sync("market.etf_adj_factor", request)`
- 随后对同一组 5 个 frozen sleeves 执行按月回补

按月回补原因：

- Tushare `fund_adj` 对全窗口请求每个标的只返回约 2,000 行。
- 需要按月回补才能恢复更早历史复权因子。
- 快速按月循环曾触发 Tushare 频率限制：`400` calls per minute。
- 续跑时先等待频率窗口重置，并在月度请求之间增加节流。

结果：

- 状态：`success`
- 完整回补后写入记录数：`10,182`
- watermark 已更新

## 数据缺口与处理

Tushare 源端未返回以下两条复权因子：

| 标的 | 交易日 | 日线数据 | 复权因子 |
| --- | --- | --- | --- |
| `511010.SH` | `2020-09-18` | 存在 | 缺失 |
| `518850.SH` | `2020-09-18` | 存在 | 缺失 |

对这两个标的和日期执行过单日精确重试，Tushare 返回 `0` 行。

因此 derived panel 采用 adjustment-available 语义：

- `derived.etf_aw_sleeve_daily` 由 `market.etf_daily` 和 `market.etf_adj_factor` 内连接生成。
- 缺少复权因子的行不会进入 derived panel。
- 这样可以保持 `adj_factor`、`adj_close`、`adj_pct_chg` 语义明确，避免对源端缺口做静默 forward-fill。

## 验证

自动化测试：

```bash
python -m unittest -v tests/etl/test_stage_b.py tests/etl/test_stage_c.py
```

结果：

- `31` 个测试通过

额外本地校验：

- SH/SZ 交易日历从 `2016-01-01` 到 `2026-05-01` 覆盖完整。
- `canonical_rebalance_calendar` 包含 `124` 条月度记录。
- `canonical_sleeves` 包含 5 个 ETF 全天候 frozen sleeves。
- `derived.etf_aw_sleeve_daily` 包含 `10,181` 行，写入 `124` 个按月分区。
- Stage C derived validation 通过：
  - 非空
  - 无重复业务键
  - 复权因子存在
  - 复权收盘价为正
  - 只包含已知 frozen sleeves

## 数据产物

本次生成的本地产物：

- `data/tradepilot.duckdb` 中的 DuckDB 行
- `data/lakehouse/raw/` 下的 raw parquet
- `data/lakehouse/normalized/` 下的 normalized parquet
- `data/lakehouse/derived/` 下的 derived parquet

这些产物是本地运行输出，不应提交到 Git。

## 结论

Stage C 已经具备本地 ETF 全天候 v1 数据基座。SH/SZ 交易日历基座、月度调仓日历、frozen sleeve universe、ETF 日线、ETF 复权因子，以及 adjustment-aware sleeve 日频衍生面板，均已可供后续策略和 workflow 使用。
