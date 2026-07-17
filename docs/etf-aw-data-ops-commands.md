# ETF All-Weather Data Commands

本文记录 ETF all-weather 数据更新、定时任务、日志查看和人工核对常用指令。

生产补数据脚本放在 `scripts/etl/`；人工核验、查看和外部来源对比脚本放在 `scripts/etl-review/`。

## 1. 脚本边界

| 入口 | 类型 | 用途 | Python 入口 | 写入位置 |
| --- | --- | --- | --- | --- |
| `scripts/etl/update-etf-aw.sh` | 正常数据 pipeline | 手动补缺、`--scheduled` 定时包装、下载项目定义的正常数据源并重建 derived 数据 | `tradepilot.etl.update_etf_aw_data` | `data/tradepilot.duckdb`、`data/lakehouse/`、`logs/etf-aw-update.log` |
| `scripts/etl-review/export-etf-aw-sources.sh` | 人工 review | `--view` 查看本地结果；导出单只 ETF 或 `--full-history` 全历史的本地/外部来源日线并生成差异对比 | `tools.etl_review.view_etf_aw`、`tools.etl_review.export_etf_aw_sources` | 默认只读；传 `--csv` 或做来源对比时写 CSV |
| `python -m tools.etl_review.check_etf_aw_data` | 人工检查 | 检查 DuckDB 元数据、lakehouse parquet、derived/read model 一致性 | `tools.etl_review.check_etf_aw_data` | 只读 |
| `python -m tools.etl_review.view_parquet` | 人工查看 | 查看任意 parquet 文件或目录，适合临时排查字段和样例行 | `tools.etl_review.view_parquet` | 默认只读；传 `--csv` 时写指定 CSV |

主入口只有两个：

- 正常补数/更新：`./scripts/etl/update-etf-aw.sh`
- 人工查看/来源对比：`./scripts/etl-review/export-etf-aw-sources.sh`

`scripts/etl/` 是日常生产补数据入口，只更新项目 ETL 注册表里的正常数据源；当前 ETF all-weather 路径通过 `tradepilot.etl.sources.tushare` 和项目内 derived 构建完成，不调用 `eastmoney`、`tencent`、`sina` 这类外部网站接口。

`scripts/etl-review/` 和 `tools/etl_review` 是人工核验、排查、来源对比工具，不作为调度主路径，也不写入 DuckDB 或 lakehouse 的生产数据集。

## 2. 启动 WSL

在 Windows PowerShell 或 Windows Terminal 中运行：

```powershell
wsl
```

如果需要指定发行版：

```powershell
wsl -d NixOS
```

进入 WSL 后切到项目目录：

```bash
cd /home/nixos/workspace/TradePilot
```

远端或新机器首次跑 ETF all-weather 数据前，确认仓库根目录 `.env` 里已有 Tushare token：

```bash
TUSHARE_TOKEN=your_tushare_token
```

`data/tradepilot.duckdb` 和 `data/lakehouse/` 是本地生成数据，不随仓库共享。新 clone 或缺少 lakehouse parquet 时，`./scripts/etl/update-etf-aw.sh` 会自动从项目定义的历史起点回补：ETF 行情和交易日历从 `2016-01-01`，宏观/利率从 `2025-01-01`。

## 3. 自动更新定时任务

定时任务应调用正常 pipeline 的包装模式：

```bash
./scripts/etl/update-etf-aw.sh --scheduled
```

`--scheduled` 会进入仓库根目录、加锁、进入 `nix develop`，先更新行情和 derived 数据，再自动补齐默认模拟盘账户的缺失观察日，并把日志写到 `logs/etf-aw-update.log`。数据流水线失败时不会继续更新模拟盘观察。

查看定时任务是否启用、下一次什么时候运行：

```bash
systemctl --user list-timers --all | grep tradepilot
```

查看 timer 状态：

```bash
systemctl --user status tradepilot-etf-aw-update.timer
```

手动触发一次自动更新任务：

```bash
systemctl --user start tradepilot-etf-aw-update.service
```

查看本次 service 执行状态：

```bash
systemctl --user status tradepilot-etf-aw-update.service
```

停止并禁用自动更新：

```bash
systemctl --user disable --now tradepilot-etf-aw-update.timer
```

重新启用自动更新：

```bash
systemctl --user enable --now tradepilot-etf-aw-update.timer
```

## 4. 日志

实时查看自动更新日志：

```bash
tail -f logs/etf-aw-update.log
```

查看最近 120 行日志：

```bash
tail -n 120 logs/etf-aw-update.log
```

查看 systemd 日志：

```bash
journalctl --user -u tradepilot-etf-aw-update.service -n 120 --no-pager
```

## 5. 手动更新数据

自动补缺、下载最新数据并重建 derived 数据：

```bash
./scripts/etl/update-etf-aw.sh
```

这条命令是正常数据 pipeline 的手动入口。它更新 `reference.trading_calendar`、`market.etf_daily`、`market.etf_adj_factor`、`macro.slow_fields`、`rates.daily_rates`、`rates.lpr`、`rates.gov_curve_points`，再重建 ETF all-weather derived 数据。它不会请求 `eastmoney`、`tencent`、`sina`。

只查看将要执行的更新计划，不写数据：

```bash
./scripts/etl/update-etf-aw.sh --dry-run
```

强制忽略已有水位并完整重跑项目定义历史：

```bash
./scripts/etl/update-etf-aw.sh --full-refresh
```

指定 ETF 和时间范围：

```bash
./scripts/etl/update-etf-aw.sh --codes 510300.SH,159845.SZ --start 2026-06-01 --end 2026-06-07
```

默认代码列表已包含纳指 ETF `513100.SH`。仅补回该标的数据时：

```bash
./scripts/etl/update-etf-aw.sh --codes 513100.SH --start 2025-01-01 --end 2026-07-17
```

扩大回补窗口，例如回补最近 90 天：

```bash
./scripts/etl/update-etf-aw.sh --repair-days 90
```

## 6. 查看 ETF 数据

查看某只 ETF 的日度数据：

```bash
./scripts/etl-review/export-etf-aw-sources.sh --view 510300 2026-06-01 2026-06-07
```

查看月度 rebalance snapshot：

```bash
./scripts/etl-review/export-etf-aw-sources.sh --view 510300 2026-04-01 2026-06-07 --dataset snapshot
```

导出为 CSV：

```bash
./scripts/etl-review/export-etf-aw-sources.sh --view 510300 2026-06-01 2026-06-07 --csv /tmp/510300.csv
```

查看任意 parquet 文件或分区目录：

```bash
python -m tools.etl_review.view_parquet data/lakehouse/derived/derived.etf_aw_sleeve_daily --schema --limit 5
```

## 7. 人工来源对比

这个入口只用于人工复核本地结果，不属于正常数据更新 pipeline。默认对比 `local,tencent,sina`：`local` 是本地最终数据 `derived.etf_aw_sleeve_daily`，`tencent/sina` 是外部网站来源。需要东方财富、雪球或 Investing 时必须显式传 `--sources`。

对比单只 ETF 的指定日期区间：

```bash
./scripts/etl-review/export-etf-aw-sources.sh 511010 2026-06-01 2026-06-07
```

输出目录默认在：

```text
data/source-review/
```

指定来源：

```bash
./scripts/etl-review/export-etf-aw-sources.sh 510300 2026-05-01 2026-06-07 --sources local,tencent,sina
```

如果要把东方财富加入人工检查：

```bash
./scripts/etl-review/export-etf-aw-sources.sh 510300 2026-05-01 2026-06-07 --sources local,eastmoney,tencent,sina
```

尝试导出 Investing 页面表格：

```bash
./scripts/etl-review/export-etf-aw-sources.sh 511010 2026-06-01 2026-06-07 --sources investing --investing-url https://cn.investing.com/etfs/guotai-sse-deliverable-5-tb-historical-data
```

尝试导出雪球数据时通常需要从浏览器复制 Cookie：

```bash
./scripts/etl-review/export-etf-aw-sources.sh 511010 2026-06-01 2026-06-07 --sources xueqiu --xueqiu-cookie 'xq_a_token=...; u=...'
```

每次运行会输出：

```text
combined.csv     # 所有来源合并后的长表
comparison.csv   # 按 trade_date 横向展开后的逐日差异表
summary.csv      # 每个字段的最大差异、缺失数量、差异日期
errors.csv       # 失败来源记录；只有失败时才生成
```

按 5 只 ETF all-weather sleeve 做全量历史导出和对比：

```bash
./scripts/etl-review/export-etf-aw-sources.sh --full-history
```

全量模式默认来源是 `local,tencent,sina`，默认时间是 `2016-01-01` 到今天。输出目录默认在：

```text
data/source-review/full-history/
```

全量结果会生成：

```text
all_codes_combined.csv        # 5 只 ETF 所有来源的合并长表
all_codes_comparison.csv      # 5 只 ETF 的逐日横向对比
all_codes_summary.csv         # 5 只 ETF 的字段差异汇总
all_codes_mismatch_rows.csv   # 有差异或缺失来源的日期行
all_codes_errors.csv          # 失败来源记录；只有失败时才生成
run_manifest.csv              # 每只 ETF、每个来源的行数和状态
```

如果要把东方财富也加入全量检查：

```bash
./scripts/etl-review/export-etf-aw-sources.sh --full-history --sources local,eastmoney,tencent,sina
```

若东方财富、雪球或 Investing 被网站限制，失败会写入 `all_codes_errors.csv`。

## 8. 数据质量检查

运行 ETF all-weather 数据检查：

```bash
python -m tools.etl_review.check_etf_aw_data
```

运行 ETL 测试：

```bash
python -m unittest discover -s tests/etl -p 'test_*.py'
```

检查后端能否正常导入：

```bash
python -c "from tradepilot.main import app; print('OK')"
```

## 9. 无人登录时继续运行

当前使用的是 user systemd timer。若 WSL 发行版没有启动，任务不会凭空执行；若需要用户未登录时也继续运行，需要用有权限的账号执行：

```bash
sudo loginctl enable-linger nixos
```

检查 linger 状态：

```bash
loginctl show-user nixos -p Linger -p State -p RuntimePath
```
