# ETF All-Weather Data Commands

本文记录 ETF all-weather 数据更新、定时任务、日志查看和人工核对常用指令。

## 1. 启动 WSL

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

`data/tradepilot.duckdb` 和 `data/lakehouse/` 是本地生成数据，不随仓库共享。新 clone 或缺少 lakehouse parquet 时，`./scripts/update-etf-aw` 会自动从项目定义的历史起点回补：ETF 行情和交易日历从 `2016-01-01`，宏观/利率从 `2025-01-01`。

## 2. 自动更新定时任务

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

## 3. 日志

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

## 4. 手动更新数据

自动补缺、下载最新数据并重建 derived 数据：

```bash
./scripts/update-etf-aw
```

只查看将要执行的更新计划，不写数据：

```bash
./scripts/update-etf-aw --dry-run
```

强制忽略已有水位并完整重跑项目定义历史：

```bash
./scripts/update-etf-aw --full-refresh
```

指定 ETF 和时间范围：

```bash
./scripts/update-etf-aw --codes 510300.SH,159845.SZ --start 2026-06-01 --end 2026-06-07
```

扩大回补窗口，例如回补最近 90 天：

```bash
./scripts/update-etf-aw --repair-days 90
```

## 5. 查看 ETF 数据

查看某只 ETF 的日度数据：

```bash
./scripts/view-etf-aw 510300 2026-06-01 2026-06-07
```

查看月度 rebalance snapshot：

```bash
./scripts/view-etf-aw 510300 2026-04-01 2026-06-07 --dataset snapshot
```

导出为 CSV：

```bash
./scripts/view-etf-aw 510300 2026-06-01 2026-06-07 --csv /tmp/510300.csv
```

## 6. 外部网站数据导出

从多个外部来源下载同一只 ETF 的日线，导出 CSV，并自动生成交叉对比文件：

```bash
./scripts/export-etf-aw-sources 511010 2026-06-01 2026-06-07
```

默认来源是 `eastmoney,tencent,sina`。输出目录默认在：

```text
data/source-review/
```

指定来源：

```bash
./scripts/export-etf-aw-sources 510300 2026-05-01 2026-06-07 --sources eastmoney,tencent,sina
```

尝试导出 Investing 页面表格：

```bash
./scripts/export-etf-aw-sources 511010 2026-06-01 2026-06-07 --sources investing --investing-url https://cn.investing.com/etfs/guotai-sse-deliverable-5-tb-historical-data
```

尝试导出雪球数据时通常需要从浏览器复制 Cookie：

```bash
./scripts/export-etf-aw-sources 511010 2026-06-01 2026-06-07 --sources xueqiu --xueqiu-cookie 'xq_a_token=...; u=...'
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
./scripts/export-etf-aw-full-history
```

全量脚本默认来源是 `local,tencent,sina`，默认时间是 `2016-01-01` 到今天。`local` 表示本地最终数据 `derived.etf_aw_sleeve_daily`。输出目录默认在：

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
./scripts/export-etf-aw-full-history --sources local,eastmoney,tencent,sina
```

若东方财富、雪球或 Investing 被网站限制，失败会写入 `all_codes_errors.csv`。

## 7. 数据质量检查

运行 ETF all-weather 数据检查：

```bash
python -m tradepilot.etl.check_etf_aw_data
```

运行 ETL 测试：

```bash
python -m unittest discover -s tests/etl -p 'test_*.py'
```

检查后端能否正常导入：

```bash
python -c "from tradepilot.main import app; print('OK')"
```

## 8. 无人登录时继续运行

当前使用的是 user systemd timer。若 WSL 发行版没有启动，任务不会凭空执行；若需要用户未登录时也继续运行，需要用有权限的账号执行：

```bash
sudo loginctl enable-linger nixos
```

检查 linger 状态：

```bash
loginctl show-user nixos -p Linger -p State -p RuntimePath
```
