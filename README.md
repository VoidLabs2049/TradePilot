# TradePilot

TradePilot 是一个面向 A 股投资决策的本地化看板系统，目标不是做券商终端替代品，而是把 **市场概览、个股分析、交易计划、每日扫描、调度自动化** 串成一个可以持续使用的个人工作台。

当前项目已经从纯 Mock/V1 原型推进到 **AKShare 主路径 + Tushare 补强 + DuckDB 本地存储 + FastAPI/React 可视化** 的阶段，并具备基础的盘后自动化能力。

---

## 当前能力概览

### 已实现

- **市场概览**
  - 大盘指数快照
  - 行业 / 概念板块强弱
  - 市场 breadth 快照
  - 交易时段状态判断
- **个股分析**
  - MACD / 金叉死叉 / 背离 / 成交量异动
  - PE / PB 分位数与估值分析
  - 综合评分与信号解释
- **交易计划与持仓管理**
  - 持仓 CRUD
  - 交易记录
  - 交易计划创建、状态跟踪、止盈止损监控
- **每日扫描与预警**
  - 扫描自选股
  - 扫描持仓股
  - 固定扫描核心标的（指数 / ETF）
  - 生成扫描建议与系统预警
- **采集与自动化**
  - 手动触发 market / news / bilibili sync
  - APScheduler 驱动的定时任务
  - 调度状态与最近运行历史展示

### 当前数据源策略

- **AKShare**：主要真实数据路径
  - 个股 / 指数日线
  - ETF 资金流
  - 北向资金
  - 估值与板块数据
- **Tushare**：补强数据源
  - `trade_cal`：交易日历
  - `daily_info`：市场日度统计 / breadth 补强
- **MockProvider**：当前默认开发配置
  - 代码里的 `DATA_PROVIDER` 目前默认仍是 `MOCK`
  - 如需走真实行情链路，需要切到 AKShare provider 并配置对应环境
- **DuckDB**：本地分析库与运行历史存储

### 当前已知问题

- `GET /api/signal/market_sentiment`：当前不可用。真实数据链路在 AKShare/Tushare 串行调用下耗时过长，接口在现有同步实现中会超时。
- `POST /api/briefing/scan/run`：当前不可用。扫描流程依赖 `market_sentiment` 与多段结构化数据拉取，现阶段无法在可接受时间内稳定返回。
- TODO：为外部数据请求补充单次超时与 fail-fast 策略，并缩短 `market_sentiment` 默认查询窗口。
- TODO：将 `scan/run` 改为后台任务模式或拆分扫描阶段，避免单个同步请求阻塞。

---

## 技术栈

- **后端**：Python + FastAPI + DuckDB
- **前端**：React 18 + TypeScript + Vite + Ant Design + `@ant-design/charts`
- **数据源**：AKShare + Tushare
- **调度**：APScheduler
- **开发环境**：Nix Flakes

---

## 界面与路由

当前前端页面：

- `/` — **市场概览**
- `/dashboard` — **仪表盘**
- `/analysis` — **个股分析**
- `/sectors` — **行业地图**
- `/portfolio` — **持仓管理**
- `/plans` — **交易计划**

其中 Dashboard 目前已经包含：

- 指数卡片
- 市场情绪
- 资金面
- 今日扫描建议
- 最新预警
- 调度器状态
- 最近调度历史
- 持仓盈亏
- 活跃交易计划

---

## 快速开始

### 1. 进入开发环境

```bash
nix develop
```

### 2. 配置环境变量

在仓库根目录创建 `.env`：

```bash
TUSHARE_TOKEN=your_tushare_token
```

说明：

- 没有 `TUSHARE_TOKEN` 时，项目仍可启动
- 但 Tushare 补强路径会不可用，交易日历和市场日度统计不会落库

如果你希望启用真实行情主路径，还需要把 `tradepilot/config.py` 中的 `DATA_PROVIDER` 切到 `DataProviderType.AKSHARE`。

### 3. 启动后端

```bash
python -m uvicorn tradepilot.main:app --reload
```

后端地址：

- Swagger: http://localhost:8000/docs
- Health: http://localhost:8000/api/health

### 4. 启动前端

```bash
cd webapp
yarn install
yarn dev
```

前端地址：

- http://localhost:5173

---

## 常用开发命令

### 后端

```bash
# 启动 API
python -m uvicorn tradepilot.main:app --reload

# 运行测试
python -m unittest discover
```

### 前端

```bash
cd webapp

# 开发
yarn dev

# 构建
yarn build
```

---

## 手动触发 API

### 采集相关

- `POST /api/collector/market/sync`
- `POST /api/collector/news/sync`
- `POST /api/collector/bilibili/sync`
- `GET /api/collector/runs`
- `GET /api/collector/status`

### 扫描与预警

- `POST /api/briefing/scan/run`
- `GET /api/briefing/scan/latest`
- `GET /api/briefing/alerts`
- `POST /api/briefing/alerts/{alert_id}/read`

### 调度器

- `GET /api/scheduler/status`
- `GET /api/scheduler/history`

---

## 调度器行为（当前版本）

当前 `lifespan` 启动时会自动启动 scheduler。

默认任务：

- `market_sync`：周一到周五 16:00
- `news_sync`：周一到周五 9:00–15:30，每 30 分钟
- `daily_scan`：周一到周五 16:30

说明：

- `market_sync` 和 `daily_scan` 会先检查 Tushare `trade_cal`
- 如果是非交易日，会自动 `skip`
- 每次运行都会写入 `scheduler_history`
- 失败会生成系统级 `alerts`

---

## 项目结构

```text
tradepilot/
├── main.py                     # FastAPI 入口 + scheduler lifespan
├── config.py                   # 路径、数据源、.env 读取、Tushare 配置
├── db.py                       # DuckDB 连接与表初始化
│
├── api/
│   ├── market.py               # 行情数据接口
│   ├── analysis.py             # 技术/估值/轮动分析接口
│   ├── signal.py               # 信号与评分接口
│   ├── portfolio.py            # 持仓管理接口
│   ├── trade_plan.py           # 交易计划接口
│   ├── collector.py            # 手动采集接口
│   ├── briefing.py             # 每日扫描 / 预警接口
│   └── scheduler_api.py        # 调度状态 / 历史接口
│
├── data/
│   ├── provider.py             # 结构化数据 provider 抽象
│   ├── mock_provider.py        # Mock provider
│   ├── akshare_provider.py     # AKShare 主数据源
│   └── tushare_client.py       # Tushare trade_cal / daily_info 补强
│
├── analysis/
│   ├── technical.py
│   ├── valuation.py
│   ├── fund_flow.py
│   ├── sector_rotation.py
│   ├── signal.py
│   └── risk.py
│
├── ingestion/
│   ├── models.py
│   └── service.py              # market/news/bilibili sync 与落库
│
├── scanner/
│   └── daily.py                # 每日扫描、预警、核心标的扫描
│
├── scheduler/
│   ├── engine.py               # APScheduler 启停与 job 注册
│   └── jobs.py                 # market/news/scan job 与 history 记录
│
└── summary/
    ├── service.py              # 市场概览与 5 分钟简报
    ├── models.py
    └── cache.py

webapp/
├── src/App.tsx
├── src/services/api.ts
└── src/pages/
    ├── MarketSummary/
    ├── Dashboard/
    ├── StockAnalysis/
    ├── SectorMap/
    ├── Portfolio/
    └── TradePlan/
```

---

## DuckDB 里的关键表

### 行情与分析基础

- `stock_daily`
- `stock_weekly`
- `stock_monthly`
- `index_daily`
- `etf_flow`
- `northbound_flow`
- `margin_data`
- `stock_valuation`
- `sector_data`

### Tushare 补强

- `trading_calendar`
- `market_daily_stats`

### 运行与业务状态

- `ingestion_runs`
- `scheduler_history`
- `daily_scan_results`
- `alerts`
- `portfolio`
- `trades`
- `trade_plan`

---

## 设计原则

- **本地优先**：DuckDB 一库到底，不引入额外服务
- **渐进增强**：在 V1 基础上逐层扩展，不大拆重构
- **双数据源分工**：AKShare 主路径，Tushare 只补弱项
- **自动化优先于炫技**：先把每日链路跑通，再继续扩展 LLM / research

---

## 当前限制与已知取舍

- Tushare 当前只接了 `trade_cal` 和 `daily_info`
- `daily_info` 补强在手动大范围同步时做了天数限制，避免打爆 Tushare 调用频率
- 调度历史和预警会持续写入 DuckDB，目前还没有归档 / 清理策略
- 社区采集、研究项目层、LLM 简报生成还未进入当前稳定版本

---

## 下一步可继续做的方向

- 引入更多 Tushare 数据面（如 `index_daily`、`daily_basic`）
- 让市场概览显式展示 `market_daily_stats` 补强结果
- 增加 scheduler 手动 trigger / 启停控制
- 补充简报生成与研究工作流
- 继续优化 Dashboard 的调度与预警交互体验

---

## 文档

- `docs/futures-v2-design/V2-投资工作流设计.md` — 当前中长期演进设计
- `docs/系统设计.md` — 原始系统设计说明
- `docs/投资策略.md` — 策略原文
