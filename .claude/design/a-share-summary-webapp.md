---
title: "A股市场概览 — 前后端自动刷新版"
status: in-progress
mode: both
created: 2026-03-09
updated: 2026-03-10
modules: [data-provider, api-routes, webapp-frontend]
---

# A股市场概览 — 前后端自动刷新版

## Overview

将现有 `skills/a-share-summary` 独立 CLI 脚本的功能迁移到 TradePilot 前后端框架中，实现一个自动刷新的 A 股市场概览页面。该页面支持两种模式：

1. **Daily 模式**：完整市场总结（大盘指数、市场情绪、行业/概念板块涨跌榜、个股涨跌 TOP10）
2. **5 分钟模式**：盘中简报（市场 Regime、关注板块/个股 watchlist、告警信号）

后端通过新的 API 端点提供数据，前端实现定时轮询自动刷新。

## Goals

- [ ] 后端新增市场概览 API（daily + 5m 两种模式）
- [ ] 数据获取逻辑从独立脚本迁移到后端服务层
- [ ] 前端新增"市场概览"页面，支持 daily / 5m 模式切换
- [ ] 前端支持可配置间隔的自动刷新（盘中默认 5 分钟）
- [ ] Watchlist（关注板块/个股）支持前端配置和持久化

## Constraints

- 数据源仍为 akshare，无需 API key
- akshare API 有调用频率限制，需控制并发（当前脚本用 ThreadPoolExecutor 最多 8 线程）
- 盘中数据仅在交易时段（9:30-15:00）有意义，非交易时段应显示最近收盘数据
- 不引入 WebSocket/SSE 复杂度，使用前端轮询 + 后端缓存即可满足 5 分钟刷新需求
- 保持与现有 TradePilot 架构一致（Provider 模式、FastAPI router、React 页面模式）

## Scope

### Modules Involved

| Module | Current Role | Planned Changes |
|--------|-------------|-----------------|
| Data Provider | 11 个 DataProvider 接口（日/周/月K、ETF、两融等） | 新增 `MarketSnapshotService` 独立于 DataProvider，直接调用 akshare 快照类 API |
| API Routes | 6 组 router（market/portfolio/analysis/signal/trade_plan/collector） | 新增 `api/summary.py` router，提供 daily/5m 市场概览端点 |
| Frontend | 5 个页面 + api.ts 服务层 | 新增 MarketSummary 页面 + 对应 API 调用 + 自动刷新逻辑 |

### Key Files

| File | Role | Impact |
|------|------|--------|
| `tradepilot/summary/__init__.py` | 新建 - 市场概览服务包 | 新增 |
| `tradepilot/summary/service.py` | 新建 - 核心数据获取+聚合逻辑 | 新增（从 fetch_a_share.py 迁移） |
| `tradepilot/summary/models.py` | 新建 - Pydantic v2 响应模型 | 新增 |
| `tradepilot/summary/cache.py` | 新建 - 内存缓存层（TTL 控制） | 新增 |
| `tradepilot/api/summary.py` | 新建 - FastAPI router | 新增 |
| `tradepilot/main.py` | FastAPI 入口 | 挂载新 router |
| `webapp/src/pages/MarketSummary/index.tsx` | 新建 - 市场概览页面 | 新增 |
| `webapp/src/services/api.ts` | API 调用封装 | 新增 summary 相关函数 |
| `webapp/src/App.tsx` | 路由配置 | 新增 /summary 路由 |

### Out of Scope

- 不修改现有 DataProvider ABC 接口（快照类 API 和历史 K 线 API 模式不同）
- 不修改现有 5 个页面的功能
- 不引入 WebSocket/SSE
- 不做 DuckDB 持久化（概览数据为即时快照，无需历史存储）
- 不实现后端定时任务（由前端轮询驱动）

## Design

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Frontend (React)                                         │
│                                                          │
│  MarketSummary Page                                      │
│  ├── Mode Switch (Daily / 5m)                            │
│  ├── Auto-refresh Toggle + Interval Config               │
│  ├── Daily View                                          │
│  │   ├── IndexOverview (6 指数卡片)                       │
│  │   ├── MarketBreadth (情绪指标)                         │
│  │   ├── SectorRanking (行业/概念 涨跌榜)                 │
│  │   └── TopStocks (涨跌幅 TOP10)                        │
│  └── 5m View                                             │
│      ├── RegimeIndicator (市场状态仪表)                    │
│      ├── SectorWatchlist (关注板块表格)                    │
│      ├── StockWatchlist (关注个股表格)                     │
│      └── AlertList (告警列表)                             │
│                                                          │
│  useInterval(fetchData, refreshInterval)                  │
│  ↓ GET /api/summary/daily  or  /api/summary/5m           │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│ Backend (FastAPI)                                        │
│                                                          │
│  api/summary.py (Router)                                 │
│  ├── GET /api/summary/daily?industry_top_n=&concept_top_n=│
│  ├── GET /api/summary/5m?sectors=...&stocks=...          │
│  ├── GET /api/summary/trading-status                     │
│  └── GET /api/summary/watchlist                          │
│      PUT /api/summary/watchlist                          │
│                           │                              │
│                           ▼                              │
│  summary/service.py (MarketSnapshotService)              │
│  ├── get_daily_summary() → DailySummary                  │
│  └── get_5m_brief(sectors, stocks) → FiveMinBrief        │
│                           │                              │
│                           ▼                              │
│  summary/cache.py (TTL Cache)                            │
│  └── 缓存快照数据，TTL=60s（避免重复调用 akshare）         │
│                           │                              │
│                           ▼                              │
│  akshare (External API)                                  │
│  ├── stock_zh_index_spot_em()     — 指数快照               │
│  ├── stock_zh_a_spot_em()         — 全市场快照             │
│  ├── stock_board_industry_name_em() — 行业板块             │
│  └── stock_board_concept_name_em()  — 概念板块             │
└─────────────────────────────────────────────────────────┘
```

### Interfaces

#### Backend API

```
GET /api/summary/daily?industry_top_n=10&industry_bottom_n=10&concept_top_n=15&concept_bottom_n=15
  → DailySummaryResponse

GET /api/summary/5m?sectors=AI应用,算力&stocks=600673,300418
  → FiveMinBriefResponse

GET /api/summary/trading-status
  → TradingStatusResponse

GET /api/summary/watchlist
  → WatchlistConfig

PUT /api/summary/watchlist
  Body: WatchlistConfig
  → WatchlistConfig
```

#### Pydantic Models (`summary/models.py`)

```python
class IndexSnapshot(BaseModel):
    code: str
    name: str
    close: float
    change_pct: float
    change_val: float
    volume: float
    turnover: float

class MarketBreadth(BaseModel):
    total: int
    up: int
    down: int
    flat: int
    limit_up: int
    limit_up_20: int
    limit_down: int
    limit_down_20: int

class SectorRecord(BaseModel):
    code: str
    name: str
    change_pct: float
    up_count: int
    down_count: int
    leader: str

class StockRecord(BaseModel):
    code: str
    name: str
    change_pct: float

class DailySummaryResponse(BaseModel):
    date: str
    timestamp: str
    indices: list[IndexSnapshot]
    breadth: MarketBreadth
    industry_top: list[SectorRecord]
    industry_bottom: list[SectorRecord]
    concept_top: list[SectorRecord]
    concept_bottom: list[SectorRecord]
    stocks_top: list[StockRecord]
    stocks_bottom: list[StockRecord]

class RegimeInfo(BaseModel):
    label: str           # risk_on / neutral / risk_off
    score: float
    drivers: dict

class WatchSectorRecord(BaseModel):
    name: str
    matched_name: str
    change_pct: float
    up_count: int
    down_count: int
    strength: float
    status: str          # strong / weak / neutral / missing

class WatchStockRecord(BaseModel):
    code: str
    name: str
    price: float
    change_pct: float
    change_val: float
    turnover_rate: float
    volume_ratio: float
    status: str          # breakout / breakdown / active / watch / missing

class FiveMinBriefResponse(BaseModel):
    date: str
    timestamp: str
    regime: RegimeInfo
    sector_watchlist: list[WatchSectorRecord]
    stock_watchlist: list[WatchStockRecord]
    alerts: list[str]

class WatchlistConfig(BaseModel):
    watch_sectors: list[str]
    watch_stocks: list[dict]   # [{code, name}]

class TradingStatusResponse(BaseModel):
    is_trading: bool         # 当前是否在交易时段
    status: str              # "trading" / "pre_market" / "lunch_break" / "closed"
    next_open: str | None    # 下次开盘时间 (ISO format)，已开盘时为 None
    message: str             # 人类可读状态描述
```

### Data Flow

1. **前端发起请求** → GET /api/summary/daily 或 /api/summary/5m
2. **Router 层** → 调用 `MarketSnapshotService`
3. **Service 层** → 检查 TTL 缓存
   - 命中：直接返回缓存数据
   - 未命中：并发调用 akshare API（ThreadPoolExecutor），组装响应，写入缓存
4. **响应返回** → Pydantic model 序列化为 JSON
5. **前端接收** → 更新 React state，渲染页面
6. **自动刷新** → `useInterval` hook 按间隔重复步骤 1

### 缓存策略

```python
class SnapshotCache:
    """简单的 TTL 内存缓存。"""

    def __init__(self, ttl_seconds: int = 60):
        self._ttl = ttl_seconds
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.time() - ts > self._ttl:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.time(), value)
```

- **daily** 缓存 key: `"daily:{date}"`，TTL 60 秒
- **5m** 缓存 key: `"5m:{date}:{minute_bucket}"`，TTL 60 秒
- 同一分钟内多次请求复用缓存，避免 akshare 限频

### 前端自动刷新

```typescript
// useInterval hook
function useInterval(callback: () => void, delay: number | null) {
  const savedCallback = useRef(callback);
  useEffect(() => { savedCallback.current = callback; });
  useEffect(() => {
    if (delay === null) return;
    const id = setInterval(() => savedCallback.current(), delay);
    return () => clearInterval(id);
  }, [delay]);
}

// 交易状态感知的刷新逻辑
const [tradingStatus, setTradingStatus] = useState<TradingStatus | null>(null);
const [autoRefresh, setAutoRefresh] = useState(true);

// 根据交易状态计算刷新间隔
const refreshDelay = useMemo(() => {
  if (!autoRefresh || !tradingStatus) return null;
  if (tradingStatus.is_trading) return 5 * 60 * 1000;  // 5 min
  return null;  // 非交易时段停止刷新
}, [autoRefresh, tradingStatus]);

// 定时检查交易状态（每分钟）
useInterval(fetchTradingStatus, 60 * 1000);
// 根据交易状态刷新数据
useInterval(fetchData, refreshDelay);
```

### Watchlist 持久化

Watchlist 配置存储在本地 JSON 文件（`data/watchlist.json`），通过 API 读写：

- `GET /api/summary/watchlist` — 读取当前 watchlist
- `PUT /api/summary/watchlist` — 更新 watchlist
- 前端提供编辑界面（Modal 表单）

不使用 DuckDB 存储，因为 watchlist 是简单的用户偏好配置。

## Design Decisions

### Decision 1: 独立 Service 而非扩展 DataProvider

**Date**: 2026-03-09
**Status**: Proposed

**Context**: 现有 `DataProvider` ABC 定义了 11 个面向历史 K 线的接口（`get_stock_daily`, `get_index_daily` 等）。市场概览需要的是实时快照类 API（`stock_zh_index_spot_em`, `stock_zh_a_spot_em` 等），数据模式完全不同。

**Options considered**:
1. **扩展 DataProvider**: 添加 `get_index_snapshot()`, `get_market_breadth()` 等方法 — Pros: 统一接口 / Cons: 快照 API 和历史 API 模式差异大，MockProvider 也需同步扩展，接口膨胀
2. **独立 MarketSnapshotService**: 新建 `summary/service.py`，直接调用 akshare — Pros: 职责清晰，不影响现有接口，迁移方便 / Cons: akshare 调用逻辑与 AKShareProvider 有少量重复

**Decision**: Option 2 — 独立 Service。快照类数据的生命周期、缓存策略、调用模式都与历史 K 线不同，强行合并会导致接口混乱。

**Consequences**: `summary/service.py` 直接 `import akshare`，与 `data/akshare_provider.py` 并行存在。

### Decision 2: 前端轮询而非 WebSocket/SSE

**Date**: 2026-03-09
**Status**: Proposed

**Context**: 需要实现"自动刷新"功能。

**Options considered**:
1. **WebSocket**: 服务端推送 — Pros: 实时性最佳 / Cons: 实现复杂，akshare 本身无推送能力，后端仍需轮询 akshare
2. **SSE (Server-Sent Events)**: 单向推送 — Pros: 比 WS 简单 / Cons: 同上，多余复杂度
3. **前端轮询 + 后端缓存**: 前端 setInterval 定时 GET — Pros: 实现简单，与 5 分钟刷新频率匹配，后端缓存避免重复 API 调用 / Cons: 非真实时（但数据源本身就不是真实时）

**Decision**: Option 3 — 轮询。数据源频率限制了实时性上限，轮询是最简方案。

**Consequences**: 前端需 `useInterval` hook + 刷新开关 UI。后端 60 秒 TTL 缓存抵消多客户端并发。

### Decision 3: Watchlist 文件存储而非 DuckDB

**Date**: 2026-03-09
**Status**: Proposed

**Context**: 用户的关注板块/个股列表需要持久化。

**Options considered**:
1. **DuckDB 表**: 与其他数据一致 — Pros: 统一存储 / Cons: 简单配置用数据库过重
2. **JSON 文件**: `data/watchlist.json` — Pros: 简单，可直接编辑，版本控制友好 / Cons: 不支持多用户（当前是单用户系统，不是问题）

**Decision**: Option 2 — JSON 文件。单用户系统中 JSON 文件是最简方案。

**Consequences**: 新增 `data/watchlist.json` 文件，API 提供 CRUD。

### Decision 4: 交易时段检测 + 智能刷新

**Date**: 2026-03-10
**Status**: Decided

**Context**: 用户要求非交易时段能智能处理刷新行为。

**Design**:
- 后端提供 `GET /api/summary/trading-status` 返回当前交易状态
- 交易时段定义：工作日 9:15-11:30, 13:00-15:15（含集合竞价）
- 前端根据交易状态调整行为：
  - **交易中**: 5m 模式自动刷新，默认间隔 5 分钟
  - **非交易时段**: 停止自动刷新，显示"已收盘"状态，展示最近数据
  - **盘前/盘后**: 降低刷新频率（30 分钟）或停止
- 前端在页面头部显示交易状态指示器（绿点=交易中，灰点=已收盘）

**Consequences**: 新增 `TradingStatus` 模型和对应 API 端点。前端 `useInterval` 的 delay 根据交易状态动态调整。

### Decision 5: 行业/概念显示数量可配置

**Date**: 2026-03-10
**Status**: Decided

**Context**: 用户要求行业/概念板块的显示数量可配置。

**Design**:
- API 端点接受 query params：`industry_top_n`, `industry_bottom_n`, `concept_top_n`, `concept_bottom_n`
- 默认值：行业 top/bottom 各 10，概念 top/bottom 各 15（与原脚本一致）
- 前端提供设置控件（数字输入或下拉选择）

## Phases

### Phase 1: 后端服务层
- [ ] 创建 `tradepilot/summary/` 包（`__init__.py`, `models.py`, `service.py`, `cache.py`）
- [ ] 从 `fetch_a_share.py` 迁移数据获取逻辑到 `service.py`
- [ ] 实现 Pydantic v2 响应模型
- [ ] 实现 TTL 缓存层

### Phase 2: 后端 API 路由
- [ ] 创建 `tradepilot/api/summary.py` router
- [ ] 实现 `GET /api/summary/daily` 端点
- [ ] 实现 `GET /api/summary/5m` 端点
- [ ] 实现 `GET/PUT /api/summary/watchlist` 端点
- [ ] 挂载到 `main.py`

### Phase 3: 前端页面
- [ ] 创建 `webapp/src/pages/MarketSummary/index.tsx`
- [ ] 实现 Daily 视图（指数卡片 + 情绪指标 + 板块排名 + 个股 TOP10）
- [ ] 实现 5m 视图（Regime 仪表 + Watchlist 表格 + 告警列表）
- [ ] 实现模式切换 + 自动刷新控制
- [ ] 更新 `api.ts` 和 `App.tsx`

### Phase 4: Watchlist 管理
- [ ] 前端 Watchlist 编辑 Modal
- [ ] 后端 watchlist JSON 文件读写
- [ ] 默认 watchlist 配置

## Open Questions

- [x] 是否需要交易时段检测（非交易时段停止自动刷新或降低频率）？ → **需要**，见 Decision 4
- [x] daily 模式的行业/概念显示数量是否需要可配置（当前脚本默认 10/15）？ → **需要**，通过 API query params 配置
- [x] 新页面放在侧边栏的哪个位置（第一个？在 Dashboard 之后）？ → **侧边栏第一位**

## Session Log

- **2026-03-09**: 初始设计会话 — 分析了现有 `a-share-summary` 脚本结构（~1000 行，包含 fetch/parse/format 三层），梳理了 TradePilot 后端架构（Provider 模式 + 6 组 API router）和前端架构（5 页面 + Ant Design + Vite），确定了设计方向：独立 Service 层 + 前端轮询 + JSON 文件存储 watchlist。创建了设计文档初稿。
- **2026-03-10**: 解答 Open Questions — 确认需要交易时段检测（Decision 4），行业/概念显示数量可配置（Decision 5），新页面放侧边栏第一位。更新了 API 接口（新增 `trading-status` 端点、`daily` 端点增加 `top_n` query params）和前端刷新逻辑（交易状态感知）。
