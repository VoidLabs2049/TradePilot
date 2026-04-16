# ETF All-Weather Data Source Map

## The Short Answer

如果只是为了把 ETF 全天候策略做成一个 `月度-季度` 的可研究、可回测、可逐步上线的系统，真正需要先拿稳的不是“所有数据”，而是下面这几层：

1. ETF / index 日频历史
2. 国内月度宏观
3. 利率 / 货币 / 曲线
4. 黄金与商品 proxy
5. 海外宏观 overlay

期权情绪、复杂信用利差、多交易所期货归一化，先不要当成 v1 必需品。

---

## Category-by-Category

| Data Category | Primary Source | Validation / Fallback | v1? | Main Caveat |
|---|---|---|---|---|
| ETF/fund daily history | Tushare | AKShare / Eastmoney | Yes | token-gated or wrapper drift |
| ETF metadata / scale / shares | Tushare | SSE/SZSE / Eastmoney / 天天基金 | Yes | fields may need cross-check |
| Index history | Tushare | CSIndex / SSE index pages | Yes | official pages better for methodology than bulk history |
| China macro monthly | NBS + PBOC | Tushare / AKShare wrappers | Yes | release-date alignment is mandatory |
| Rates / yield curve / interbank | Chinamoney + ChinaBond + Shibor | Tushare wrappers | Yes | sites are awkward even when authoritative |
| Gold / commodity proxies | ETF prices + SHFE/INE futures | Eastmoney futures pages | Yes | continuous-contract logic may be needed later |
| Overseas overlay | FRED | Eastmoney global pages | Yes | not China-native; overlay only |
| Futures/options sentiment | CFFEX / SHFE official | Eastmoney pages | No, later | normalization complexity |
| Credit spread proxies | ChinaBond / Chinamoney | global comparison via FRED only | No, later | domestic spread system takes work |

---

## Recommended v1 Stack

### 1. Core Panel Backbone

- `Tushare`

Use for:
- ETF daily bars
- index history
- trade calendar
- part of macro panel
- possible fund metadata

Why:
- best single Python-friendly normalized source
- easiest way to assemble a cross-asset research panel quickly

Risk:
- token/points gating
- should not become the only truth source

### 2. Official Slow-Data Anchors

- `NBS`
- `PBOC`
- `Chinamoney`
- `ChinaBond`
- `Shibor`

Use for:
- PMI / CPI / PPI / GDP / industrial / retail / FAI
- M1 / M2 / TSF / LPR
- interbank rates
- government curve and bond-market context

Why:
- these are the real anchors for a monthly macro allocation system

Risk:
- extraction is less convenient than wrapper APIs

### 3. Wrapper / Fallback Layer

- `AKShare`

Use for:
- pulling public Chinese data quickly
- filling gaps when direct official scraping is inconvenient
- fallback for ETF/spot/macro pages

Risk:
- upstream page changes can silently break endpoints

### 4. ETF Validation Layer

- `SSE / SZSE`
- `Eastmoney / 天天基金`

Use for:
- ETF universe discovery
- listing and notice validation
- fund scale and metadata cross-checks

Risk:
- public endpoints may be undocumented

### 5. Global Overlay Layer

- `FRED`

Use for:
- US yields
- global risk-off signals
- USD liquidity and global macro overlay

Why:
- stable API, clean metadata, ideal for v1 external overlay

---

## What We Can Realistically Obtain Now

### Definitely realistic now

- A股/ETF/指数日频价格
- 交易日历
- ETF proxy quotes
- PMI / CPI / PPI / GDP / M1 / M2 / 社融
- 国债/货币市场利率与部分曲线数据
- 黄金和部分商品 proxy
- 海外利率和美元相关 overlay

### Obtainable but should be treated carefully

- ETF AUM / 份额 / 规模
- 信用利差 proxy
- 期权 PCR / OI / IV
- 期货仓位与跨交易所持仓结构

These are not impossible, but they are more likely to cause false precision early.

---

## Practical Brainstorming Takeaway

If our goal is just to get the strategy into `honest research mode`, then the data question is already sufficiently answered:

- we do **not** lack data to start
- we mainly lack a **field-level source schema** and **release-date alignment discipline**

The real v1 bottleneck is not “where to get everything”, but:

1. picking the smallest sufficient source stack
2. defining one canonical source per field
3. documenting fallback sources
4. enforcing publication-date correctness

---

## Bottom Line

For this strategy, the best v1 answer is:

- `Tushare` for the core panel
- `NBS/PBOC/Chinamoney/ChinaBond/Shibor` for official macro and rates anchors
- `AKShare` as wrapper and fallback
- `FRED` for global overlay
- `SSE/SZSE/Eastmoney` for ETF validation and metadata completion

That stack is already enough to support a serious first research cycle.
