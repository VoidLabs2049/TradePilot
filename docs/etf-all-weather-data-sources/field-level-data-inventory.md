# ETF All-Weather Field-Level Data Inventory

## Purpose

This document answers a narrower question than the source map:

> For a v1 China ETF all-weather strategy, what exact fields do we need, where should each field come from, what is the fallback, and what timing constraint matters?

It is intentionally scoped to the `small, transparent, monthly` version of the strategy.

---

## Reading Rule

For each field, the important items are:

- `Primary source`
- `Fallback`
- `Cadence`
- `Timing rule`
- `Why we need it`

If a field has unclear timing semantics or unstable meaning, it should not become a hard v1 dependency.

---

## Layer 0 — Trading Calendar And Decision Dates

These fields are not alpha signals, but they govern whether the entire panel is honest.

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `trade_date` | Tushare `trade_cal` | local weekday fallback only as emergency | daily | always map rebalance date to real open trading day | backtest and live schedule anchor |
| `pretrade_date` | Tushare `trade_cal` | derive from open-day list | daily | use for prior-close alignment and lag handling | avoids holiday/weekend leakage |
| `rebalance_date_monthly` | derived from trade calendar | none | monthly | choose fixed rule such as month-end last open day or macro-release-safe date | makes decisions reproducible |

### Judgment

This layer is foundational. A strategy with imperfect signals but correct timing is still researchable; a strategy with future leakage in dates is not.

---

## Layer 1 — Asset Sleeve Price Fields

These fields support sleeve returns, realized volatility, trend, drawdown, and rebalancing.

### 1.1 Equity Sleeves

Recommended v1 sleeves:
- large-cap equity ETF
- small-cap equity ETF

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `equity_large_close` | Tushare ETF/fund daily | AKShare / Eastmoney | daily | use close known at trade-day end; next-period return starts after decision cutoff | core risk-on sleeve |
| `equity_small_close` | Tushare ETF/fund daily | AKShare / Eastmoney | daily | same as above | captures style/cycle beta |
| `equity_large_volume` | Tushare | AKShare | daily | informational, not required for first score layer | liquidity sanity check |
| `equity_small_volume` | Tushare | AKShare | daily | informational | liquidity sanity check |
| `equity_large_amount` | Tushare | AKShare | daily | same-day only | turnover/liquidity check |
| `equity_small_amount` | Tushare | AKShare | daily | same-day only | turnover/liquidity check |

### 1.2 Bond Sleeve

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `bond_close` | Tushare fund daily | AKShare / Eastmoney | daily | same close convention as equities | duration defense sleeve |
| `bond_volume` | Tushare | AKShare | daily | informational | tradability check |
| `bond_amount` | Tushare | AKShare | daily | informational | tradability check |

### 1.3 Gold Sleeve

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `gold_close` | Tushare fund daily | AKShare / Eastmoney | daily | same close convention | stress hedge / real-rate proxy |
| `gold_volume` | Tushare | AKShare | daily | informational | liquidity sanity check |

### 1.4 Cash / Short-Duration Sleeve

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `cash_proxy_return` | short-bond fund daily or explicit cash assumption | constant risk-free proxy | daily or monthly | if no tradable cash sleeve, document synthetic assumption | neutral buffer / dry powder |

### Judgment

For v1, price series matter more than perfect metadata. The first objective is to get clean sleeve return histories with stable product mapping.

---

## Layer 2 — Instrument Metadata And Validation Fields

These fields are not required for first backtest math, but they matter for universe hygiene and later deployment.

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `fund_code` | Tushare | SSE/SZSE | static | verify once, refresh on instrument changes | canonical identifier |
| `fund_name` | Tushare | SSE/SZSE / Eastmoney | static | verify once | human-readable mapping |
| `listing_exchange` | SSE/SZSE official | Tushare | static | verify once | venue and trading rules |
| `benchmark_index` | fund prospectus / CSIndex / exchange | Eastmoney | static | verify once | benchmark exposure truth |
| `fund_share` / `shares_outstanding` | Tushare | Eastmoney / 天天基金 | daily or periodic | do not assume exactness without cross-check | capacity and scale awareness |
| `aum` / `fund_scale` | fund site / Eastmoney / Tushare if available | exchange pages | periodic | cross-check because definitions and dates may differ | avoid tiny/fragile products |
| `management_fee` / `custody_fee` | prospectus / fund page | Eastmoney | static/periodic | verify on updates | live cost realism |

### Judgment

`AUM / 份额 / 规模` is useful, but not a reason to delay v1. It should be a validation layer, not the bottleneck.

---

## Layer 3 — Domestic Macro Slow Variables

These are the structural state inputs. Release-date alignment is mandatory.

### 3.1 Growth And Activity

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `official_pmi` | NBS | Tushare / AKShare | monthly | only usable after official release date | core growth signal |
| `official_pmi_mom` | derived | none | monthly | derive from released values only | direction often matters more than level |
| `caixin_pmi` | official / wrapper | AKShare | monthly | use real release date | private/smaller-firm cross-check |
| `industrial_production_yoy` | NBS | Tushare / AKShare | monthly | use release date lag | growth confirmation |
| `retail_sales_yoy` | NBS | Tushare / AKShare | monthly | use release date lag | domestic demand signal |
| `fixed_asset_investment_ytd` | NBS | Tushare / AKShare | monthly | use as reported, no faux monthly reconstruction unless documented | investment cycle confirmation |
| `exports_yoy` | customs / wrapper | AKShare | monthly | use release date lag | external-demand check |

### 3.2 Inflation And Pricing Pressure

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `cpi_yoy` | NBS | Tushare / AKShare | monthly | use release date | inflation backdrop |
| `core_cpi_yoy` | NBS if available | manual official parsing / wrapper | monthly | use release date | cleaner domestic demand signal |
| `ppi_yoy` | NBS | Tushare / AKShare | monthly | use release date | pricing / industrial cycle signal |
| `ppi_mom` | NBS or derived | wrapper | monthly | derive carefully from available official form | directional pricing pressure |

### 3.3 Money And Credit

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `m1_yoy` | PBOC | Tushare / AKShare | monthly | use release date; annotate post-2025 definition regime | liquidity / activity proxy |
| `m2_yoy` | PBOC | Tushare / AKShare | monthly | use release date | broad monetary condition |
| `m1_m2_spread` | derived | none | monthly | compare only within compatible definition regime | activity-vs-hoarding proxy |
| `tsf_yoy` or `credit_impulse_proxy` | PBOC | Tushare / AKShare | monthly | use release date and documented construction | growth x credit axis |
| `new_loans_total` | PBOC | Tushare / AKShare | monthly | use release date | credit pulse detail |
| `new_loans_structure` | PBOC reports if parsed | manual later | monthly | later-stage if structured extraction is reliable | quality of credit expansion |

### Judgment

These are the highest-value slow fields. For v1, `PMI + PPI + M1/M2 + TSF` is enough; the rest are confirmation or robustness fields.

---

## Layer 4 — Rates, Liquidity, And Curve Fields

This layer helps explain the bond sleeve and broad liquidity regime.

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `shibor_overnight` | Shibor official | Tushare / AKShare | daily | use published daily rate | short funding condition |
| `shibor_1w` | Shibor official | Tushare / AKShare | daily | same | short liquidity condition |
| `dr007` or repo benchmark | Chinamoney / PBOC-related source | wrapper | daily | same-day published market rate | policy/liquidity transmission |
| `lpr_1y` | PBOC / national interbank quoting center | Tushare / AKShare | monthly | use release date | policy reference rate |
| `lpr_5y` | same | same | monthly | use release date | housing / duration sensitivity |
| `cn_gov_10y_yield` | ChinaBond / Chinamoney | Tushare if exposed | daily | same-day market close | domestic duration anchor |
| `cn_gov_1y_yield` | ChinaBond / Chinamoney | wrapper | daily | same | curve slope input |
| `cn_yield_curve_slope_10y_1y` | derived | none | daily | derive from same-day yields | bond regime / growth-pressure proxy |

### Judgment

This layer is more important than many “fancy” signals. If we cannot explain bond sleeve allocation using rates/liquidity context, the system is under-specified.

---

## Layer 5 — Market Confirmation Fields

These are faster than macro but slower and more structural than technical execution filters.

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `hs300_close` | Tushare index daily | AKShare | daily | close known end-of-day | large-cap benchmark |
| `zz1000_close` | Tushare index daily | AKShare | daily | same | small-cap benchmark |
| `hs300_vs_zz1000_20d` | derived | none | daily/monthly decision use | derive from prior available closes only | style / risk appetite confirmation |
| `bond_trend_20d` | derived from bond sleeve price | none | daily/monthly | derive from prior closes only | market pricing of duration |
| `gold_trend_20d` | derived from gold sleeve price | none | daily/monthly | derive from prior closes only | market pricing of stress / real rates |
| `market_breadth_proxy` | Tushare / AKShare if stable | omit if noisy | daily | use only if source is stable | optional confirmation |
| `style_label` | derived | none | weekly/monthly | derived from relative strength thresholds | interpretable state label |

### Judgment

For v1, this layer should stay sparse. `HS300 vs ZZ1000`, `bond trend`, and `gold trend` are already enough to tell us whether the market agrees with the macro read.

---

## Layer 6 — Technical Execution Filters

These are not state-defining in v1. They are execution throttles.

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `ma_20` / `ma_30` | derived from sleeve prices | none | daily | use prior closes only | trend filter |
| `ma_60` | derived | none | daily | same | medium-term trend confirmation |
| `atr_20` | derived from high/low/close if available | simplified realized vol if not | daily | same | position scaling |
| `realized_vol_20d` | derived | none | daily | same | risk scaling fallback |
| `adx_14` | derived if OHLC data is clean | omit for v1 if unnecessary | daily | same | optional trend-strength filter |
| `rsi_14` | derived | omit if overcomplicating | daily | same | optional entry heat filter |

### Judgment

`ATR / realized vol` matters more than `RSI / ADX` for v1. Scaling risk honestly is more important than adding signal decoration.

---

## Layer 7 — Overseas Overlay Fields

These are not mandatory for the first domestic-only pass, but they are realistic and valuable.

| Field | Primary Source | Fallback | Cadence | Timing Rule | Why We Need It |
|---|---|---|---|---|---|
| `us10y_yield` | FRED | other public market sources | daily | same-day published series | global discount-rate pressure |
| `us2y_yield` | FRED | same | daily | same | policy-sensitive rate |
| `us_curve_slope_10y_2y` | derived | none | daily | derive from same-date data | recession / policy regime signal |
| `dxy_proxy` | FRED-compatible series or public market source | later custom source | daily | same | dollar pressure overlay |
| `gold_global_reference` | FRED or market source | later custom source | daily | same | cross-check gold sleeve regime |
| `oil_proxy` | FRED / public commodity source | later futures source | daily | same | global inflation / supply shock overlay |

### Judgment

This layer should remain an overlay, not the domestic core. Useful, but not permission to turn the system into a giant macro machine.

---

## Layer 8 — Better Delayed Fields

These are obtainable but should not block v1.

| Field Group | Why Delay |
|---|---|
| ETF option `PCR / OI / IV / skew` | data engineering and interpretation burden is high relative to v1 value |
| cross-exchange futures positioning | normalization and continuous-series logic are complex |
| domestic credit spread system | useful, but difficult to make robust quickly |
| CTA / neutral fund exposure proxies | instrument mapping is messy and may not be stable |
| full commodity sleeve state model | futures and ETF expression may diverge materially |

---

## Minimal V1 Field Set

If we strip everything down to the minimum serious version, the field set is approximately:

### Sleeve prices
- large-cap ETF close
- small-cap ETF close
- bond sleeve close
- gold sleeve close
- cash/short-bond proxy return

### Macro
- official PMI
- PPI yoy
- M1 yoy
- M2 yoy
- TSF yoy or credit-pulse proxy

### Rates / liquidity
- Shibor 1w
- LPR 1y or 5y
- China 10Y gov yield
- China curve slope

### Market confirmation
- HS300 vs ZZ1000 relative strength
- bond trend
- gold trend

### Execution
- realized vol or ATR

This is already enough to support a serious first notebook.

---

## Final Judgment

The field-level answer is now clear:

- we do not need more categories before starting
- we need disciplined source assignment and timing rules for the categories we already chose

The most important fields are not the most exotic ones. They are the ones that let us build a clean monthly decision table without future leakage.
