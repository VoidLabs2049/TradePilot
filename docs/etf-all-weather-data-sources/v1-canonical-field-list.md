# V1 Canonical Field List

## Purpose

This document freezes the actual v1 field boundary for the ETF all-weather strategy.

It answers four questions:

1. which sleeves are in v1
2. which fields are allowed into the model
3. which fields are validation-only
4. which fields are explicitly deferred

This is the boundary document that should drive schema design.

---

## V1 Sleeve Set

The v1 system uses exactly these `5` sleeves:

| Sleeve Role | Canonical Instrument | Why Selected |
|---|---|---|
| large-cap equity | `510300.SH` | stable broad beta proxy |
| small-cap equity | `159845.SZ` | clean small-cap / higher-beta proxy |
| bond defense | `511010.SH` | cleanest defensive duration proxy in current tests |
| gold hedge | `518850.SH` | interpretable gold hedge sleeve |
| cash / neutral buffer | `159001.SZ` | cleanest low-volatility parking sleeve |

Backup instruments may exist, but they are not part of the canonical v1 definition.

---

## Field Role Labels

Each field below is assigned one of these roles:

- `primary`
  - allowed to enter v1 state or allocation logic directly
- `confirmatory`
  - may throttle or validate a primary view, but should not define the regime alone
- `execution_only`
  - may affect scaling or implementation, not macro state classification
- `validation_only`
  - used for auditing, sanity checks, or universe control
- `defer`
  - explicitly not part of v1

---

## Section A — Canonical Sleeve Identity Fields

These fields define the traded objects and must exist in the schema, but they are not model signals.

| Field | Role | Source Priority | Notes |
|---|---|---|---|
| `sleeve_code` | `validation_only` | static canonical config | must match one of the 5 chosen instruments |
| `sleeve_name` | `validation_only` | Tushare -> exchange validation | human-readable mapping |
| `sleeve_role` | `validation_only` | static canonical config | one of equity_large / equity_small / bond / gold / cash |
| `benchmark_name` | `validation_only` | Tushare -> prospectus validation | benchmark purity note |
| `listing_exchange` | `validation_only` | exchange / Tushare | venue metadata |
| `list_date` | `validation_only` | Tushare | backtest eligibility boundary |
| `exposure_note` | `validation_only` | manual maintained note | especially important for bond sleeve |

---

## Section B — Canonical Sleeve Price Fields

These are mandatory.

| Field | Role | Source Priority | Timing Rule |
|---|---|---|---|
| `trade_date` | `primary` | Tushare trade calendar | canonical trading key |
| `equity_large_close` | `primary` | Tushare fund_daily | end-of-day, next-period use |
| `equity_small_close` | `primary` | Tushare fund_daily | end-of-day, next-period use |
| `bond_close` | `primary` | Tushare fund_daily | end-of-day, next-period use |
| `gold_close` | `primary` | Tushare fund_daily | end-of-day, next-period use |
| `cash_close` | `primary` | Tushare fund_daily | end-of-day, next-period use |

### Required Supporting Market Fields

| Field | Role | Source Priority | Notes |
|---|---|---|---|
| `equity_large_vol` | `validation_only` | Tushare | tradability audit |
| `equity_large_amount` | `validation_only` | Tushare | tradability audit |
| `equity_small_vol` | `validation_only` | Tushare | tradability audit |
| `equity_small_amount` | `validation_only` | Tushare | tradability audit |
| `bond_vol` | `validation_only` | Tushare | bond sleeve liquidity warning field |
| `bond_amount` | `validation_only` | Tushare | bond sleeve liquidity warning field |
| `gold_vol` | `validation_only` | Tushare | tradability audit |
| `cash_vol` | `validation_only` | Tushare | tradability audit |

Unit convention:

- ETF daily `volume` / source `vol` uses Tushare `fund_daily` units: hands.
- ETF daily `amount` uses Tushare `fund_daily` units: thousand CNY.
- `derived.etf_aw_sleeve_daily` inherits these units without conversion.
- Any future design that changes these fields to shares or CNY must use a new explicit field name or document the conversion at the transformation boundary.

---

## Section C — Canonical Macro Slow Fields

These are the slow structural inputs.

| Field | Role | Source Priority | Effective Rule |
|---|---|---|---|
| `official_pmi` | `primary` | Tushare / official mapping | after conservative PMI effective date |
| `official_pmi_mom` | `primary` | derived from effective PMI values | only after both months are effective |
| `ppi_yoy` | `primary` | Tushare / official mapping | after conservative CPI/PPI effective date |
| `m1_yoy` | `primary` | Tushare -> AKShare fallback | after conservative money/credit effective date |
| `m2_yoy` | `primary` | Tushare -> AKShare fallback | same |
| `m1_m2_spread` | `primary` | derived | only after both M1 and M2 are effective |
| `tsf_yoy` or `credit_impulse_proxy` | `primary` | Tushare / official mapping | after conservative money/credit effective date |

### Macro Confirmation Fields

| Field | Role | Source Priority | Notes |
|---|---|---|---|
| `cpi_yoy` | `confirmatory` | Tushare / official mapping | do not let headline CPI define regime alone |
| `core_cpi_yoy` | `confirmatory` | official / manual / wrapper | optional if stable extraction exists |
| `industrial_production_yoy` | `confirmatory` | Tushare / official mapping | growth confirmation |
| `retail_sales_yoy` | `confirmatory` | Tushare / official mapping | domestic demand confirmation |
| `fixed_asset_investment_ytd` | `confirmatory` | Tushare / official mapping | keep as reported |
| `exports_yoy` | `confirmatory` | Tushare / wrapper / official mapping | external-demand confirmation |
| `new_loans_total` | `confirmatory` | Tushare / official mapping | credit detail, not root driver |

### Macro Constraints

- All slow fields must carry:
  - `period_label`
  - `release_date`
  - `effective_date`
  - `revision_note`
- `m1_yoy`, `m2_yoy`, and `m1_m2_spread` must additionally carry:
  - `regime_note`
  - `definition_regime`

---

## Section D — Canonical Rates And Liquidity Fields

These explain the bond sleeve and broad domestic liquidity background.

| Field | Role | Source Priority | Timing Rule |
|---|---|---|---|
| `shibor_1w` | `primary` | Tushare -> AKShare fallback | latest published quote up to decision date |
| `lpr_1y` | `primary` | Tushare -> AKShare fallback | source date or conservative 20th rule |
| `lpr_5y` | `confirmatory` | Tushare -> AKShare fallback | same |
| `cn_gov_10y_yield` | `primary` | Tushare yc_cb / curve extractor | end-of-day / next-day executable logic |
| `cn_gov_1y_yield` | `confirmatory` | Tushare yc_cb / curve extractor | same |
| `cn_yield_curve_slope_10y_1y` | `confirmatory` | derived | only after both endpoints extracted cleanly |

### Rates Constraints

- curve fields require a windowed/paged extraction method before being trusted historically
- until that exists, curve fields are `primary-designated but operationally caveated`

---

## Section E — Canonical Market Confirmation Fields

These are faster than macro and should confirm, not dominate.

| Field | Role | Source Priority | Construction Rule |
|---|---|---|---|
| `hs300_close` | `validation_only` | Tushare index_daily | canonical benchmark input |
| `zz1000_close` | `validation_only` | Tushare index_daily | canonical benchmark input |
| `hs300_vs_zz1000_20d` | `confirmatory` | derived from index closes | prior-close only |
| `bond_trend_20d` | `confirmatory` | derived from `bond_close` | prior-close only |
| `gold_trend_20d` | `confirmatory` | derived from `gold_close` | prior-close only |

### Rule

- None of these fields may define the macro state alone.
- Their role is to confirm or throttle budget tilt.

---

## Section F — Canonical Execution Fields

These influence sizing and implementation, not macro classification.

| Field | Role | Source Priority | Construction Rule |
|---|---|---|---|
| `realized_vol_20d_equity_large` | `execution_only` | derived | prior-close only |
| `realized_vol_20d_equity_small` | `execution_only` | derived | prior-close only |
| `realized_vol_20d_bond` | `execution_only` | derived | prior-close only |
| `realized_vol_20d_gold` | `execution_only` | derived | prior-close only |
| `realized_vol_20d_cash` | `execution_only` | derived | prior-close only |

### Optional But Not Canonical In First Pass

| Field | Role |
|---|---|
| `atr_20` | `execution_only`, optional |
| `ma_20 / ma_30 / ma_60` | optional |
| `adx_14` | `defer` unless later justified |
| `rsi_14` | `defer` unless later justified |

---

## Section G — Validation-Only Fields

These should exist if available, but must not silently become model inputs.

| Field | Why Keep It |
|---|---|
| `fund_share` / `shares_outstanding` | capacity and scale awareness |
| `aum` / `fund_scale` | avoid tiny or fragile products |
| `management_fee` / `custody_fee` | live implementation realism |
| `market_breadth_proxy` | exploratory validation only |
| `gold_volume` / `bond_volume` / `cash_volume` | tradability audit |

---

## Section H — Explicitly Deferred Fields

These are out of v1 by design.

| Field Group | Status | Why Deferred |
|---|---|---|
| ETF options `PCR / OI / IV / skew` | `defer` | too much complexity for current edge |
| cross-exchange futures positioning | `defer` | normalization burden too high |
| full domestic credit spread system | `defer` | useful later, not needed for first notebook |
| CTA / neutral fund proxy layer | `defer` | instrument mapping not clean enough |
| commodity sleeve fields | `defer` | not part of the first 5-sleeve definition |
| overseas overlay fields | `defer` for core v1 | keep outside root system until domestic core is stable |
| ML classifier inputs beyond the rule stack | `defer` | not earned yet |

---

## Minimal V1 Model Input Set

If we compress v1 to the strict minimum inputs that are allowed to touch state or allocation logic, the canonical set is:

### Sleeve Prices

- `equity_large_close`
- `equity_small_close`
- `bond_close`
- `gold_close`
- `cash_close`

### Macro Primary

- `official_pmi`
- `official_pmi_mom`
- `ppi_yoy`
- `m1_yoy`
- `m2_yoy`
- `m1_m2_spread`
- `tsf_yoy` or `credit_impulse_proxy`

### Rates Primary

- `shibor_1w`
- `lpr_1y`
- `cn_gov_10y_yield`

### Market Confirmation

- `hs300_vs_zz1000_20d`
- `bond_trend_20d`
- `gold_trend_20d`

### Execution

- realized volatility family for each sleeve

Everything else should justify itself before promotion.

---

## Final Judgment

The v1 boundary is now explicit.

This matters because a strategy usually fails long before live trading, at the moment when “maybe useful” fields quietly become permanent scope.

This file is the defense against that drift.
