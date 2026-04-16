# Release-Date Rules For V1 Slow Fields

## Purpose

Stage 01 testing confirmed that the core slow-data interfaces are fetchable, but most of them do **not** carry a canonical `release_date` field.

So before schema design or backtest logic, we need explicit release rules.

This document defines, for each v1 slow field:

- `release_date_rule`
- `effective_date_rule`
- `revision_note`
- `fallback_timing_assumption`

These are implementation constraints, not narrative comments.

---

## Core Principle

If a field does not provide a trustworthy native `release_date`, the strategy must use a conservative external release rule.

Conservative means:

- prefer “usable slightly later than reality” over “usable too early”
- when in doubt, shift the field to the next safe trading day after the public release window

The cost of slight delay is lower than the cost of hidden future leakage.

---

## Global Conventions

### Convention 1 — Decision Clock

For monthly v1 allocation, the model should assume decisions are made on a designated `rebalance_date_monthly`.

Any slow field may be used only if its `effective_date <= rebalance_date_monthly`.

### Convention 2 — Safe Effective Date

If an indicator is released on a calendar day that may or may not be a trading day, the `effective_date` should be:

- the release day itself if the strategy assumes after-release processing and next-tradable execution, or
- the next open trading day under the repo's canonical trade calendar

For v1, use the conservative rule:

> `effective_date = next open trading day on or after the public release date`

### Convention 3 — Revision Discipline

If the source API exposes only the latest revised history and not first-release history, the backtest must carry a `revision_note` saying so.

V1 default:

- accept latest-history series for research prototyping
- explicitly mark them as `revision-risk-present`
- do not pretend they are perfect real-time vintage data

---

## Field Rules

## 1. Official PMI

### Fields

- `official_pmi`
- `official_pmi_mom`

### Release Rule

- Public window: `month end / day 1`
- Conservative `release_date_rule`:
  - assign release date as the `1st calendar day of the following month`

### Effective Rule

- `effective_date_rule`:
  - first open trading day on or after the assigned release date

### Revision Note

- `revision_note = latest_history_only_unless_vintage_captured`

### Fallback Timing Assumption

- If source lacks explicit release timestamp, do **not** allow use before the next open trading day after month end.

### Implementation Note

- `official_pmi_mom` must be derived only after both adjacent months are independently effective.

---

## 2. Caixin PMI

### Fields

- `caixin_pmi`

### Release Rule

- Public window: `day 1 to day 3 of the following month`
- Conservative `release_date_rule`:
  - assign release date as the `3rd calendar day of the following month`

### Effective Rule

- `effective_date_rule`:
  - first open trading day on or after the assigned release date

### Revision Note

- `revision_note = latest_history_only_unless_vintage_captured`

### Fallback Timing Assumption

- If the exact release day is unavailable in the source panel, always use the post-day-3 trading day.

---

## 3. Exports / Trade Data

### Fields

- `exports_yoy`

### Release Rule

- Public window: `day 7 to day 10`
- Conservative `release_date_rule`:
  - assign release date as the `10th calendar day of the following month`

### Effective Rule

- first open trading day on or after the assigned release date

### Revision Note

- `revision_note = latest_history_only_unless_vintage_captured`

---

## 4. CPI / Core CPI / PPI

### Fields

- `cpi_yoy`
- `core_cpi_yoy`
- `ppi_yoy`
- `ppi_mom`

### Release Rule

- Public window: `day 9 to day 12`
- Conservative `release_date_rule`:
  - assign release date as the `12th calendar day of the following month`

### Effective Rule

- first open trading day on or after the assigned release date

### Revision Note

- `revision_note = latest_history_only_unless_vintage_captured`

### Fallback Timing Assumption

- Headline CPI and PPI should not be backfilled to month-end.
- If `core_cpi_yoy` is missing in the structured API and obtained from official text/manual parsing, it inherits the same release rule.

---

## 5. Money Supply And Credit Pulse

### Fields

- `m1_yoy`
- `m2_yoy`
- `m1_m2_spread`
- `tsf_yoy`
- `credit_impulse_proxy`
- `new_loans_total`

### Release Rule

- Public window: `day 10 to day 15`
- Conservative `release_date_rule`:
  - assign release date as the `15th calendar day of the following month`

### Effective Rule

- first open trading day on or after the assigned release date

### Revision Note

- `revision_note = latest_history_only_unless_vintage_captured`
- `m1_semantic_regime_note = pre_2025_definition_vs_post_2025_definition`

### Fallback Timing Assumption

- `m1_m2_spread` becomes effective only when both `m1_yoy` and `m2_yoy` for the same month are effective.
- `credit_impulse_proxy` becomes effective only after its underlying TSF field is effective and the construction rule is documented.

### Additional Constraint

- Pre-2025 and post-2025 `m1`-family data must be tagged with a regime boundary before entering model logic.

---

## 6. GDP / Industrial Production / Retail Sales / FAI

### Fields

- `gdp_yoy` or quarterly GDP field
- `industrial_production_yoy`
- `retail_sales_yoy`
- `fixed_asset_investment_ytd`

### Release Rule

- Public window: `day 15 to day 17`
- Conservative `release_date_rule`:
  - assign release date as the `17th calendar day of the following month`
  - for GDP quarter releases, use the same conservative day rule in the release month

### Effective Rule

- first open trading day on or after the assigned release date

### Revision Note

- `revision_note = latest_history_only_unless_vintage_captured`

### Fallback Timing Assumption

- `fixed_asset_investment_ytd` must be stored as reported; no synthetic monthly conversion without separate documentation.

---

## 7. LPR

### Fields

- `lpr_1y`
- `lpr_5y`

### Release Rule

- Public day: around the `20th` of each month
- Conservative `release_date_rule`:
  - use the actual quoted `date` from the source when available
  - if unavailable, assign the `20th calendar day of the month`

### Effective Rule

- if source gives the exact announcement date, `effective_date = that date`
- if the date is not a trading day in the panel logic, use the next open trading day for strategy decisions

### Revision Note

- `revision_note = low_revision_risk_relative_to_other_slow_fields`

### Fallback Timing Assumption

- LPR is cleaner than most slow fields because the announcement date is naturally date-stamped.

---

## 8. Shibor

### Fields

- `shibor_overnight`
- `shibor_1w`

### Release Rule

- daily published market fixing
- `release_date_rule = same calendar date as quoted`

### Effective Rule

- for monthly allocation, same-day quote may be used as a state observation only if the decision policy assumes post-fix availability
- conservative default for v1 monthly system:
  - use the latest available published quote up to and including the decision date

### Revision Note

- `revision_note = low_revision_risk`

### Fallback Timing Assumption

- none beyond normal same-day market data discipline

---

## 9. China Government Yield Curve

### Fields

- `cn_gov_10y_yield`
- `cn_gov_1y_yield`
- `cn_yield_curve_slope_10y_1y`

### Release Rule

- daily market curve observation
- `release_date_rule = trade_date`

### Effective Rule

- usable at end-of-day for research and next-period decisions
- if the system assumes execution after close is not realistic, use next trading day as the first executable date

### Revision Note

- `revision_note = extraction_method_risk_present`

### Fallback Timing Assumption

- because `yc_cb` long-window fetches can be truncated, extraction must be windowed or paged before historical use is trusted

---

## Canonical Rule Table

| Field Group | Conservative Release Rule | Effective Rule | Main Caveat |
|---|---|---|---|
| Official PMI | 1st of following month | next open day | source lacks native release_date |
| Caixin PMI | 3rd of following month | next open day | source may not carry exact release timestamp |
| Exports | 10th of following month | next open day | use conservative customs window |
| CPI / PPI | 12th of following month | next open day | latest-history and no native release_date |
| M1 / M2 / TSF / loans | 15th of following month | next open day | M1 semantic drift |
| GDP / industrial / retail / FAI | 17th of following month | next open day | latest-history and mixed reporting styles |
| LPR | source date or 20th | same day or next open day | exact date is usually available |
| Shibor | quote date | same day observation | same-day usage policy must be explicit |
| China gov curve | trade_date | end-of-day / next day execution | extraction truncation risk |

---

## Default As-Of Logic For V1

When building the monthly feature table:

1. assign each slow field a `period_label`
2. compute its `release_date` using the above rule
3. compute `effective_date` as next open trading day on or after `release_date`
4. when generating features for a `rebalance_date`, include only rows with `effective_date <= rebalance_date`

This rule should be hard-coded at the schema / feature-building layer, not left to notebook memory.

---

## Final Judgment

These rules are intentionally conservative.

They may slightly delay signal availability relative to the true market calendar, but they materially reduce the chance of hidden future leakage.

For v1, that is the correct trade-off.
