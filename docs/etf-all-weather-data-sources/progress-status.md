# Progress Status — ETF All-Weather

## Purpose

This document is the continuation handoff for the current ETF all-weather workstream.

Primary design entry:

- `../etf-all-weather-implementation/current-design.md`

Primary data-source handoff summary:

- `developer-handoff-summary.md`

It answers three questions:

1. what has already been completed
2. what remains open
3. where the next execution step should begin

---

## Current Scope Status

### Completed

The following research and data-source work has been completed and documented:

1. **Mission framing and source-map research**
   - `mission-charter.md`
   - `milestone-01-local-capability-and-category-map.md`
   - `milestone-02-external-source-survey.md`
   - `data-source-map.md`

2. **Field-level and risk-level analysis**
   - `field-level-data-inventory.md`
   - `data-risk-map.md`

3. **Reliability planning and real Stage 01 execution**
   - `data-reliability-test-plan.md`
   - `stage-01-data-reliability-test-report.md`

4. **Time semantics for slow fields**
   - `release-date-rules-v1-slow-fields.md`

5. **Asset sleeve narrowing for v1**
   - `bond-sleeve-candidate-comparison.md`
   - `cash-short-duration-proxy-comparison.md`

6. **Frozen v1 field boundary**
   - `v1-canonical-field-list.md`

The following implementation stages have also been completed in the repository:

7. **Stage B real-data ingestion slice**
   - `reference.instruments`
   - `reference.trading_calendar`
   - `market.etf_daily`
   - `market.index_daily`
   - raw and normalized Parquet paths
   - validation gating, dedupe-on-rewrite, and watermark advancement
   - report: `../stage-b-ingestion-real-data-test-report.md`

8. **Stage C ETF all-weather v1 data base**
   - `reference.trading_calendar.full_history`
   - `reference.rebalance_calendar.monthly_post_20`
   - `reference.etf_aw_sleeves.frozen_v1`
   - `market.etf_daily`
   - `market.etf_adj_factor`
   - `derived.etf_aw_sleeve_daily`
   - report: `../stage-c-data-backfill-report.md`

9. **Stage D monthly rebalance snapshot**
   - `derived.etf_aw_rebalance_snapshot.build`
   - `derived.etf_aw_rebalance_snapshot`
   - read model contract: `etf_aw_snapshot_v1` / `etf_aw_snapshot_contract_v1`

10. **Stage E market-only regime score**
    - `derived.etf_aw_regime_score.build`
    - `derived.etf_aw_regime_score`
    - read model contract: `etf_aw_regime_score_v1`
    - labels include `risk_on`, `hedge_bid`, `defensive`, `mixed`, and `insufficient_data`

11. **Stage F macro/rates context**
    - `macro.slow_fields`
    - `rates.daily_rates`
    - `rates.lpr`
    - `rates.gov_curve_points`
    - read models: `get_latest_etf_aw_macro_rates_context`, `list_etf_aw_macro_rates_contexts`

12. **Stage G strategy context**
    - `derived.etf_aw_market_features.build`
    - `derived.etf_aw_strategy_context.build`
    - read models: `get_latest_etf_aw_market_features`, `get_latest_etf_aw_strategy_context`
    - contract boundary: context only, no `target_weight`, `trade_action`, or order instruction fields

### Stable v1 Outcome So Far

The current frozen v1 sleeve set is:

1. `510300.SH` — large-cap equity
2. `159845.SZ` — small-cap equity
3. `511010.SH` — bond defense
4. `518850.SH` — gold hedge
5. `159001.SZ` — cash / neutral buffer

The current frozen v1 field boundary is documented in:

- `v1-canonical-field-list.md`

The current implementation design boundary is documented in:

- `../etf-all-weather-implementation/current-design.md`

---

## What Is Not Finished Yet

The workstream is **not** at strategy-complete state.

The main unfinished parts are:

### 1. Risk budget layer

Not implemented yet.

Still needed:

- `derived.etf_aw_risk_budget` schema
- read model contract
- rules-based `strategy_context -> sleeve risk budget` mapper
- degradation behavior for incomplete, stale, or unavailable contexts
- tests for valid, partial, stale, and missing inputs

### 2. Target weight layer

Not implemented yet.

Still needed:

- `derived.etf_aw_target_weight` schema
- budgeted inverse-vol MVP
- later simplified ERC only if the inverse-vol baseline is insufficient
- covariance window and minimum-observation rules
- explicit handling for cash sleeve low volatility
- explainability fields for budget, risk inputs, raw weights, constrained weights, and downgrade reasons

### 3. Rebalance recommendation layer

Not implemented yet.

Still needed:

- current-position input contract
- turnover estimate
- cost filter
- minimum trade amount and ETF lot-size handling
- cash buffer
- paper rebalance plan

This layer must not auto-submit orders.

### 4. Backtest and baseline comparison

Not implemented yet.

Still needed:

- monthly explainability table
- equal-weight baseline
- static inverse-vol baseline
- static risk-parity-like baseline
- cost and turnover assumptions
- parameter perturbation checks

### 5. Remaining Stage 02+ data validation

Partially done.

Stage B-G created a usable data and context base. Still needed later:

- validation-only ETF metadata fields
- AUM / fund_share reconciliation
- optional market breadth review
- optional overseas overlay review
- deferred fields only if promoted

### 6. Pre-development data-research closure

Closed at research-note level.

See:

- `pre-development-gap-checklist.md`
- `stage-01-v1-sleeve-validation-addendum.md`
- `etf-return-semantics-note.md`
- `monthly-rebalance-date-rule-note.md`
- `minimum-official-source-verification-note.md`
- `revision-risk-ranking-note.md`
- `bond-sleeve-suitability-signoff-511010.md`

---

## Recommended Next Step

The most natural next step is:

### `derived.etf_aw_risk_budget` design

Reason:

- the asset boundary is frozen
- the data base and context layers are implemented through Stage G
- current Stage G intentionally stops before `target_weight` and `trade_action`
- the next missing strategy layer is risk budget generation

This means the project can move from research/context assembly into the first explicit strategy calculation layer without reopening upstream scoping questions.

---

## Suggested Immediate Execution Order

When resuming, use this order:

1. keep `../etf-all-weather-implementation/current-design.md` as the current design entry
2. design `derived.etf_aw_risk_budget`
3. implement a rules-based risk budget mapper
4. design `derived.etf_aw_target_weight`
5. implement budgeted inverse-vol MVP
6. add monthly explainability table and baseline comparison
7. only then consider simplified ERC or execution constraints

---

## Continuation Anchor

If resuming later, the minimum file set to reload is:

1. `../etf-all-weather-implementation/current-design.md`
2. this file: `progress-status.md`
3. `v1-canonical-field-list.md`
4. `release-date-rules-v1-slow-fields.md`
5. `stage-01-data-reliability-test-report.md`
6. `../stage-b-ingestion-real-data-test-report.md`
7. `../stage-c-data-backfill-report.md`
8. `pre-development-gap-checklist.md`
9. `etf-return-semantics-note.md`
10. `monthly-rebalance-date-rule-note.md`
11. `minimum-official-source-verification-note.md`
12. `revision-risk-ranking-note.md`
13. `bond-sleeve-suitability-signoff-511010.md`
14. `developer-handoff-summary.md`

---

## Closure Status

Current project state:

- `data research and v1 boundary definition complete`
- `pre-development data-research closure complete`
- `Stage B-G data/context implementation complete enough for the next strategy layer`
- `risk budget, target weight, rebalance plan, backtest, and shadow portfolio not yet complete`
