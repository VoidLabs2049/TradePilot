# ETF All-Weather Data Reliability Test Plan

## Purpose

Before we move further into schema design or implementation, we need an explicit test plan for data reliability.

This plan answers:

> How do we verify, field by field and layer by layer, that the data is reliable enough for a monthly ETF all-weather research system?

The aim is not to prove the data is perfect.
The aim is to make hidden failure modes visible early enough that they do not contaminate the strategy logic.

---

## Testing Philosophy

Data reliability here means more than “the API returned something.”

A field is only reliable if it passes enough of these dimensions:

1. **Availability**
   - Can we fetch it consistently?
2. **Identity**
   - Are we sure this field refers to the instrument / concept we think it does?
3. **Timing**
   - Is the value usable only after the correct release or market timestamp?
4. **Continuity**
   - Is the historical series reasonably continuous, or full of silent gaps / schema changes?
5. **Cross-source agreement**
   - Does it broadly agree with at least one validation source?
6. **Interpretation fitness**
   - Even if accurate, is it suitable for the role we want to give it?

The first five are data tests.
The sixth is a strategy-facing test and should not be skipped.

---

## Pass / Fail Labels

Each field group should be assigned one of these statuses after testing:

- `pass`
  - acceptable for v1 primary use
- `pass_with_caveat`
  - usable, but with documented boundary or weakness
- `validation_only`
  - useful for checking, not for model input
- `defer`
  - too unstable / ambiguous / expensive for v1
- `fail`
  - not trustworthy enough for current use

---

## Test Phases

## Phase 1 — Source-Level Reliability

Question:

> Is this source operationally stable enough to be part of the stack at all?

### Tests

1. **Fetch repeatability test**
   - fetch the same endpoint multiple times across different runs
   - confirm schema and key fields are stable

2. **Historical reach test**
   - verify the source covers the date range required for backtest

3. **Null / gap test**
   - check missing rows, blank fields, obvious truncation, or sudden start dates

4. **Schema drift test**
   - compare current output field names/units with older expectations or docs

5. **Operational dependency test**
   - note token gating, anti-scraping behavior, undocumented endpoints, or fragile wrappers

### Output

Each source gets a short note:
- stable primary
- stable fallback
- fragile fallback
- validation-only

---

## Phase 2 — Instrument Identity And Mapping Tests

Question:

> Are we sure the traded objects and benchmark identities are mapped correctly?

### Targets

- large-cap ETF sleeve
- small-cap ETF sleeve
- bond sleeve
- gold sleeve
- cash / short-bond proxy

### Tests

1. **Code-name consistency test**
   - verify `fund_code`, `fund_name`, and exchange listing match across sources

2. **Benchmark mapping test**
   - confirm each ETF's stated benchmark index from prospectus / exchange / fund page

3. **Sleeve role test**
   - confirm the instrument actually represents the intended sleeve exposure
   - example: bond sleeve should be checked for duration purity versus credit mixture

4. **Tradability sanity test**
   - inspect volume, amount, listing history, and obvious liquidity holes

5. **Metadata drift test**
   - check whether benchmark, share class, or structural features changed over time

### Output

Each sleeve gets an `exposure note`:
- intended role
- actual tradable proxy
- known mismatch risk

---

## Phase 3 — Price Series Integrity Tests

Question:

> Are the sleeve price histories clean enough for return, volatility, and trend calculations?

### Targets

- ETF / fund daily prices
- index daily prices
- cash proxy return series

### Tests

1. **Duplicate-row test**
   - no repeated date rows for the same instrument

2. **Gap test**
   - identify missing dates relative to trade calendar

3. **Suspension / illiquidity test**
   - detect long runs of unchanged price with near-zero volume

4. **Extreme-jump sanity test**
   - flag implausible returns for manual check

5. **Cross-source close comparison**
   - compare close prices across primary and fallback sources on sampled dates

6. **Return consistency test**
   - compute simple returns and verify no impossible artifacts from bad corporate-action handling or source issues

### Output

For each instrument:
- price series health status
- date coverage summary
- known gap/suspension note

---

## Phase 4 — Release-Timing Tests For Slow Data

Question:

> Are macro and policy fields aligned to the dates when they were actually knowable?

### Targets

- PMI
- CPI / core CPI / PPI
- M1 / M2
- TSF / loans
- GDP / industrial / retail / FAI / exports
- LPR

### Tests

1. **Release-date capture test**
   - every slow field must have a release date or explicit lag rule

2. **Effective-date alignment test**
   - confirm the model only uses the field after release

3. **Month-end leakage test**
   - ensure monthly observations are not backfilled to the entire month before publication

4. **Announcement-lag simulation test**
   - rerun a sample panel as-of specific rebalance dates and confirm only then-known values appear

5. **Revision policy test**
   - define whether the stored series reflects first release or latest revised data

### Output

For each slow field:
- `release_date_rule`
- `effective_date_rule`
- `revision_note`

---

## Phase 5 — Semantic Drift Tests

Question:

> Has the meaning of the field changed enough that we must split regimes or downgrade its authority?

### Targets

- M1 / M2 family
- TSF and loan-structure fields
- ETF benchmark / sleeve identity
- any rate series with methodology change

### Tests

1. **Definition-change audit**
   - document known methodology or definition changes

2. **Regime-boundary tagging test**
   - split series into pre/post regimes where required

3. **Comparability test**
   - check whether pre-change and post-change values can be used in one model without adjustment

4. **Interpretation downgrade test**
   - if comparability is weak, demote the field from primary signal to supporting evidence

### Output

For each affected field:
- `regime_note`
- `comparable / partially comparable / non-comparable`
- recommended model role

---

## Phase 6 — Cross-Source Agreement Tests

Question:

> Does the field broadly agree with at least one independent validation surface?

### Tests

1. **Point-in-time spot check**
   - sample several dates and compare values across sources

2. **Direction-consistency test**
   - even if levels differ, check whether direction and major turning points align

3. **Unit / scale audit**
   - confirm notional, percent, basis-point, and raw-value conventions match

4. **Tolerance-band rule**
   - define acceptable difference thresholds by field type

### Output

For each field group:
- agreement score
- tolerance note
- whether fallback is truly independent or just another wrapper of the same upstream source

---

## Phase 7 — Strategy-Fitness Tests

Question:

> Even if the field is accurate, should it actually enter the v1 model?

### Tests

1. **Role clarity test**
   - can we clearly say whether the field is:
   - primary signal
   - confirmation signal
   - execution filter
   - validation-only field

2. **Complexity-earned test**
   - does the field add insight not already captured by a simpler field?

3. **Interpretability test**
   - can the monthly decision table explain how this field affected allocation?

4. **Narrative-risk test**
   - is the field especially likely to create coherent but false stories?

### Output

Each field is assigned one role:
- `primary`
- `confirmatory`
- `execution_only`
- `validation_only`
- `defer`

---

## Field Group Test Matrix

## A. Trading Calendar

Must pass:
- source-level reliability
- gap test
- holiday mapping sanity

Failure consequence:
- entire backtest timing becomes suspect

## B. Sleeve Prices

Must pass:
- instrument identity
- price continuity
- cross-source close comparison
- tradability sanity

Failure consequence:
- all return, volatility, and trend features become unreliable

## C. ETF Metadata / AUM

Must pass:
- code / name mapping
- benchmark mapping
- cross-source scale comparison

Can tolerate:
- minor reporting lag differences

Default role:
- `validation_only` or `pass_with_caveat`

## D. Macro Slow Fields

Must pass:
- release-date capture
- effective-date alignment
- revision policy note

High-risk fields needing special semantic review:
- `m1_yoy`
- `m2_yoy`
- `m1_m2_spread`
- `tsf_yoy`

## E. Rates / Curve Fields

Must pass:
- point-in-time availability
- unit/scale audit
- direction-consistency across sources

## F. Market Confirmation Fields

Must pass:
- derived-field reproducibility
- role clarity

Risk:
- should not be quietly promoted into main macro driver

## G. Technical Filters

Must pass:
- derived-field reproducibility
- complexity-earned test

Default expectation:
- many should end up `execution_only` or `defer`

## H. Overseas Overlay

Must pass:
- point-in-time consistency
- role clarity as overlay only

Risk:
- over-dominating domestic state engine

---

## Staged Execution Plan

## Stage 1 — Must Validate Before Any Backtest

1. trade calendar
2. sleeve price series
3. PMI / PPI / M1 / M2 / TSF release timing
4. China 10Y yield / Shibor / LPR timing and units

Goal:
- establish a trustworthy minimum panel

## Stage 2 — Validate Before Building Regime Logic

1. benchmark mapping of sleeves
2. relative-strength derived fields
3. bond and gold trend fields
4. semantic-regime notes for M1/M2 family

Goal:
- avoid mislabeling exposures and over-reading signals

## Stage 3 — Validate Before Any Advanced Expansion

1. AUM / fund_share fields
2. breadth fields
3. overseas overlay fields
4. optional technical filters
5. any credit or options data

Goal:
- prevent optional complexity from contaminating v1

---

## Suggested Evidence Format For Each Test

Each tested field or field group should leave behind a small validation note with:

- field name
- role in strategy
- primary source
- fallback source
- tests run
- result label
- known caveats
- promote / keep / defer decision

This creates a pre-strategy audit trail.

---

## Exit Criteria

The data layer is ready for v1 schema and ingestion design only when all of these are true:

1. the minimal v1 field set has a source assignment
2. each slow field has a release-date rule
3. each sleeve has an exposure note
4. high-risk semantic-drift fields have regime notes
5. price series and calendar tests are passing
6. optional fields are explicitly marked `defer` or `validation_only` rather than silently floating in scope

---

## Final Judgment

The right next move is not to code blindly.

It is to turn data reliability into a staged verification program.

Only after that should the strategy be allowed to harden into schema, ingestion, and backtest logic.
