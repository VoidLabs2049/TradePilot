# Progress Status — ETF All-Weather Data Sources

## Purpose

This document is the continuation handoff for the current ETF all-weather data workstream.

Primary developer-facing handoff summary:

- `developer-handoff-summary.md`

It answers three questions:

1. what has already been completed
2. what remains open
3. where the next execution step should begin

---

## Current Scope Status

### Completed

The following work has been completed and documented:

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

### Stable v1 Outcome So Far

The current frozen v1 sleeve set is:

1. `510300.SH` — large-cap equity
2. `159845.SZ` — small-cap equity
3. `511010.SH` — bond defense
4. `518850.SH` — gold hedge
5. `159001.SZ` — cash / neutral buffer

The current frozen v1 field boundary is documented in:

- `v1-canonical-field-list.md`

---

## What Is Not Finished Yet

The workstream is **not** at implementation-complete state.

The main unfinished parts are:

### 1. V1 schema design

Not done yet.

Still needed:
- table layout
- primary keys
- naming conventions
- typed field definitions
- storage separation between raw / normalized / derived

### 2. Release-date metadata hardening in data model

Rules are documented, but not yet encoded into schema or ingestion logic.

Still needed:
- `period_label`
- `release_date`
- `effective_date`
- `revision_note`
- `definition_regime` for M1-family fields

### 3. Curve extraction hardening

Not done yet.

Current state:
- `yc_cb` is useful, but naïve long-window fetch is truncated by row limits

Still needed:
- paged or windowed extraction design
- canonical 1Y / 10Y extraction logic
- history completeness verification after redesign

### 4. Remaining Stage 02+ data validation

Not done yet.

Stage 01 covered the minimum panel.
Still needed later:
- validation-only metadata fields
- AUM / fund_share reconciliation
- optional market breadth review
- optional overseas overlay review
- deferred fields only if promoted

### 5. Strategy implementation layer

Not started in code yet.

Still needed:
- notebook MVP scaffold
- explainability table
- baseline comparison pack
- backtest logic
- shadow portfolio phase

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

### `v1 schema design`

Reason:

- the asset boundary is frozen
- the field boundary is frozen
- timing rules are frozen enough to encode

This means the project can now move from research specification into a formal data model without reopening upstream scoping questions.

---

## Suggested Immediate Execution Order

When resuming, use this order:

1. design `v1 schema`
2. design `v1 schema`
3. encode release/effective-date metadata fields
4. design curve extraction method for rates layer
5. define raw -> normalized -> derived pipeline
6. only then start notebook MVP implementation

---

## Continuation Anchor

If resuming later, the minimum file set to reload is:

1. `synthesis-01.md`
2. `v1-canonical-field-list.md`
3. `release-date-rules-v1-slow-fields.md`
4. `stage-01-data-reliability-test-report.md`
5. this file: `progress-status.md`
6. `pre-development-gap-checklist.md`
7. `etf-return-semantics-note.md`
8. `monthly-rebalance-date-rule-note.md`
9. `minimum-official-source-verification-note.md`
10. `revision-risk-ranking-note.md`
11. `bond-sleeve-suitability-signoff-511010.md`
12. `developer-handoff-summary.md`

---

## Closure Status

Current project state:

- `data research and v1 boundary definition complete`
- `pre-development data-research closure complete`
- `schema and implementation not yet complete`
