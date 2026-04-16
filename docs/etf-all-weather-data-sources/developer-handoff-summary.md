# Developer Handoff Summary — ETF All-Weather V1

## Purpose

This document is the single developer-facing handoff summary for the current ETF all-weather research work.

It is written for the next implementation owner.

It answers five practical questions:

1. what this project is trying to build
2. what has already been decided and should not be reopened casually
3. what data/source/timing conclusions have already been earned
4. what caveats must survive into code
5. what the next implementation sequence should be

This is a summary layer over the existing research artifacts.

It does **not** replace the underlying notes.

---

## 1. Project Objective

The current target is a **small, transparent, monthly ETF all-weather v1 allocation engine** for China-market implementation research.

This is **not** currently:

- a deep-learning macro allocator
- an end-to-end black-box portfolio model
- a giant multi-asset global macro platform
- a high-frequency or event-driven system

The intended v1 shape is:

- monthly decision cycle
- small fixed sleeve set
- transparent risk-budget logic
- simplified ERC-style allocation
- honest macro timing discipline
- strong explainability and low hidden complexity

The core design philosophy is:

> first make a system that is honest, stable, and auditable; only then consider richer state expression or more complex models.

---

## 2. Strategic Boundary Already Frozen

The research phase has already frozen the most important v1 boundaries.

These should **not** be casually reopened during implementation unless a real blocker is discovered.

### 2.1 Frozen v1 sleeve set

The v1 system uses exactly these `5` sleeves:

1. `510300.SH` — large-cap equity
2. `159845.SZ` — small-cap equity
3. `511010.SH` — bond defense
4. `518850.SH` — gold hedge
5. `159001.SZ` — cash / neutral buffer

Important:
- backup instruments may exist in research notes
- they are **not** part of the canonical v1 definition

### 2.2 Frozen v1 scope

Included in v1:

- ETF sleeve prices
- domestic macro slow variables
- domestic rates/liquidity context
- sparse market-confirmation layer
- execution-only realized-vol style scaling fields

Explicitly deferred from v1:

- ETF options sentiment
- full domestic credit-spread system
- CTA / neutral proxy layer
- commodity sleeve root system
- overseas overlay as core driver
- ML classifier inputs beyond the earned rule stack
- deep-learning regime model

### 2.3 Frozen return convention

v1 should use an **adjustment-aware / total-return-like ETF return basis**, not raw close return.

Reason:
- raw close materially distorts at least part of the selected sleeve set
- this is already demonstrated in research

### 2.4 Frozen monthly decision clock

The canonical v1 monthly rebalance rule is:

> `rebalance_date_monthly = first open trading day on or after the 20th calendar day of each month`

Reason:
- this is the earliest simple monthly clock that still keeps the core slow-field set usable under conservative release rules, including `LPR`

---

## 3. Data Stack Conclusion

The practical v1 stack is already decided.

### 3.1 Primary stack

- `Tushare` for the core panel backbone
- official sources for macro/rates anchoring and recovery:
  - `NBS`
  - `PBOC`
  - `ChinaBond`
  - `Chinamoney`
  - `Shibor`
- `AKShare` as wrapper/fallback, not sole truth
- `FRED` only for later/small global overlay use

### 3.2 Practical interpretation

The stack should be read as:

- `Tushare` = easiest normalized implementation backbone
- official pages = authoritative anchor and fallback path
- `AKShare` = convenience wrapper and gap-filler
- official source does **not** imply low-friction automation

### 3.3 Official-source direct-path status

Already minimally verified:

- `NBS`: direct page path works
- `PBOC`: direct page path works
- `ChinaBond`: direct page path works
- `Chinamoney`: partially reachable, operationally awkward
- `Shibor`: not dependable as direct path in current environment

Implication for development:
- do not assume all official anchors are equally automation-friendly
- preserve wrapper convenience, but do not design the system as if wrappers are infallible

---

## 4. Canonical Data Layers

The field boundary is already frozen in detail in the underlying notes.

At a summary level, v1 has these data layers.

### 4.1 Layer 0 — Calendar and decision timing

- `trade_date`
- `pretrade_date`
- `rebalance_date_monthly`

This layer is foundational.

### 4.2 Layer 1 — Sleeve price layer

- large-cap ETF
- small-cap ETF
- bond sleeve
- gold sleeve
- cash sleeve

These drive returns, vol, drawdown, and allocation math.

### 4.3 Layer 2 — Instrument identity / validation layer

- fund code and name
- listing exchange
- benchmark identity
- fund share / AUM / fees

These are mostly validation fields, not model signals.

### 4.4 Layer 3 — Domestic macro slow variables

Primary fields:

- `official_pmi`
- `official_pmi_mom`
- `ppi_yoy`
- `m1_yoy`
- `m2_yoy`
- `m1_m2_spread`
- `tsf_yoy` or `credit_impulse_proxy`

Confirmation fields:

- `cpi_yoy`
- `core_cpi_yoy`
- `industrial_production_yoy`
- `retail_sales_yoy`
- `fixed_asset_investment_ytd`
- `exports_yoy`
- `new_loans_total`

### 4.5 Layer 4 — Rates and liquidity

Primary or near-primary:

- `shibor_1w`
- `lpr_1y`
- `cn_gov_10y_yield`

Confirmation:

- `lpr_5y`
- `cn_gov_1y_yield`
- `cn_yield_curve_slope_10y_1y`

### 4.6 Layer 5 — Market confirmation layer

- `hs300_vs_zz1000_20d`
- `bond_trend_20d`
- `gold_trend_20d`

These should confirm or throttle, not define macro state alone.

### 4.7 Layer 6 — Execution-only scaling layer

- `realized_vol_20d_*`
- optional later: `atr_20`

These are execution/risk-scaling inputs, not macro-state drivers.

---

## 5. Timing Discipline That Must Survive Into Code

This is one of the most important earned conclusions from research.

### 5.1 Slow fields must be time-aware

Every slow macro field must eventually carry:

- `period_label`
- `release_date`
- `effective_date`
- `revision_note`

Additional mandatory metadata for M1-family fields:

- `definition_regime`
- `regime_note`

### 5.2 Conservative effective-date logic

Rule already frozen:

- if a field has no native trustworthy `release_date`, assign one conservatively by field family
- `effective_date = next open trading day on or after release_date`
- a slow field may only enter the feature set when `effective_date <= rebalance_date_monthly`

### 5.3 Why this matters

The most dangerous v1 failure is not bad model selection.

It is hidden future leakage from using macro fields before they were knowable.

Do not “fix later” what the research phase already explicitly solved.

---

## 6. Core Data Caveats That Developers Must Respect

### 6.1 `M1 / M1-M2 / TSF` are important but caveated

They remain in v1, but they are the highest-caution slow fields.

Do not code them as if they are clean timeless truth.

### 6.2 Bond sleeve is clean enough, but not abstractly pure

`511010.SH` is signed off as:

- acceptable for v1
- with caveat

The caveat is economic, not data-quality-related:

- it is a 5Y sovereign duration proxy
- not a maximally convex crisis hedge
- not a universal “bond factor”

### 6.3 Curve layer is still operationally caveated

The curve fields are conceptually in v1, but historical extraction still needs hardening.

In practice:
- `yc_cb` style extraction can truncate on longer windows
- a windowed/paged method still needs to be designed before curve history is treated as operationally clean

### 6.4 Wrapper agreement is not truth by itself

If several wrappers agree, that can still mean they are inheriting the same upstream issue.

Retain explicit official-anchor awareness in implementation design.

---

## 7. What Has Been Verified Already

### 7.1 Sleeve-level reliability

Actual selected v1 sleeves have now been validated at Stage-01-equivalent level.

Most important operational outcome:
- `511010.SH` and `159001.SZ` both pass cleanly in the tested window

### 7.2 Return convention

Already settled:
- use adjustment-aware ETF returns
- do not use raw close as canonical sleeve-return source

### 7.3 Monthly decision timing

Already settled:
- use first open trading day on or after the 20th calendar day

### 7.4 Official-source recovery layer

Already minimally verified:
- enough to justify official sources as real anchors
- not enough to assume they are frictionless data feeds

### 7.5 Pre-development data research closure

This is effectively complete.

The remaining blockers are no longer research-scope questions.

They are engineering/implementation tasks.

---

## 8. What Has Not Been Implemented Yet

This is the most important section for the next owner.

The project is **not** implementation-complete.

The main unfinished work is:

### 8.1 `v1 schema design`

Still not done.

Needed:
- table layout
- primary keys
- naming conventions
- typed field definitions
- separation of `raw / normalized / derived`

### 8.2 release/effective-date encoding in data model

Rules exist in research.

They are not yet encoded into a formal schema or ingestion pipeline.

### 8.3 curve extraction hardening

Still not done.

Needed:
- paged or windowed extraction design
- canonical 1Y / 10Y extraction logic
- history completeness verification

### 8.4 implementation layer

Still not started in code.

Needed:
- notebook MVP scaffold
- baseline score / risk-budget logic
- explainability table
- backtest logic
- shadow-portfolio phase

---

## 9. Recommended Implementation Order

The research phase already narrowed the correct execution sequence.

Do not jump straight into backtest notebooks without this order.

### Step 1 — Design `v1 schema`

Why first:
- sleeve set is frozen
- field boundary is frozen
- timing rules are frozen enough to encode

### Step 2 — Encode timing metadata

Specifically:
- `period_label`
- `release_date`
- `effective_date`
- `revision_note`
- `definition_regime`

### Step 3 — Design `raw -> normalized -> derived` pipeline

This is critical.

Do not let notebooks become the first place where normalization logic lives.

### Step 4 — Harden rates / curve extraction

Especially for:
- `cn_gov_10y_yield`
- `cn_gov_1y_yield`
- `cn_yield_curve_slope_10y_1y`

### Step 5 — Build notebook MVP

Only after the above.

The MVP should be:
- small
- explainable
- monthly
- rule-first

### Step 6 — Build backtest + explainability layer

Minimum outputs should include:
- state summary
- sleeve returns
- budget tilt explanation
- final weights
- turnover
- realized risk contribution

### Step 7 — Shadow portfolio phase

Before any serious live use.

---

## 10. Recommended V1 Logic Shape

The next implementation owner should keep the first live version simple.

### State layer

Use:
- rule-based macro scoring
- simple market confirmation

Do not start with:
- learned classifier
- nonlinear meta-model
- end-to-end allocation engine

### Budget layer

Use:
- neutral baseline budget
- limited tilt by macro + confirmation signals

### Optimization layer

Use:
- simplified ERC / near-ERC
- clear upper bounds
- low complexity constraints

### Execution layer

Use:
- monthly rebalance only
- low-frequency risk scaling

Do not start with:
- threshold-rich overactive execution logic
- technical-indicator-heavy control stack

---

## 11. What Should Not Be Reopened During Implementation

The next owner should avoid turning implementation into a re-research cycle.

Unless a real blocker appears, do **not** reopen:

- the 5-sleeve v1 definition
- the monthly frequency assumption
- the post-20th decision clock
- the adjustment-aware ETF return convention
- the defer decision on deep learning / options sentiment / full credit system

The correct mindset is:

> implement the earned v1, do not redesign the mission in the middle of coding.

---

## 12. Residual Risks

Even with research closure, these risks remain and should be acknowledged in code comments / docs / validation.

### Research-valid but still real risks

- macro revision risk is present
- M1 semantic drift is real
- bond sleeve is instrument-specific rather than idealized factor-pure
- curve extraction still needs operational hardening
- official direct paths are uneven in automation friendliness

These are not reasons to stop.

They are reasons to keep implementation honest.

---

## 13. Minimal Reload Set For Future Work

If another developer needs to reload only the minimum serious context, use this order:

1. `progress-status.md`
2. `v1-canonical-field-list.md`
3. `release-date-rules-v1-slow-fields.md`
4. `stage-01-data-reliability-test-report.md`
5. `stage-01-v1-sleeve-validation-addendum.md`
6. `etf-return-semantics-note.md`
7. `monthly-rebalance-date-rule-note.md`
8. `minimum-official-source-verification-note.md`
9. `revision-risk-ranking-note.md`
10. `bond-sleeve-suitability-signoff-511010.md`

This summary file should sit above those notes as the primary handoff cover sheet.

---

## 14. Final Developer Message

The research phase is finished enough.

The project is no longer blocked by uncertainty about:

- what sleeves to trade
- what fields belong in v1
- what timing discipline to use
- what return convention to use
- whether the chosen v1 proxies are operationally acceptable

The project is now blocked by engineering work, not by missing macro ideas.

The correct next move is:

> start schema and pipeline implementation without reopening the frozen v1 boundary unless a concrete blocker appears.
