# Pre-Development Gap Checklist — ETF All-Weather Data Research

## Purpose

This checklist captures the remaining data-research gaps that should be reviewed before moving from ETF all-weather research into implementation.

It is intentionally scoped to `pre-development` work only.

It does **not** cover:
- schema design
- code architecture
- backtest implementation
- model design beyond what is needed to close data-research uncertainty

The goal is simple:

> finish the last few research closures that would otherwise cause rework during development.

---

## Current Judgment

The project is **not missing broad direction**.

The source stack, v1 sleeve set, field boundary, and slow-field timing rules are already largely defined.

What remains are a small number of high-leverage closures:
- final sleeve-level validation for the actually selected instruments
- price-return semantics
- frozen monthly decision timing
- minimum official-source verification
- revision-risk assessment
- final bond-sleeve purity recheck

---

## Gap 1 — Final Stage-01-Equivalent Validation For Actual V1 Sleeves

### Why this is still open

The frozen v1 sleeve set is:

1. `510300.SH`
2. `159845.SZ`
3. `511010.SH`
4. `518850.SH`
5. `159001.SZ`

But the real Stage 01 validation report was run on `511020.SH` rather than the now-selected `511010.SH`, and did not yet fully validate the chosen cash sleeve.

### What must be answered

For `511010.SH` and `159001.SZ`, verify at minimum:

- source availability
- history continuity / gap count
- duplicate-row detection
- zero-value / stale-value detection
- repeatability
- liquidity sanity
- mapping / benchmark identity

### Why it matters

The frozen v1 sleeve set is only fully real if the selected instruments themselves have passed the same quality gate as the originally tested sleeves.

### Completion standard

Produce a short validation note or addendum that explicitly tests `511010.SH` and `159001.SZ` at the same standard used in `stage-01-data-reliability-test-report.md`.

---

## Gap 2 — ETF Return Semantics: Price Return vs Total Return

Status:
- closed by `etf-return-semantics-note.md`

### Why this is still open

The current research has focused on ETF daily availability, continuity, mapping, and timing semantics.

What is not yet clearly frozen is the return convention used by the backtest layer:

- raw close return
- adjusted close return
- total-return equivalent
- explicit dividend handling assumption

### What must be answered

- Does the chosen data source expose adjusted series consistently for the selected ETFs?
- If not, how material are dividends / splits / distributions for each v1 sleeve?
- Can price-only series be used honestly for v1 research, or would that distort long-horizon sleeve comparison?
- Is one sleeve materially more exposed to distribution effects than the others?

### Why it matters

If sleeve return definitions are inconsistent, the strategy may mis-measure:

- long-run return
- realized volatility
- correlation
- risk contribution
- drawdown

This would silently distort the allocation engine before any model logic is even applied.

### Completion standard

Write a short note freezing the v1 return convention and documenting any known distortion from price-only treatment.

Closure result:
- v1 should use an adjustment-aware / total-return-like ETF return basis rather than raw close return

---

## Gap 3 — Freeze One Canonical `rebalance_date_monthly` Rule

Status:
- closed by `monthly-rebalance-date-rule-note.md`

### Why this is still open

The current research correctly states that macro fields may only be used after their `effective_date`, but the exact monthly decision clock is not yet frozen into one canonical operational rule.

### Candidate choices

- month-end last open day
- month-start first open day
- fixed macro-safe date after the core release window
- hybrid rule with explicit fallback

### What must be answered

- Which single rule defines the monthly decision date in v1?
- Does that rule maximize macro completeness, trading simplicity, or both?
- Which macro fields are unavailable under earlier choices?
- Which market fields become stale under later choices?

### Why it matters

Without one canonical decision rule, all later alignment work remains partly ambiguous.

This is not a coding detail. It is a research boundary decision.

### Completion standard

Freeze one v1 monthly decision-clock rule and state clearly why it wins over the alternatives.

Closure result:
- v1 uses `first open trading day on or after the 20th calendar day of each month`

---

## Gap 4 — Minimum Official-Source Verification

Status:
- closed by `minimum-official-source-verification-note.md`

### Why this is still open

The source map correctly identifies `NBS`, `PBOC`, `Chinamoney`, `ChinaBond`, and `Shibor` as official anchors.

But most real fetch validation so far has focused on `Tushare` and `AKShare` wrappers rather than verifying that the official-source path is operationally recoverable when wrappers drift.

### What must be answered

At minimum, verify the practical direct path for:

- one NBS macro field
- one PBOC money/credit field
- one official rates/liquidity field
- one curve-related official source path

This does **not** require building a full scraper suite.

It only requires proving that the repo has a real fallback path when wrapper endpoints break.

### Why it matters

If wrappers fail and no one has verified the official fallback path, the project will discover too late that its “official anchors” were conceptual rather than operational.

### Completion standard

Write a minimum official-source verification note: one representative field per official source family, with access method, friction, and operational verdict.

Closure result:
- `NBS`, `PBOC`, and `ChinaBond` have minimally verified direct paths in this environment
- `Chinamoney` is reachable but operationally awkward
- `Shibor` is not dependable as a direct path in the current environment

---

## Gap 5 — Revision-Risk Assessment For Slow Macro Fields

Status:
- closed by `revision-risk-ranking-note.md`

### Why this is still open

Current documents already acknowledge that many structured series likely reflect `latest_history_only` rather than true first-release vintages.

That is honest, but it still leaves one open question:

> How dangerous is this in practice for v1 regime scoring?

### What must be answered

- Which v1 slow fields are most revision-sensitive?
- Which ones are likely low-risk even if only latest history is available?
- Would plausible revisions materially change quadrant scoring or only slightly perturb a score?
- Which fields deserve lower authority because of revision uncertainty?

### Why it matters

All revision risk is not equal.

The research should distinguish between:
- acceptable prototyping risk
- misleading backtest risk

### Completion standard

Produce a brief ranking of v1 slow fields by revision-risk severity and recommended model authority.

Closure result:
- `M1 / M1-M2 / TSF` are the highest-caution v1 slow fields
- `PMI / PMI_mom / PPI / LPR` remain the cleaner high-authority anchors

---

## Gap 6 — Final Bond-Sleeve Purity And Tradability Recheck

Status:
- closed by `bond-sleeve-suitability-signoff-511010.md`

### Why this is still open

The project already narrowed the bond sleeve thoughtfully, and `511010.SH` is the current chosen v1 instrument.

But before development begins, the final selected bond sleeve should be rechecked as the actual production candidate, not only as a member of the earlier comparison universe.

### What must be answered

- benchmark purity
- duration character
- liquidity stability through time
- missing-date behavior
- whether it is sufficiently close to the intended “bond defense” role
- whether any hidden credit / policy-bank / execution caveat should be explicitly attached

### Why it matters

Bond sleeve confusion is one of the fastest ways for an all-weather system to look diversified while actually carrying the wrong defense exposure.

### Completion standard

Write one final `511010.SH` suitability note with explicit conclusion:

- acceptable for v1 as-is
- acceptable with caveat
- not acceptable and must be replaced

Closure result:
- `511010.SH` is signed off as `acceptable with caveat`

---

## Priority Order

If only the minimum pre-development closures are done, use this order:

1. validate `511010.SH` and `159001.SZ`
2. freeze `rebalance_date_monthly`
3. freeze ETF return semantics
4. verify minimum official-source direct paths
5. rank revision risk across slow fields
6. write final bond-sleeve suitability recheck

---

## Boundary Reminder

Do **not** use this checklist as an excuse to reopen the entire v1 scope.

This is a closure checklist, not a mission reset.

The v1 sleeve set and field boundary remain frozen unless one of the above checks reveals a genuine blocker.

---

## Exit Condition

Pre-development data research is “sufficiently closed” when:

- the actual chosen v1 sleeves have been validated
- monthly decision timing is frozen
- return semantics are frozen
- official-source recovery path is minimally verified
- revision risk is ranked and documented
- bond-sleeve suitability is explicitly signed off

At that point, moving into schema design and implementation becomes justified without pretending that every data ambiguity has been eliminated.

## Current Closure Status

All pre-development gaps listed in this checklist are now closed at the research-note level.

This does not mean every future implementation question is solved.

It means the remaining unknowns no longer justify delaying the move from data-research closure into formal schema and implementation work.
