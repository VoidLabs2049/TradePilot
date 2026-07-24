# Monthly Rebalance Date Rule Note — V1 Freeze

## Purpose

This note closes one specific pre-development research gap for the ETF all-weather strategy:

> what single canonical `rebalance_date_monthly` rule should v1 use?

This is a research-boundary decision.

It is not a schema or execution-engine design note.

---

## Problem

The existing research already did the hard part:

- slow fields have conservative `release_date_rule`
- each field receives an `effective_date`
- the system may only use fields whose `effective_date <= rebalance_date_monthly`

But the monthly decision clock itself was still open.

Without freezing one canonical rule, all later feature alignment remains partly ambiguous.

---

## Candidate Rules Considered

### Candidate A — Month-end last open day

### Strength

- operationally simple
- easy to explain
- uses the fullest within-month market information

### Weakness

- unnecessarily delays use of already-effective macro data
- blends “macro-safe timing” with “calendar convenience” rather than making one principle explicit
- can make the monthly system slower than necessary without earning much extra robustness

### Candidate B — Month-start first open day

### Strength

- simple recurring schedule
- reacts earlier to new-month macro releases such as PMI

### Weakness

- misses CPI/PPI, credit, industrial/retail/FAI, and LPR for the same monthly cycle
- would force the system to pretend many core v1 slow fields do not exist in the decision clock

This is not compatible with the current v1 field boundary.

### Candidate C — Macro-safe date after the core release window

Interpretation:
- choose a fixed monthly date late enough that the main slow fields are already effective

### Strength

- consistent with the whole release-date discipline
- keeps the monthly system honest
- avoids pretending month-end values were knowable earlier

### Weakness

- less intuitive than a pure month-end rule
- requires one explicit date choice

---

## Key Timing Constraint From Existing Rules

Under the current conservative release framework:

- PMI is safe after the `1st`
- exports after the `10th`
- CPI / PPI after the `12th`
- money / credit after the `15th`
- GDP / industrial / retail / FAI after the `17th`
- LPR around the `20th`

This creates one clear conclusion:

> if v1 wants to keep `lpr_1y` as a primary field and still use the main slow macro set honestly, the monthly decision clock must sit after the `20th` window, not before it.

---

## Frozen V1 Rule

The canonical v1 monthly rebalance rule is:

> `rebalance_date_monthly = first open trading day on or after the 20th calendar day of each month`

This is the default monthly decision clock for the ETF all-weather v1 system.

### Why this wins

1. it includes the full core slow-field set under the current conservative timing rules
2. it keeps `lpr_1y` usable without special exceptions
3. it avoids hidden future leakage from pretending mid-month releases were known at month-start
4. it is still simple enough to implement and explain

---

## Interpretation Of The Rule

For each month:

1. take the `20th` calendar day
2. if that day is an open trading day, use it
3. if not, use the next open trading day from the canonical trade calendar

This rule defines the monthly decision anchor.

It does **not** by itself define the exact execution timestamp.

For research alignment, the important closure is narrower:

- all slow fields must satisfy `effective_date <= rebalance_date_monthly`
- daily market fields must use only data actually observable by that decision point

---

## Why Month-end Was Not Chosen

Month-end last open day is not wrong.

It was not chosen because it adds delay without solving a real v1 timing problem.

Once the strategy already waits until the first open day on or after the `20th`, the key slow fields are available under a conservative clock.

Waiting until month-end would mostly:

- reduce responsiveness
- add extra within-month drift to the market layer
- make the monthly cycle less directly tied to the macro publication structure

The cleaner research logic is to anchor the schedule to the release calendar, not to an arbitrary month-end convention.

---

## Why Month-start Was Rejected

Month-start first open day would be attractive only for a much thinner signal set.

But current v1 explicitly includes:

- CPI / PPI
- money / credit
- GDP / industrial / retail / FAI
- LPR

So a month-start clock would force one of two bad outcomes:

1. discard too many intended v1 fields
2. silently leak future information

Neither is acceptable.

---

## Boundary And Caveats

This rule is frozen for `v1` only.

Possible future reasons to revisit it:

- the field boundary changes materially
- `lpr_1y` is demoted from `primary`
- a future `v1.5` or `v2` system separates macro refresh and portfolio rebalance into different clocks

For now, none of those changes are earned.

---

## Closure Result

This note closes the monthly decision-clock ambiguity.

The v1 ETF all-weather system should assume:

- one monthly rebalance decision date
- anchored to the first open trading day on or after the `20th` calendar day
- with all slow fields filtered by `effective_date <= rebalance_date_monthly`

This is the correct trade-off between:

- macro completeness
- timing honesty
- implementation simplicity
