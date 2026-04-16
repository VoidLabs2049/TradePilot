# Revision-Risk Ranking Note — V1 Slow Fields

## Purpose

This note closes one specific pre-development gap:

> among the v1 slow fields, which ones carry the most serious revision risk, and how much authority should each field have in the regime layer?

This is not a vintage-database reconstruction.

It is a practical ranking for honest v1 research.

---

## Core Principle

Not all `latest_history_only` risk is equally dangerous.

The right question is not:

> does revision risk exist?

It clearly does.

The right question is:

> which fields could materially distort v1 state scoring if only revised history is available, and which fields are still acceptable with reduced authority?

---

## Ranking Scale

- `very high`
- `high`
- `medium`
- `low`

Each field also gets an `authority guidance` label:

- `primary_ok`
- `primary_with_caution`
- `confirmatory_preferred`
- `validation_only_if_no_better_path`

---

## Field Ranking

## 1. `m1_yoy`

- revision risk: `very high`
- authority guidance: `primary_with_caution`

### Why

- this is not only a revision problem but also a semantic-regime problem
- post-2025 definition changes make historical comparability weaker even before ordinary revision concerns are considered
- the field is still useful, but it is too dangerous to treat as clean continuous truth

### V1 handling judgment

- keep as `primary`
- always tag `definition_regime`
- never let it dominate the state read alone

---

## 2. `m1_m2_spread`

- revision risk: `very high`
- authority guidance: `primary_with_caution`

### Why

- it inherits the risk of both `m1_yoy` and `m2_yoy`
- it is especially vulnerable to false precision because the spread looks interpretable and directional

### V1 handling judgment

- keep as `primary`
- but require regime tagging and cross-confirmation
- never over-credit it as a master liquidity truth

---

## 3. `tsf_yoy` / `credit_impulse_proxy`

- revision risk: `high`
- authority guidance: `primary_with_caution`

### Why

- credit data is important, but revisions and construction choices can materially alter the apparent pulse
- proxy construction adds one more layer of modeler discretion

### V1 handling judgment

- usable as a core slow field
- but the construction rule must remain explicit
- should be paired with market confirmation before strong allocation tilt

---

## 4. `new_loans_total`

- revision risk: `high`
- authority guidance: `confirmatory_preferred`

### Why

- highly sensitive to monthly noise and structure effects
- more likely to create false narrative conviction than stable regime information

### V1 handling judgment

- better treated as credit-detail confirmation than a root driver

---

## 5. `industrial_production_yoy`

- revision risk: `medium`
- authority guidance: `confirmatory_preferred`

### Why

- release timing matters materially
- revisions matter, but usually less than in money/credit fields for the purpose of broad regime classification

### V1 handling judgment

- keep confirmatory
- useful for checking whether PMI-style strength is backed by realized activity

---

## 6. `retail_sales_yoy`

- revision risk: `medium`
- authority guidance: `confirmatory_preferred`

### Why

- still subject to timing and latest-history caveat
- but less likely than credit fields to create large hidden regime flips in v1 scoring

### V1 handling judgment

- keep confirmatory
- good as domestic-demand confirmation, not primary state engine

---

## 7. `fixed_asset_investment_ytd`

- revision risk: `medium`
- authority guidance: `confirmatory_preferred`

### Why

- the larger risk is interpretation style, not only revision
- YTD reporting can tempt false monthly precision if mishandled

### V1 handling judgment

- use only as reported
- keep confirmatory rather than state-defining

---

## 8. `exports_yoy`

- revision risk: `medium`
- authority guidance: `confirmatory_preferred`

### Why

- externally useful, but not stable enough to deserve primary macro authority in this system

### V1 handling judgment

- confirmation field only

---

## 9. `official_pmi`

- revision risk: `low`
- authority guidance: `primary_ok`

### Why

- the bigger risk is over-interpretation and release timing, not major revision behavior
- the field is operationally central and comparatively stable once release discipline is enforced

### V1 handling judgment

- keep as `primary`
- still never interpret one print as a full regime switch

---

## 10. `official_pmi_mom`

- revision risk: `low`
- authority guidance: `primary_ok`

### Why

- derived from released PMI values
- still sensitive to threshold storytelling, but not the worst revision-risk offender

### V1 handling judgment

- strong primary direction field
- only after both months are independently effective

---

## 11. `ppi_yoy`

- revision risk: `low`
- authority guidance: `primary_ok`

### Why

- revision danger is smaller than the interpretation danger
- the more serious issue is mistaking industrial pricing pressure for broad macro inflation truth

### V1 handling judgment

- keep primary
- but read together with PMI, rates, and market response

---

## 12. `cpi_yoy`

- revision risk: `low`
- authority guidance: `confirmatory_preferred`

### Why

- revision risk is not the main problem
- the real issue is structural noise from food / pork cycles in China inflation interpretation

### V1 handling judgment

- remain confirmatory
- do not promote to root state driver

---

## 13. `core_cpi_yoy`

- revision risk: `medium`
- authority guidance: `confirmatory_preferred`

### Why

- conceptually useful
- but extraction path may be weaker and more manual than headline CPI

### V1 handling judgment

- useful if stable extraction exists
- otherwise do not force into core v1 dependency

---

## 14. `lpr_1y`

- revision risk: `low`
- authority guidance: `primary_ok`

### Why

- release date is naturally date-stamped
- revision risk is meaningfully lower than most slow macro fields

### V1 handling judgment

- clean primary rate field under the frozen post-20th monthly decision clock

---

## Summary Ranking

### Highest caution set

- `m1_yoy`
- `m1_m2_spread`
- `tsf_yoy` / `credit_impulse_proxy`

### Medium caution set

- `new_loans_total`
- `industrial_production_yoy`
- `retail_sales_yoy`
- `fixed_asset_investment_ytd`
- `exports_yoy`
- `core_cpi_yoy`

### Lower revision-risk set

- `official_pmi`
- `official_pmi_mom`
- `ppi_yoy`
- `cpi_yoy`
- `lpr_1y`

---

## Final V1 Judgment

The v1 system does **not** need to demote all revision-risk fields out of the model.

What it does need is a hierarchy:

1. keep `PMI / PMI_mom / PPI / LPR` as the cleaner high-authority anchors
2. keep `M1 / M1-M2 / TSF` as important but explicitly caveated primaries
3. keep activity and trade fields confirmatory
4. keep `new_loans_total` and other detailed credit fields out of the root state engine

This is the honest compromise for a v1 monthly system without true real-time vintage infrastructure.
