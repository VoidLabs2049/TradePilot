# Stage 01 V1 Sleeve Validation Addendum

## Purpose

This addendum closes a specific pre-development gap:

> the frozen v1 sleeve set changed after the original Stage 01 report, so the actually selected bond and cash sleeves must be validated at the same practical standard.

This note does not replace the original Stage 01 report.

It supplements it by testing the final selected v1 instruments:

- `511010.SH` — bond defense
- `159001.SZ` — cash / neutral buffer

---

## Test Standard

The validation follows the same practical dimensions used in `stage-01-data-reliability-test-report.md`:

1. identity / mapping
2. source availability
3. coverage and continuity
4. duplicate-row detection
5. zero-value / zero-volume checks
6. extreme-return sanity
7. liquidity sanity
8. fetch repeatability

## Test Window

- `start_date = 20220101`
- `end_date = 20260415`

Primary tested source:
- `Tushare Pro`

---

## 1. `511010.SH` — 国债ETF国泰

### Identity

- `ts_code = 511010.SH`
- `name = 国债ETF国泰`
- `fund_type = 债券型`
- `invest_type = 被动指数型`
- `list_date = 20130325`
- `benchmark = 上证5年期国债指数收益率`

Operational note:
- `fund_basic(market='E')` surfaces the row cleanly
- a generic `fund_basic(market='')` lookup did not surface it directly in this test run

### Coverage

- rows: `1035`
- first trade date: `20220104`
- last trade date: `20260415`
- zero-close rows: `0`

### Gap / Duplicate / Zero Checks

- missing trade dates vs canonical `trade_cal`: `0`
- duplicate trade-date rows: `0`
- zero-volume rows: `0`

### Extreme-Return Sanity

- max abs `pct_chg`: `0.456`
- rows with abs `pct_chg > 5%`: `0`

### Liquidity Sanity

- rows with `vol < 10`: `0`
- rows with `vol < 100`: `0`
- rows with `amount < 1000`: `0`

### Repeatability

- same shape: `True`
- same columns: `True`
- same values: `True`

### Verdict

- `pass`

### Notes

- This sleeve is materially cleaner than the earlier `511020.SH` candidate in Stage 01 terms.
- In the tested window, there were no continuity gaps and no obvious thin-liquidity artifacts.
- This closes the earlier concern that the final chosen bond sleeve had not yet been validated at the same standard as the original candidates.

---

## 2. `159001.SZ` — 货币ETF易方达

### Identity

- `ts_code = 159001.SZ`
- `name = 货币ETF易方达`
- `fund_type = 货币市场型`
- `invest_type = 货币型`
- `list_date = 20141020`
- `benchmark = 活期存款基准利率*(1-利息税税率)`

Operational note:
- `fund_basic(market='E')` surfaces the row cleanly
- a generic `fund_basic(market='')` lookup did not surface it directly in this test run

### Coverage

- rows: `1035`
- first trade date: `20220104`
- last trade date: `20260415`
- zero-close rows: `0`

### Gap / Duplicate / Zero Checks

- missing trade dates vs canonical `trade_cal`: `0`
- duplicate trade-date rows: `0`
- zero-volume rows: `0`

### Extreme-Return Sanity

- max abs `pct_chg`: `0.011`
- rows with abs `pct_chg > 5%`: `0`

### Liquidity Sanity

- rows with `vol < 10`: `0`
- rows with `vol < 100`: `0`
- rows with `amount < 1000`: `0`

### Repeatability

- same shape: `True`
- same columns: `True`
- same values: `True`

### Verdict

- `pass`

### Notes

- The sleeve behaves exactly as hoped for a cash / neutral buffer candidate: smooth, continuous, and operationally clean.
- The near-flat return path should be interpreted as economic character, not as a data-quality problem.

---

## Comparison And Interpretation

Both actual v1 sleeves pass a Stage-01-equivalent reliability check cleanly.

Shared positive findings:
- full tested-window continuity
- no missing trade dates
- no duplicate rows
- no zero-close rows
- no zero-volume rows
- clean repeatability on repeated fetches

Comparative interpretation:
- `159001.SZ` is cleaner statistically because its return path is naturally smoother
- `511010.SH` is cleaner operationally than the previously tested `511020.SH` candidate

Important boundary:
- the two sleeves are both technically acceptable, but economically they serve different roles
- `159001.SZ` is a neutral cash-like buffer
- `511010.SH` is a duration-bearing bond defense sleeve
- they must not be treated as interchangeable “safe assets” inside the strategy logic

---

## Closure Effect

This addendum closes one of the explicit pre-development gaps:

- actual selected v1 bond and cash sleeves are now validated at Stage-01-equivalent standard

This does **not** close the remaining pre-development items such as:
- ETF return semantics
- canonical monthly rebalance-date rule
- official-source direct-path verification
- revision-risk ranking
- final bond-sleeve suitability sign-off beyond pure data cleanliness
