# ETF Return Semantics Note — V1 Convention Freeze

## Purpose

This note closes one specific pre-development research question for the ETF all-weather strategy:

> should v1 use raw ETF price returns, or an adjusted / total-return-like convention?

This is a data-research question, not a schema or implementation question.

---

## Tested Universe

The frozen v1 sleeves:

1. `510300.SH`
2. `159845.SZ`
3. `511010.SH`
4. `518850.SH`
5. `159001.SZ`

## Test Window

- `20220101` to `20260415`

## Tested Tushare Endpoints

- `fund_daily`
- `fund_adj`

Practical test method:
- verify both datasets exist for each sleeve
- inspect whether `adj_factor` changes over time
- compare raw cumulative return from `close`
- compare a simple adjusted-close proxy based on `close` and `adj_factor`

Important boundary:
- this note does **not** claim to reconstruct a perfect legal/economic total-return series for every ETF structure
- it only answers whether raw `close` is safe enough as the canonical v1 return measure

---

## Main Finding

Raw ETF close is **not** a sufficiently stable v1 return convention across the selected sleeves.

At least three of the five sleeves show meaningful or very large divergence once `adj_factor` behavior is taken seriously:

- `510300.SH`
- `159845.SZ`
- `511010.SH`

By contrast:

- `518850.SH`
- `159001.SZ`

showed no adjustment effect in the tested window.

This is enough to freeze the v1 research convention:

> v1 should use an adjusted / total-return-like ETF return treatment, not raw close return.

---

## Sleeve-by-Sleeve Findings

## 1. `510300.SH` — 沪深300ETF华泰柏瑞

- `fund_daily`: available
- `fund_adj`: available
- `adj_factor`: changed over time
- distinct factor values: `6`
- change points: `5`
- raw cumulative return: `-5.93%`
- adjusted-return proxy cumulative return: `+4.00%`
- gap: `+9.93 pct pts`

### Judgment

This sleeve is materially affected by adjustment treatment.

Using raw close would understate the sleeve's economic return over the tested window.

---

## 2. `159845.SZ` — 中证1000ETF华夏

- `fund_daily`: available
- `fund_adj`: available
- `adj_factor`: changed over time
- distinct factor values: `3`
- change points: `2`
- raw cumulative return: `+298.21%`
- adjusted-return proxy cumulative return: `+4.73%`
- gap: `-293.48 pct pts`

### Judgment

This sleeve is the strongest evidence that raw close cannot be trusted as a canonical v1 return measure.

The difference is too large to treat as noise.

Whether the driver is split-like price mechanics, distribution adjustment, or other ETF-specific normalization behavior, the practical implication is the same:

> raw price path is not an honest economic-return series for v1 research.

---

## 3. `511010.SH` — 国债ETF国泰

- `fund_daily`: available
- `fund_adj`: available
- `adj_factor`: changed over time
- distinct factor values: `4`
- change points: `3`
- raw cumulative return: `+11.22%`
- adjusted-return proxy cumulative return: `+13.42%`
- gap: `+2.20 pct pts`

### Judgment

The difference is smaller than for the equity sleeves, but still material enough that raw close is not ideal as the frozen v1 standard.

For a bond-defense sleeve, even modest cumulative-return distortion can affect later:

- volatility estimates
- correlation estimates
- risk contribution comparisons

---

## 4. `518850.SH` — 黄金ETF华夏

- `fund_daily`: available
- `fund_adj`: available
- `adj_factor`: unchanged in the tested window
- distinct factor values: `1`
- change points: `0`
- raw cumulative return: `+177.30%`
- adjusted-return proxy cumulative return: `+177.30%`
- gap: `0.00 pct pts`

### Judgment

In the tested window, raw and adjusted treatment are identical for this sleeve.

This does **not** prove gold ETFs never need adjustment.
It only shows that no adjustment event affected this window.

---

## 5. `159001.SZ` — 货币ETF易方达

- `fund_daily`: available
- `fund_adj`: available
- `adj_factor`: unchanged in the tested window
- distinct factor values: `1`
- change points: `0`
- raw cumulative return: `-0.00%`
- adjusted-return proxy cumulative return: `-0.00%`
- gap: `0.00 pct pts`

### Judgment

In this test window, raw and adjusted treatment behave the same.

That makes the sleeve easy to work with operationally, but it does not rescue raw-close treatment for the overall system because the equity and bond sleeves already invalidate that simplification.

---

## Frozen V1 Convention

The v1 research convention should be:

- use `adjusted / total-return-like` ETF return treatment as the canonical return basis
- do **not** use raw `close` return as the root return definition for the 5-sleeve system

Why this wins:

1. it is the only convention that stays reasonably coherent across mixed sleeve types
2. it prevents silent distortion in long-horizon return and risk estimates
3. the selected v1 sleeves already provide `fund_adj`, so this is an earned simplification rather than a fantasy requirement

Operational interpretation:

- for research and backtest purposes, the default return series should be based on Tushare adjustment-aware treatment
- raw close can still be retained as a validation field, but not as the canonical sleeve-return source

---

## What This Still Does Not Prove Perfectly

This note does **not** fully settle:

- exact legal dividend mechanics for every ETF product
- whether a specific adjusted-price formula should be forward-adjusted or backward-adjusted in implementation
- whether money-market ETF economics require product-specific treatment beyond `fund_adj`

Those are later implementation details.

For pre-development research, the closure is narrower and sufficient:

> raw ETF close is not an honest enough universal v1 return convention, so v1 should freeze on an adjustment-aware return basis.
