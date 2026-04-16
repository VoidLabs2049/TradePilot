# Bond Sleeve Suitability Sign-Off — `511010.SH`

## Purpose

This note closes the final pre-development bond-sleeve question:

> is `511010.SH` acceptable as the v1 bond-defense sleeve, and if so, under what caveat?

This is a sign-off note, not a new candidate search.

---

## Instrument

- `ts_code = 511010.SH`
- `name = 国债ETF国泰`
- benchmark: `上证5年期国债指数收益率`

Intended v1 role:
- `bond defense`
- stable duration-sensitive defensive sleeve

---

## What Was Already Established Earlier

From the earlier bond-sleeve comparison work:

- `511010.SH` had the longest listed history in the candidate set
- zero missing trade days in the tested window used there
- zero obvious low-liquidity flags under the test thresholds used there
- benchmark exposure was judged clean and interpretable
- the main weakness was not dirtiness, but lower convexity versus longer-duration alternatives

From the new Stage-01-equivalent validation addendum:

- rows: `1035`
- first trade date in test window: `20220104`
- last trade date in test window: `20260415`
- missing trade dates vs calendar: `0`
- duplicate rows: `0`
- zero-close rows: `0`
- zero-volume rows: `0`
- rows with `vol < 10`: `0`
- rows with `vol < 100`: `0`
- rows with `amount < 1000`: `0`
- repeatability: clean

This means the instrument is now validated both conceptually and operationally.

---

## Suitability Test

### 1. Benchmark purity

Judgment:
- acceptable

Reason:
- the benchmark is clean sovereign government-bond exposure rather than a mixed policy-bank / credit bucket
- this aligns with the intended v1 role better than more complex fixed-income proxies

### 2. Duration character

Judgment:
- acceptable with caveat

Reason:
- 5-year duration is defensive and interpretable
- but it is less crisis-convex than 10Y or 30Y alternatives
- this is a trade-off, not a defect, for v1

### 3. Liquidity stability

Judgment:
- acceptable

Reason:
- current testing did not surface the continuity or thin-liquidity issues seen in `511020.SH`
- this matters more for v1 than maximizing duration expression

### 4. Missing-date behavior

Judgment:
- acceptable

Reason:
- no missing trade dates were found in the tested window

### 5. Role alignment

Judgment:
- acceptable with caveat

Reason:
- the instrument is sufficiently close to the intended `bond defense` role
- but it should still be treated as an instrument-specific duration proxy, not a perfect abstract “risk-off factor”

---

## Explicit Caveat

The correct caveat is not “data quality is weak.”

The correct caveat is:

> `511010.SH` is a clean and usable v1 bond-defense sleeve, but it is a 5-year sovereign duration proxy, not a maximally convex crisis hedge and not a universal bond factor.

That means:

- it should anchor the v1 defense leg
- it should not be over-narrated as the full truth of Chinese rate risk
- longer-duration or policy-bank alternatives remain future comparison tools, not the v1 default

---

## Final Sign-Off Decision

Decision:
- `acceptable with caveat`

Why not plain `acceptable as-is`:
- every bond sleeve carries some interpretation risk
- the strategy must remember this is a product-level exposure, not a pure theoretical duration factor

Why not `not acceptable`:
- both the earlier comparison work and the new data validation point in the same direction
- among current candidates, `511010.SH` remains the cleanest operational fit for v1

---

## V1 Implication

`511010.SH` is formally signed off as the v1 bond sleeve under the following understanding:

- it is the default defensive duration proxy
- it is preferred because of cleanliness, continuity, benchmark clarity, and stable tradability
- its lower convexity relative to longer-duration ETFs is an accepted v1 simplification, not a disqualifier
