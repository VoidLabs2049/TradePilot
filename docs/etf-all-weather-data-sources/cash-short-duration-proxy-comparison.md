# Cash / Short-Duration Proxy Comparison For V1

## Purpose

With the bond sleeve now narrowed toward `511010.SH`, the remaining v1 asset-choice question is:

> What should serve as the cash / short-duration buffer sleeve?

This sleeve is not meant to generate return.
Its primary roles are:

- liquidity buffer
- low-volatility parking sleeve
- rebalance ammunition
- neutral anchor when regime confidence is low

---

## Candidates Tested

| Code | Name | Benchmark | List Date |
|---|---|---|---|
| `159001.SZ` | 货币ETF易方达 | 活期存款基准利率*(1-利息税税率) | 2014-10-20 |
| `511800.SH` | 易方达货币ETF | 活期存款利率(税后) | 2014-12-08 |
| `511860.SH` | 货币ETF博时 | 七天通知存款利率(税后) | 2014-12-09 |
| `511810.SH` | 货币ETF南方 | 同期中国人民银行公布的七天通知存款利率(税后) | 2015-01-05 |
| `511360.SH` | 短融ETF海富通 | 中证短融指数收益率 | 2020-09-25 |
| `511160.SH` | 国债ETF东财 | 中证1-3年国债指数收益率 | 2025-01-06 |

---

## Comparison Standard

Candidates were judged on:

1. history coverage
2. missing trade days
3. liquidity cleanliness
4. volatility suppression
5. role purity as a `cash / near-cash` sleeve

The v1 requirement is conservative:

> choose the cleanest low-volatility parking sleeve, not the one with the most carry.

---

## Coverage And Liquidity Comparison

Window tested: `2022-01-01` to `2026-03-31`

| Code | Rows | Missing Trade Days | Low Vol <10 | Low Vol <100 | Low Amount <1000 |
|---|---:|---:|---:|---:|---:|
| `511800.SH` | 1025 | 0 | 0 | 47 | 47 |
| `511860.SH` | 1020 | 5 | 65 | 388 | 388 |
| `511810.SH` | 1025 | 0 | 0 | 0 | 0 |
| `159001.SZ` | 1025 | 0 | 0 | 0 | 0 |
| `511360.SH` | 1025 | 0 | 0 | 0 | 0 |
| `511160.SH` | 297 | 728 | 0 | 0 | 0 |

### Immediate Interpretation

- `159001.SZ` and `511810.SH` are the cleanest operationally.
- `511800.SH` is acceptable but has some low-liquidity observations under the test thresholds.
- `511860.SH` is materially worse on continuity and thin-trade rows.
- `511360.SH` is liquid, but it is a short-credit instrument rather than a pure cash proxy.
- `511160.SH` is too new to serve as the root v1 cash sleeve.

---

## Behavior Comparison

Window tested: `2022-01-01` to `2026-03-31`

| Code | Max Abs Daily Pct | Std of Daily Pct |
|---|---:|---:|
| `511800.SH` | 0.3259 | 0.0176 |
| `511860.SH` | 0.2669 | 0.0147 |
| `511810.SH` | 0.0839 | 0.0096 |
| `159001.SZ` | 0.0110 | 0.0012 |
| `511360.SH` | 1.0470 | 0.0484 |
| `511160.SH` | 0.1935 | 0.0367 |

### Interpretation

- `159001.SZ` is by far the most cash-like in behavior.
- `511810.SH` is also strong, but still slightly more active than `159001.SZ`.
- `511360.SH` is too risk-bearing to be the primary cash sleeve.
- `511160.SH` looks reasonable as a short-duration rate proxy, but its listing history is far too short.

---

## Candidate Judgments

## 1. `159001.SZ` — 货币ETF易方达

### Strengths

- full coverage in the test window
- zero missing trade days
- zero low-liquidity flags under the chosen thresholds
- the calmest volatility profile by far
- strongest fit for a true `parking sleeve`

### Weaknesses

- behaves more like cash than a duration asset, so it offers little rate-convexity
- may slightly understate what a very short-duration bond sleeve could contribute in a falling-rate environment

### Judgment

Best default `v1 cash / neutral buffer sleeve`.

## 2. `511810.SH` — 货币ETF南方

### Strengths

- full coverage
- clean liquidity profile
- stable and low-volatility

### Weaknesses

- slightly less cash-like than `159001.SZ`
- benchmark tied to 7-day notice deposit rate rather than the most minimal cash expression

### Judgment

Best backup candidate.

## 3. `511800.SH` — 易方达货币ETF

### Judgment

Acceptable, but weaker than `159001.SZ` and `511810.SH` under the current tests.

## 4. `511860.SH` — 货币ETF博时

### Judgment

Not preferred due to thin-trade observations and a few missing days.

## 5. `511360.SH` — 短融ETF海富通

### Strengths

- full coverage
- no liquidity red flags under current thresholds

### Weaknesses

- benchmark is short-credit exposure, not pure cash
- volatility is meaningfully above the money ETF set

### Judgment

Useful later as a `carry-enhanced low-risk sleeve` candidate, but not as the default v1 neutral cash buffer.

## 6. `511160.SH` — 国债ETF东财

### Judgment

Too new for current v1 role despite reasonable behavior.

---

## Recommendation

### Primary Recommendation

Use `159001.SZ` as the `v1 cash / short-duration proxy`.

### Why

Because it is the cleanest answer to the actual v1 requirement:

- preserve capital-like behavior
- keep volatility near zero
- remain operationally clean and liquid enough
- serve as rebalance ammunition rather than as a hidden extra risk sleeve

### Secondary Recommendation

Keep `511810.SH` as the backup if exchange, implementation, or brokerage constraints make `159001.SZ` less convenient.

### Explicit Non-Recommendation

Do not use `511360.SH` as the default cash sleeve.

It is a valid low-risk instrument, but it is not a true neutral parking sleeve; it introduces extra credit-spread behavior into a part of the portfolio that is supposed to stay simple.

---

## Operational Consequence

If this recommendation is accepted, the v1 sleeve set becomes:

1. `510300.SH` — large-cap equity
2. `159845.SZ` — small-cap equity
3. `511010.SH` — bond defense
4. `518850.SH` — gold hedge
5. `159001.SZ` — cash / neutral buffer

---

## Final Judgment

For v1, the cash sleeve should maximize `clean neutrality`, not yield ambition.

On current evidence, `159001.SZ` is the best fit.
