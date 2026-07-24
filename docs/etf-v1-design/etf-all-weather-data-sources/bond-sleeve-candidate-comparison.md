# Bond Sleeve Candidate Comparison For V1

## Purpose

Stage 01 testing showed that the initial bond sleeve choice, `511020.SH`, is usable but not ideal because of missing dates and thin early liquidity.

This note compares practical bond-ETF candidates for the v1 all-weather bond sleeve.

The goal is not to choose the “best-performing bond ETF.”
The goal is to choose the most suitable `duration defense proxy` for a transparent monthly allocation engine.

---

## Comparison Standard

Candidates were judged on four dimensions:

1. history coverage
2. missing trading days
3. liquidity sanity
4. benchmark purity relative to the intended sleeve role

The intended sleeve role is:

> a stable, liquid-enough domestic defensive duration proxy that can serve as the bond leg in a simple monthly all-weather system.

---

## Candidates Tested

| Code | Name | Benchmark | List Date |
|---|---|---|---|
| `511010.SH` | 国债ETF国泰 | 上证5年期国债指数收益率 | 2013-03-25 |
| `511020.SH` | 国债ETF平安 | 中证5-10年期国债活跃券指数收益率*100% | 2019-02-22 |
| `511520.SH` | 政金债ETF富国 | 中债7-10年政策性金融债指数收益率 | 2022-10-25 |
| `511580.SH` | 国债政金债ETF招商 | 中证国债及政策性金融债0-3年指数收益率 | 2022-12-14 |
| `511090.SH` | 30年国债ETF鹏扬 | 中债-30年期国债指数收益率 | 2023-06-13 |
| `511100.SH` | 国债ETF华夏 | 上证基准做市国债指数收益率 | 2023-12-25 |
| `511130.SH` | 30年国债ETF博时 | 上证30年期国债指数收益率 | 2024-03-28 |

---

## Coverage And Liquidity Comparison

Window tested: `2022-01-01` to `2026-03-31`

| Code | Rows | Missing Trade Days | Low Vol <10 | Low Vol <100 | Low Amount <1000 |
|---|---:|---:|---:|---:|---:|
| `511010.SH` | 1025 | 0 | 0 | 0 | 0 |
| `511020.SH` | 1013 | 12 | 55 | 135 | 134 |
| `511100.SH` | 546 | 479 | 0 | 1 | 1 |
| `511130.SH` | 485 | 540 | 0 | 0 | 0 |
| `511520.SH` | 832 | 193 | 0 | 0 | 0 |
| `511580.SH` | 790 | 235 | 0 | 23 | 18 |
| `511090.SH` | 677 | 348 | 0 | 0 | 0 |

### Immediate Interpretation

- `511010.SH` is the cleanest candidate by far in coverage and liquidity continuity.
- `511020.SH` has acceptable history length but noticeably worse continuity and early liquidity.
- newer ETFs may have good liquidity now, but they shorten the backtest window too much for v1.

---

## Behavior Comparison

Window tested: `2024-01-01` to `2026-03-31`

| Code | Mean Abs Daily Pct | Max Abs Daily Pct | Std of Daily Pct |
|---|---:|---:|---:|
| `511010.SH` | 0.0687 | 0.4513 | 0.0971 |
| `511020.SH` | 0.0921 | 0.7334 | 0.1324 |
| `511520.SH` | 0.1127 | 1.1465 | 0.1609 |
| `511090.SH` | 0.3328 | 2.8727 | 0.4627 |
| `511130.SH` | 0.3276 | 2.3359 | 0.4541 |

### Interpretation

- `511010.SH` is the calmest and most defensive candidate.
- `511020.SH` is still moderate, but more volatile and less clean operationally.
- `511520.SH` is acceptable if policy-bank exposure is desired, but it is not as long-lived historically.
- `511090.SH` and `511130.SH` are much more duration-sensitive and volatile; they behave more like aggressive long-duration conviction sleeves than neutral v1 defense legs.

---

## Correlation Snapshot

Daily `pct_chg` correlation, overlapping sample from `2024-01-01` onward:

| Pair | Approx Interpretation |
|---|---|
| `511010` vs `511020` = `0.857` | strong overlap |
| `511020` vs `511520` = `0.881` | strong overlap |
| `511520` vs `511090` = `0.902` | strong overlap, but different duration sensitivity |
| `511090` vs `511130` = `0.985` | near-substitute long-duration pair |

### Interpretation

- The candidates all sit in the same broad rates family, but not at the same risk level.
- The real choice is not “which one is bond-like,” but “what duration sensitivity level does v1 want?”

---

## Candidate Judgments

## 1. `511010.SH` — 国债ETF国泰

### Strengths

- longest listed history in this candidate set
- zero missing trade days in test window
- zero obvious low-liquidity flags under the test thresholds
- benchmark is clean and interpretable
- behavior is stable and low-volatility

### Weaknesses

- 5-year government bond exposure is less crisis-convex than 10Y or 30Y sleeves
- may understate the potential upside of a strong duration rally

### Judgment

Best default `v1 bond defense sleeve`.

It is not the most expressive duration bet, but it is the cleanest operational proxy.

## 2. `511020.SH` — 国债ETF平安

### Strengths

- benchmark is still interpretable and closer to intermediate duration
- historically available across the test window

### Weaknesses

- 12 missing trade days in tested range
- thin early liquidity
- less operationally clean than `511010.SH`

### Judgment

Reasonable backup candidate, but no longer the preferred v1 choice.

## 3. `511520.SH` — 政金债ETF富国

### Strengths

- decent current liquidity
- 7-10Y policy-bank exposure can behave defensively with somewhat stronger duration sensitivity

### Weaknesses

- shorter listed history
- policy-bank exposure is not identical to sovereign government-bond exposure

### Judgment

Good `v2 alternative` if we later want a stronger or broader fixed-income defense leg.

## 4. `511090.SH` / `511130.SH` — 30Y 国债 ETF

### Strengths

- very clear duration expression
- high convexity to rate declines

### Weaknesses

- much more volatile
- shorter history
- less suitable as a simple neutral defensive sleeve

### Judgment

Not ideal for v1 base sleeve.
Better treated as later optional duration-expression tools.

## 5. `511580.SH` / `511100.SH`

### Judgment

- `511580.SH` mixes government and policy-bank short-end exposure; useful but not the cleanest duration-defense root sleeve.
- `511100.SH` is too new for our current backtest window.

---

## Recommendation

### Primary Recommendation

Use `511010.SH` as the `v1 bond sleeve`.

### Why

Because v1 needs:

- stable history
- low missingness
- adequate liquidity continuity
- interpretable benchmark exposure

and `511010.SH` dominates on those criteria.

### Secondary Recommendation

Keep `511520.SH` as the most interesting `v2 alternative` if we later want to compare:

- government-bond defense
- policy-bank / longer-duration defense

### Explicit Non-Recommendation

Do not use `511090.SH` or `511130.SH` as the first default bond sleeve.

They are valid instruments, but they are too duration-aggressive for a simple, explainable v1 defense leg.

---

## Operational Consequence

If we accept this recommendation, the v1 sleeve set becomes:

1. `510300.SH` — large-cap equity
2. `159845.SZ` — small-cap equity
3. `511010.SH` — bond defense
4. `518850.SH` — gold hedge
5. cash / short-duration proxy

---

## Final Judgment

The correct v1 bond choice is not the most exciting fixed-income ETF.

It is the one that most cleanly serves the role of `stable defensive duration proxy`.

On current evidence, that is `511010.SH`.
