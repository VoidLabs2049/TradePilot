# Stage 01 Data Reliability Test Report

## Mission Context

This report executes the first stage of the previously defined data reliability plan for the ETF all-weather strategy.

Stage 01 scope was limited to the minimum serious v1 field set:

1. trading calendar
2. sleeve price series
3. core macro slow fields
4. core rates / liquidity fields

This is a real test report, not a planning memo.

---

## Test Environment

- Repo environment: local workspace under `/Users/lhh/Github/The-One`
- Python runtime: project environment active
- Primary data client tested: `Tushare Pro`
- Secondary / fallback client tested: `AKShare`

---

## Tested Sleeve Proxies

The following concrete sleeves were used for Stage 01 testing:

| Sleeve Role | Instrument | Benchmark / Exposure |
|---|---|---|
| large-cap equity | `510300.SH` | 沪深300指数 |
| small-cap equity | `159845.SZ` | 中证1000指数收益率 |
| bond defense | `511020.SH` | 中证5-10年期国债活跃券指数收益率*100% |
| gold hedge | `518850.SH` | 上海黄金交易所黄金现货实盘合约 Au99.99 |

These were selected because they are legible, listed, and representative enough for v1 testing.

---

## Test Dimensions Used

Each field group was tested on the following dimensions where applicable:

1. source availability
2. fetch repeatability
3. identity / mapping
4. continuity / gap detection
5. duplicate-row detection
6. null / zero-value checks
7. cross-source agreement
8. timing semantics
9. strategy-role fitness

---

## Executive Verdict

### Passed cleanly

- `trade_cal` from Tushare
- `510300.SH` price series
- `159845.SZ` price series
- `518850.SH` price series
- `Shibor` from Tushare
- `LPR` from Tushare

### Passed with caveats

- `511020.SH` bond sleeve price series
- `cn_pmi`
- `cn_cpi`
- `cn_ppi`
- `cn_m`
- `AKShare` macro fallback for money supply
- `AKShare` Shibor fallback
- `AKShare` LPR fallback
- `ChinaBond yield curve` via `yc_cb`

### Failed or not acceptable as current primary fallback

- `AKShare` ETF spot / ETF history fallback in this environment

---

## Detailed Findings

## 1. Trading Calendar

### Tests Run

- Tushare availability test
- sample trade calendar fetch for `2026-01-01` to `2026-03-31`
- repeatability test (same query twice)

### Evidence

- `tushare_available = True`
- Q1 2026 open trading days returned: `56`
- repeatability:
  - `trade_cal_same_shape = True`
  - `same_columns = True`
  - `same_values = True`

### Verdict

`pass`

### Notes

- This is reliable enough to serve as the canonical trading calendar.
- No timing blocker found at this layer.

---

## 2. Sleeve Identity And Mapping

### Tests Run

- Tushare `fund_basic` lookup for candidate sleeves
- benchmark mapping check

### Evidence

- `510300.SH` -> `沪深300ETF华泰柏瑞` -> benchmark `沪深300指数`
- `159845.SZ` -> `中证1000ETF华夏` -> benchmark `中证1000指数收益率`
- `511020.SH` -> `国债ETF平安` -> benchmark `中证5-10年期国债活跃券指数收益率*100%`
- `518850.SH` -> `黄金ETF华夏` -> benchmark `上海黄金交易所黄金现货实盘合约Au99.99价格收益率`

### Verdict

`pass_with_caveat`

### Notes

- Identity mapping is clear enough for Stage 01.
- The main caveat is not naming ambiguity but bond-sleeve purity and liquidity, discussed below.

---

## 3. Sleeve Price Series Integrity

## 3.1 Tushare Availability And Coverage

### Tests Run

- `fund_daily` history fetch for `2022-01-01` to `2026-03-31`
- gap test against trade calendar
- duplicate-row test
- zero-volume test
- extreme-return sanity test
- repeatability test for `510300.SH`

### Evidence

#### Coverage

- `510300.SH`: `1025` rows, first `20220104`, last `20260331`, zero-close rows `0`
- `159845.SZ`: `1025` rows, first `20220104`, last `20260331`, zero-close rows `0`
- `511020.SH`: `1013` rows, first `20220104`, last `20260331`, zero-close rows `0`
- `518850.SH`: `1025` rows, first `20220104`, last `20260331`, zero-close rows `0`

#### Gap / duplicate / zero-volume

- `510300.SH`: missing days `0`, dupes `0`, zero_vol `0`
- `159845.SZ`: missing days `0`, dupes `0`, zero_vol `0`
- `511020.SH`: missing days `12`, dupes `0`, zero_vol `0`
- `518850.SH`: missing days `0`, dupes `0`, zero_vol `0`

`511020.SH` sample missing dates:
- `20220105`
- `20220307`
- `20220308`
- `20220407`
- `20220421`
- `20220609`
- `20220720`
- `20220721`

#### Extreme-return sanity

- `510300.SH`: max abs pct change `9.4081`, rows >5%: `4`
- `159845.SZ`: max abs pct change `10.0232`, rows >5%: `10`
- `511020.SH`: max abs pct change `0.7334`, rows >5%: `0`
- `518850.SH`: max abs pct change `10.0`, rows >5%: `5`

#### Bond sleeve liquidity warning

For `511020.SH`:
- rows with `vol < 10`: `55`
- rows with `vol < 100`: `135`
- rows with `amount < 1000`: `134`

#### Repeatability

- `fund_daily_same_shape = True`
- `same_columns = True`
- `same_values = True`

### Verdicts

- `510300.SH`: `pass`
- `159845.SZ`: `pass`
- `518850.SH`: `pass`
- `511020.SH`: `pass_with_caveat`

### Notes

- Equity and gold sleeves are clean enough for v1 historical work.
- The selected bond ETF is usable for research, but not yet trustworthy as a frictionless “pure duration defense” sleeve because:
  - it has `12` missing trade dates in the tested range
  - early-period liquidity is extremely thin
- Bond sleeve should carry an explicit exposure and liquidity caveat in any v1 model.

---

## 4. ETF Price Fallback Reliability

### Tests Run

- AKShare `fund_etf_spot_em()`
- AKShare `fund_etf_hist_em(symbol='510300', ...)`

### Evidence

Both calls failed in this environment with network-level `RemoteDisconnected` / `ConnectionError` from upstream Eastmoney-related requests.

### Verdict

`fail`

### Notes

- This does **not** mean AKShare is universally unusable.
- It means that in the current environment, AKShare ETF price endpoints are not reliable enough to be counted as the primary fallback for Stage 01 ETF price validation.
- For ETF sleeve history, Tushare is currently the only tested reliable source in this environment.

---

## 5. Macro Slow Fields

## 5.1 PMI

### Tests Run

- Tushare `cn_pmi(start_m='202401', end_m='202603')`
- duplicate test
- timing-field inspection

### Evidence

- rows: `26`
- key column: `MONTH`
- duplicates: `0`
- first period: `202401`
- last period: `202602`
- `release_date` field present: `False`
- `UPDATE_TIME` present: `True`

Observed timing fields:
- `MONTH`
- `UPDATE_TIME`
- `CREATE_TIME`

### Verdict

`pass_with_caveat`

### Notes

- The series is fetchable and continuous enough for research.
- It is **not** self-sufficient for point-in-time use because it does not provide a canonical `release_date` field.
- The model must attach an external release-calendar rule.
- Another caveat: schema readability is poor (`PMI010000` style coded columns), so field mapping should be locked down explicitly.

## 5.2 CPI

### Tests Run

- Tushare `cn_cpi(start_m='202401', end_m='202603')`
- duplicate test
- timing-field inspection

### Evidence

- rows: `27`
- key column: `month`
- duplicates: `0`
- first period: `202401`
- last period: `202603`
- `release_date` field present: `False`

### Verdict

`pass_with_caveat`

### Notes

- Structurally usable.
- Requires external release-date rules.
- Headline CPI should still be treated as lower-authority than core CPI or cross-confirmed inflation context.

## 5.3 PPI

### Tests Run

- Tushare `cn_ppi(start_m='202401', end_m='202603')`
- duplicate test
- timing-field inspection

### Evidence

- rows: `27`
- key column: `month`
- duplicates: `0`
- first period: `202401`
- last period: `202603`
- `release_date` field present: `False`

### Verdict

`pass_with_caveat`

### Notes

- Structurally usable.
- Requires explicit release-date mapping.
- Should be used as industrial/pricing pressure context, not standalone economy-wide inflation truth.

## 5.4 Money Supply (`cn_m`)

### Tests Run

- Tushare `cn_m(start_m='202401', end_m='202603')`
- duplicate test
- timing-field inspection
- repeatability test
- cross-source comparison with AKShare `macro_china_money_supply()`

### Evidence

- rows: `26`
- key column: `month`
- duplicates: `0`
- first period: `202401`
- last period: `202602`
- `release_date` field present: `False`
- repeatability:
  - `cn_m_same_shape = True`
  - `same_columns = True`
  - `same_values = True`

Cross-source comparison:

Tushare:
- `202601`: `m1=1179680.52`, `m1_yoy=4.9`, `m2=3471860.39`, `m2_yoy=9.0`
- `202602`: `m1=1159300.00`, `m1_yoy=5.9`, `m2=3492200.00`, `m2_yoy=9.0`

AKShare:
- `2026年01月份`: `m1=1179680.52`, `m1_yoy=4.9`, `m2=3471860.39`, `m2_yoy=9.0`
- `2026年02月份`: `m1=1159258.82`, `m1_yoy=5.9`, `m2=3492159.91`, `m2_yoy=9.0`
- `2026年03月份`: available in AKShare, while Tushare sample ended at `202602`

### Verdict

`pass_with_caveat`

### Notes

- This is one of the stronger slow fields structurally.
- Tushare and AKShare agree closely enough to support fallback confidence.
- The major risk is not fetchability but semantic drift after the 2025 M1 definition change.
- This field group must carry a `regime_note` before use in the model.

---

## 6. Rates And Liquidity Fields

## 6.1 Shibor

### Tests Run

- Tushare `shibor(start_date='20260101', end_date='20260331')`
- duplicate / null tests
- cross-source comparison with AKShare `macro_china_shibor_all()`

### Evidence

- rows: `59`
- duplicates: `0`
- null `1w`: `0`
- null `on`: `0`
- first date: `20260104`
- last date: `20260331`

Cross-source comparison on `2026-03-31`:

Tushare:
- `on=1.277`, `1w=1.438`, `2w=1.469`, `1m=1.495`, `3m=1.507`, `6m=1.520`, `9m=1.529`, `1y=1.538`

AKShare:
- `O/N=1.277`, `1W=1.438`, `2W=1.469`, `1M=1.495`, `3M=1.507`, `6M=1.520`, `9M=1.529`, `1Y=1.538`

### Verdict

`pass`

### Notes

- This is a strong field group for v1.
- Cross-source agreement was exact on the sampled date.

## 6.2 LPR

### Tests Run

- Tushare `shibor_lpr(start_date='20240101', end_date='20260331')`
- duplicate test
- cross-source comparison with AKShare `macro_china_lpr()`

### Evidence

- rows: `26`
- duplicates: `0`
- first date: `20240122`
- last date: `20260320`
- `release_date` field present: `False`, but the effective published decision date is directly represented in `date`

Cross-source comparison on `2026-03-20`:

Tushare:
- `1y=3.0`, `5y=3.5`

AKShare:
- `LPR1Y=3.0`, `LPR5Y=3.5`

### Verdict

`pass`

### Notes

- This field is cleaner than most monthly macro fields because the effective decision date is explicit.
- Good candidate for direct v1 use.

## 6.3 China Government Curve (`yc_cb`)

### Tests Run

- Tushare `yc_cb(ts_code='1001.CB', start_date='20260101', end_date='20260331')`
- exact-term extraction check for `1Y` and `10Y`
- raw distribution inspection

### Evidence

- returned rows: `2000`
- returned unique dates: `2`
- available `curve_type` values: `'0'`, `'1'`
- long date-range request was effectively truncated to the latest two dates because the row limit was reached

Sample raw rows showed valid curve points, including exact `1.00` year term on latest dates.

### Verdict

`pass_with_caveat`

### Notes

- The source is real and structurally useful.
- But the direct API usage is **not** ready for long historical extraction in its naïve form because:
  - date ranges can hit a row cap
  - curve points are dense, so one request can silently compress history
  - curve type handling must be explicit
- For v1, this can still be used if extraction is redesigned around smaller windows or paged logic.
- It is not yet “plug-and-play historical 10Y series.”

---

## 7. AKShare As Fallback Layer

### Summary Verdict

- ETF price fallback: `fail`
- macro money-supply fallback: `pass_with_caveat`
- Shibor fallback: `pass_with_caveat`
- LPR fallback: `pass_with_caveat`

### Interpretation

AKShare is not uniformly good or bad.

In this environment:
- it failed on ETF/Eastmoney-style endpoints
- it worked meaningfully on macro/rates fallback endpoints

So it should be treated as a selective fallback, not as a single reliability judgment.

---

## Field Group Status Table

| Field Group | Status | Why |
|---|---|---|
| trade calendar | `pass` | repeatable, consistent, no anomalies found |
| large-cap sleeve price | `pass` | full coverage, no gaps, no duplicate rows |
| small-cap sleeve price | `pass` | full coverage, no gaps, no duplicate rows |
| gold sleeve price | `pass` | full coverage, no gaps, no duplicate rows |
| bond sleeve price | `pass_with_caveat` | 12 missing dates and thin early liquidity |
| ETF metadata mapping | `pass_with_caveat` | mapping is clean, but sleeve purity still needs care |
| PMI | `pass_with_caveat` | fetchable and continuous, but no direct release-date field |
| CPI | `pass_with_caveat` | fetchable and continuous, but no direct release-date field |
| PPI | `pass_with_caveat` | fetchable and continuous, but no direct release-date field |
| money supply | `pass_with_caveat` | fetchable and cross-source supported, but semantic drift risk is high |
| Shibor | `pass` | clean continuity and exact cross-source match |
| LPR | `pass` | clean date semantics and exact cross-source match |
| China gov curve | `pass_with_caveat` | source exists, but naïve historical extraction is truncated |
| AKShare ETF fallback | `fail` | repeated connection failure on ETF endpoints |

---

## What This Means For The Strategy

### Safe To Use In v1 Immediately

- Tushare trade calendar
- Tushare sleeve price history for `510300.SH`, `159845.SZ`, `518850.SH`
- Tushare + AKShare-validated Shibor
- Tushare + AKShare-validated LPR

### Usable Only With Explicit Caveats

- `511020.SH` bond sleeve
- PMI / CPI / PPI / M1 / M2 / TSF family
- China bond curve extraction

### Not Reliable Enough As Current Price Fallback

- AKShare ETF history / spot endpoints in this environment

---

## Recommended Immediate Follow-Up

Before any schema hardening or backtest implementation:

1. add explicit `release_date_rule` metadata for all slow macro fields
2. add `regime_note` for M1/M2 family due to post-2025 semantic drift
3. either replace or explicitly caveat `511020.SH` as the initial bond sleeve
4. design a paged / windowed extraction method for `yc_cb` if curve fields are promoted into v1
5. do not rely on AKShare ETF endpoints as current fallback for sleeve history

---

## Closure Status

`implemented and verified`

This report reflects real fetches, real failures, and real pass/caveat/fail classifications for the Stage 01 minimum field set.
