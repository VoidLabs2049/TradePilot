# Stage B Ingestion Real Data Test Report

Date: 2026-04-26

## Scope

This report records the Stage B ETL real-data verification performed against the local TradePilot database and lakehouse.

Tested paths:

- `reference.instruments`
- `reference.trading_calendar`
- `market.etf_daily`
- `market.index_daily`
- raw landing to Parquet
- normalized partition rewrite and dedupe
- dependency preflight, validation gating, and watermark advancement

## Environment

- Branch: `stage-b-design-update`
- Database: `data/tradepilot.duckdb`
- Lakehouse root: `data/lakehouse`
- Tushare config: `.env` contains `TUSHARE_TOKEN`; token value was not printed or committed.
- Test command after fixes: `python -m pytest -q`
- Final automated test result: `28 passed`

## Summary

Stage B can ingest real Tushare data for both ETF daily and index daily paths after two compatibility fixes:

1. Tushare `fund_basic` includes some 7-digit delisted fund codes such as `5012011.SH`. Stage B canonical instruments are intentionally limited to six-digit SH/SZ codes, so these rows are retained in raw Parquet but filtered out before canonical validation.
2. Tushare `index_basic` may not return `delist_date`. The client now fills the missing column with `None` before returning the catalog DataFrame.

Final real-data results:

- ETF daily sync succeeded for `510300.SH` on `2024-04-24`.
- ETF daily sync succeeded for `159915.SZ` on `2024-04-25`.
- Re-running `159915.SZ` on `2024-04-25` updated the existing business key without producing duplicates.
- Index daily sync succeeded for `000300.SH` on `2024-04-26`.
- Watermarks advanced for `reference.instruments`, `reference.trading_calendar`, `market.etf_daily`, and `market.index_daily`.

## Test Runs

### 1. No-token safety check

Before `.env` was available, `TUSHARE_TOKEN_SET=false`.

Result:

- `market.etf_daily` run `1`: failed at dependency preflight.
- `reference.instruments` run `2`: failed with `source_contract.empty_payload`.
- `reference.trading_calendar` run `3`: failed with `source_contract.empty_payload`.
- No canonical rows were written.
- No watermark was advanced.
- Empty raw Parquet batches were preserved for audit.

Conclusion: empty source payloads are gated correctly and do not silently advance canonical state.

### 2. ETF daily, SH instrument

Request:

- Dataset: `market.etf_daily`
- Instrument: `510300.SH`
- Date: `2024-04-24`

Initial result:

- `reference.instruments` fetched `2560` raw rows.
- Validation failed on `21` rows with 7-digit source codes, for example `5012011.SH`.

Fix applied:

- `InstrumentNormalizer` filters canonical instruments to Stage B supported six-digit SH/SZ codes.
- Raw source data remains unchanged.

Successful rerun:

- `market.etf_daily` run `7`: success
- `reference.instruments` run `8`: success
- ETF canonical instruments written: `2539`
- Normalized file: `data/lakehouse/normalized/market.etf_daily/year=2024/month=04/part-00000.parquet`

Sample normalized row:

| instrument_id | trade_date | open | high | low | close | volume | amount | quality_status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `510300.SH` | `2024-04-24` | 3.505 | 3.517 | 3.489 | 3.513 | 5942319.08 | 2081789.894 | `pass` |

### 3. ETF daily, SZ instrument

Request:

- Dataset: `market.etf_daily`
- Instrument: `159915.SZ`
- Date: `2024-04-25`

Result:

- `market.etf_daily` run `9`: success
- `reference.trading_calendar` run `10`: success
- Validation failures: none
- `canonical_instruments` contains `159915.SZ` with `exchange=SZ`.
- `canonical_trading_calendar` contains `SZ / 2024-04-25`.
- Normalized month partition contains `2` rows total after this run.

Sample normalized row:

| instrument_id | trade_date | open | high | low | close | volume | amount | quality_status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `159915.SZ` | `2024-04-25` | 1.710 | 1.730 | 1.700 | 1.716 | 5916389.77 | 1015097.836 | `pass` |

Conclusion: exchange-aware calendar validation works for SZ instruments.

### 4. ETF daily duplicate business key rewrite

Request:

- Dataset: `market.etf_daily`
- Instrument: `159915.SZ`
- Date: `2024-04-25`
- Same request as run `9`

Result:

- `market.etf_daily` run `11`: success
- `records_inserted=0`
- `records_updated=1`
- Normalized Parquet total rows remained `2`.
- Duplicate `(instrument_id, trade_date)` count: `0`.

Conclusion: deterministic partition rewrite and business-key dedupe work for repeated syncs.

### 5. Index daily

Request:

- Dataset: `market.index_daily`
- Instrument: `000300.SH`
- Date: `2024-04-26`

Initial result:

- `market.index_daily` run `12`: failed at dependency preflight.
- `reference.instruments` run `13`: failed because Tushare `index_basic` did not include `delist_date`.

Fix applied:

- `TushareClient.get_index_catalog()` now fills missing `delist_date` with `None`.

Successful rerun:

- `market.index_daily` run `15`: success
- `reference.instruments` run `16`: success
- Index raw instruments fetched: `1041`
- Index canonical instruments written: `732`
- Normalized file: `data/lakehouse/normalized/market.index_daily/year=2024/month=04/part-00000.parquet`
- Duplicate `(instrument_id, trade_date)` count: `0`

Sample normalized row:

| instrument_id | trade_date | open | high | low | close | volume | amount | quality_status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `000300.SH` | `2024-04-26` | 3531.9647 | 3588.4003 | 3531.9647 | 3584.2686 | 197216133.0 | 308045100.0 | `pass` |

## Final Watermarks

| dataset_name | latest_fetched_date | latest_successful_run_id |
| --- | --- | ---: |
| `market.etf_daily` | `2024-04-25` | 11 |
| `market.index_daily` | `2024-04-26` | 15 |
| `reference.instruments` | `2026-04-26` | 16 |
| `reference.trading_calendar` | `2024-04-26` | 14 |

## Code Changes From Testing

Real-data testing produced these implementation changes:

- `tradepilot/etl/normalizers.py`
  - Filter Stage B canonical instruments to six-digit SH/SZ codes.
  - Preserve unsupported source rows in raw Parquet instead of canonicalizing them.
- `tradepilot/data/tushare_client.py`
  - Fill missing `delist_date` for index catalog responses.
- `tests/etl/test_stage_b.py`
  - Add coverage for filtering unsupported 7-digit fund codes.

## Data Artifacts

Generated local artifacts:

- Raw Parquet files under `data/lakehouse/raw/`
- Normalized Parquet files under `data/lakehouse/normalized/`
- Metadata and canonical rows in `data/tradepilot.duckdb`

These artifacts are local run outputs and are not intended to be committed.

## Conclusion

Stage B is able to ingest real Tushare reference, ETF daily, and index daily data through the first executable vertical slice. Validation gating, raw retention, exchange-aware dependency checks, normalized Parquet writes, dedupe-on-rewrite, and watermark advancement behaved as expected after the compatibility fixes above.
