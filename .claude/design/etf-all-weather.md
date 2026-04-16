---
title: "ETF All Weather"
status: draft
mode: "design"
created: 2026-04-16
updated: 2026-04-16
modules: ["backend", "frontend"]
---

# ETF All Weather

## Overview

Design a new full-stack ETF all-weather capability for TradePilot. The feature should let users review and act on an ETF-based all-weather strategy inside the existing decision-support workflow, while preserving current backend/frontend API contracts wherever practical.

The initial design goal is to introduce this capability as an additive slice rather than a disruptive platform rewrite. The design should identify the minimum new data, analysis, and UI surfaces needed to make the strategy useful without destabilizing the existing daily workflow experience.

## Goals

- [ ] Add an ETF all-weather capability that does not exist today
- [ ] Fit the feature into the current full-stack architecture with minimal contract churn
- [ ] Define clear backend and frontend interfaces for strategy inputs, outputs, and presentation
- [ ] Phase delivery so the first increment is small but still useful

## Constraints

- Preserve existing APIs where possible
- Prefer additive changes over breaking redesigns
- Keep the first phase tightly scoped
- Reuse current workflow and dashboard patterns where they are a good fit

## Scope

### Modules Involved

| Module |
|--------|
| Backend |
| Frontend |

### Key Files

| File | Planned Role |
|------|--------------|
| `tradepilot/api/workflow.py` | Candidate additive API surface if ETF all-weather is exposed as workflow-compatible insight/context |
| `tradepilot/api/market.py` | Existing market data API surface that already exposes ETF-oriented endpoints and may host additive read APIs |
| `webapp/src/pages/Dashboard/index.tsx` | Current primary product surface; best default landing zone for a first additive view |
| `webapp/src/services/api.ts` | Typed frontend contract layer; stable extension point for new feature reads |
| `docs/etf-all-weather-implementation/implementation-blueprint.md` | Research-to-implementation guidance for MVP boundaries and sequencing |

### Out of Scope

- Replacing the existing workflow architecture
- Large-scale redesign of unrelated portfolio or briefing features
- Supporting every ETF strategy variant in the first phase

## Design

### Architecture

The current system is workflow-first on the frontend and already supports additive insight/context payloads on the backend. For a first ETF all-weather capability, the lowest-disruption architecture is to treat the feature as a strategy-specific read model that plugs into the existing backend service layer and is rendered either inside Dashboard or through a closely related additive page.

A likely target shape is:

1. data/research layer prepares ETF sleeve inputs and monthly strategy outputs
2. backend service layer assembles an ETF all-weather snapshot
3. API layer exposes a stable read endpoint or workflow-compatible payload extension
4. frontend renders a compact strategy summary first, with drill-down for sleeve weights, rationale, and rebalance state

### Interfaces

Initial interface boundary should stay read-only.

Candidate response areas:
- strategy metadata: strategy date, rebalance date, status, schema version
- portfolio stance: target sleeve weights, neutral weights, tilt direction, cash posture
- explanation layer: regime summary, confidence, driving signals, risk notes
- diagnostics: input freshness, missing data flags, last successful calculation

### Data Flow

Likely first-pass product data flow:
1. stage-1 ingestion pipeline lands raw ETF all-weather source data
2. normalization layer builds strategy-ready canonical tables with timing metadata
3. later feature builders assemble monthly as-of snapshots
4. strategy layer computes allocation snapshots
5. frontend displays summary plus explanation in the workflow-first shell

### Stage 1 — Data Ingestion And Storage Architecture

#### Stage-1 objective

The first engineering milestone is not the allocation engine itself. It is a trustworthy `raw -> normalized` ingestion boundary for the frozen v1 field set documented under `docs/etf-all-weather-data-sources/`.

Stage 1 should deliver:
- canonical source adapters for the frozen v1 fields
- immutable raw landing storage
- normalized queryable tables with explicit timing semantics
- quality checks and ingestion run traceability
- storage layout that can hold long history without turning the main DuckDB file into an opaque dumping ground

Stage 1 should explicitly not deliver yet:
- full backtest logic
- allocation optimization
- portfolio writeback
- broad v2/v3 field expansion

#### Recommended pipeline shape

1. **Extract**
   - pull from the chosen primary source per dataset family
   - keep fallback source support explicit, not implicit
   - fetch in bounded windows for large or fragile endpoints

2. **Raw land**
   - persist the untouched fetched payload into immutable raw files
   - record fetch metadata: source, endpoint family, request window, fetched_at, row count, checksum, run_id
   - never make notebooks or downstream loaders depend on live source fetches

3. **Normalize**
   - map source-specific columns into canonical field names
   - standardize code formats, dates, numeric types, null rules, and adjustment-aware return basis requirements
   - attach field-level timing metadata for slow fields

4. **Validate**
   - continuity checks, duplicate checks, null/zero anomaly checks, release-date/effective-date checks, code-to-sleeve mapping checks
   - mark records with quality status instead of silently dropping ambiguous cases

5. **Load curated layer**
   - upsert canonical normalized tables used by later feature builders
   - maintain run lineage from normalized rows back to raw batch metadata

6. **Publish feature-ready views later**
   - stage 1 stops at normalized storage plus minimal read models
   - wide monthly feature tables belong to the derived layer, not the raw/normalized layer

#### Recommended storage strategy for large data

Use a `hybrid local lakehouse` model:

- **Parquet on filesystem** for raw immutable history and large append-only normalized facts
- **DuckDB** for metadata tables, ingestion manifests, quality results, and query-serving views/tables

This fits the current repo because:
- the app already uses DuckDB in `tradepilot/db.py`
- DuckDB can query Parquet directly
- raw historical data stays reprocessable without bloating a single database file
- the same logical layout can later move from local disk to mounted external storage/object storage with minimal contract change

#### Physical storage layout

Recommended root:
- `data/etf_all_weather/`

Recommended zones:
- `data/etf_all_weather/raw/`
- `data/etf_all_weather/normalized/`
- `data/etf_all_weather/derived/`

Recommended partition strategy:
- raw daily market/rates data: partition by `dataset/year/month`
- raw monthly macro data: partition by `dataset/year`
- normalized daily facts: partition by `dataset/year/month`
- normalized slow fields: partition by `field_name/year`
- derived monthly feature tables: partition by `rebalance_year`

Avoid over-partitioning by instrument code in v1 because the canonical sleeve set is small and too many tiny files will hurt local performance.

#### Canonical dataset groups

1. **Reference / dimensions**
   - canonical sleeve definitions
   - instrument metadata
   - source registry
   - trading calendar and rebalance calendar

2. **Daily market facts**
   - ETF sleeve daily adjusted-price basis data
   - index daily data for confirmation series
   - daily rates/liquidity series such as Shibor and government yields

3. **Slow macro facts**
   - one canonical long-form table for macro/rates fields with `field_name`, `period_label`, `value`, `release_date`, `effective_date`, `revision_note`, `definition_regime`

4. **Pipeline metadata**
   - ingestion runs
   - raw file manifests
   - validation results
   - source freshness / watermark state

5. **Derived later**
   - monthly as-of feature snapshots
   - explainability-ready strategy inputs

#### Stage-1 schema blueprint

##### A. DuckDB metadata tables

**1. `etf_aw_ingestion_runs`**
- role: one row per job execution
- key: `run_id`
- core fields:
  - `run_id`
  - `job_name`
  - `dataset_name`
  - `source_name`
  - `trigger_mode`
  - `status`
  - `started_at`
  - `finished_at`
  - `request_start`
  - `request_end`
  - `partitions_written`
  - `records_discovered`
  - `records_inserted`
  - `records_updated`
  - `records_failed`
  - `error_message`
  - `code_version`

**2. `etf_aw_raw_batches`**
- role: manifest for immutable raw landed files
- key: `raw_batch_id`
- core fields:
  - `raw_batch_id`
  - `run_id`
  - `dataset_name`
  - `source_name`
  - `source_endpoint`
  - `storage_path`
  - `file_format`
  - `compression`
  - `partition_year`
  - `partition_month`
  - `window_start`
  - `window_end`
  - `row_count`
  - `content_hash`
  - `fetched_at`
  - `schema_version`
  - `is_fallback_source`

**3. `etf_aw_validation_results`**
- role: quality-check output by run/batch/dataset
- key: `validation_id`
- core fields:
  - `validation_id`
  - `run_id`
  - `raw_batch_id`
  - `dataset_name`
  - `check_name`
  - `check_level`
  - `status`
  - `subject_key`
  - `metric_value`
  - `threshold_value`
  - `details_json`
  - `created_at`

**4. `etf_aw_source_watermarks`**
- role: remember latest successful fetch boundary per dataset/source
- key: `(dataset_name, source_name)`
- core fields:
  - `dataset_name`
  - `source_name`
  - `latest_available_date`
  - `latest_fetched_date`
  - `latest_successful_run_id`
  - `updated_at`

##### B. Reference datasets

**5. `canonical_sleeves`**
- role: frozen v1 sleeve registry
- key: `sleeve_code`
- fields:
  - `sleeve_code`
  - `sleeve_role`
  - `sleeve_name`
  - `listing_exchange`
  - `benchmark_name`
  - `list_date`
  - `exposure_note`
  - `is_active`

**6. `canonical_instruments`**
- role: broader instrument reference for sleeves and confirmation indexes
- key: `instrument_code`
- fields:
  - `instrument_code`
  - `instrument_name`
  - `instrument_type`
  - `exchange`
  - `benchmark_name`
  - `list_date`
  - `delist_date`
  - `source_name`
  - `metadata_json`

**7. `canonical_trading_calendar`**
- role: canonical open-day table
- key: `(exchange, trade_date)`
- fields:
  - `exchange`
  - `trade_date`
  - `is_open`
  - `pretrade_date`
  - `calendar_source`

**8. `canonical_rebalance_calendar`**
- role: materialized monthly decision schedule
- key: `rebalance_date`
- fields:
  - `rebalance_date`
  - `calendar_month`
  - `rule_name`
  - `anchor_day`
  - `previous_rebalance_date`
  - `calendar_source`

##### C. Normalized fact datasets

**9. `canonical_daily_market_fact`**
- storage: Parquet partitions, queryable from DuckDB
- grain: one instrument per trade date
- key: `(instrument_code, trade_date)`
- fields:
  - `instrument_code`
  - `trade_date`
  - `open`
  - `high`
  - `low`
  - `close`
  - `adj_close`
  - `pct_chg`
  - `adj_pct_chg`
  - `vol`
  - `amount`
  - `source_name`
  - `source_trade_date`
  - `raw_batch_id`
  - `quality_status`

**10. `canonical_daily_rates_fact`**
- storage: Parquet partitions
- grain: one field per trade date
- key: `(field_name, trade_date, source_name)`
- fields:
  - `field_name`
  - `trade_date`
  - `value`
  - `unit`
  - `source_name`
  - `raw_batch_id`
  - `revision_note`
  - `quality_status`

**11. `canonical_slow_field_fact`**
- storage: Parquet partitions
- grain: one field per period label per source
- key: `(field_name, period_label, source_name)`
- fields:
  - `field_name`
  - `period_label`
  - `period_type`
  - `value`
  - `unit`
  - `release_date`
  - `effective_date`
  - `revision_note`
  - `definition_regime`
  - `regime_note`
  - `source_name`
  - `raw_batch_id`
  - `quality_status`

**12. `canonical_curve_fact`**
- storage: Parquet partitions
- grain: one tenor point per curve date
- key: `(curve_code, curve_date, tenor_years, source_name)`
- fields:
  - `curve_code`
  - `curve_date`
  - `curve_type`
  - `tenor_years`
  - `yield_value`
  - `source_name`
  - `raw_batch_id`
  - `quality_status`

##### D. Derived datasets for next stage

**13. `monthly_feature_snapshot`**
- storage: Parquet partitions
- grain: one rebalance date
- deferred to next stage, but reserved now
- key: `rebalance_date`

#### DDL-level storage specification

##### Storage placement matrix

| Dataset | Physical home | Why |
|--------|---------------|-----|
| `etf_aw_ingestion_runs` | DuckDB table | small, transactional metadata |
| `etf_aw_raw_batches` | DuckDB table | manifest and lineage lookup |
| `etf_aw_validation_results` | DuckDB table | quality audit and debugging |
| `etf_aw_source_watermarks` | DuckDB table | incremental sync control |
| `canonical_sleeves` | DuckDB table | tiny dimension, frequently joined |
| `canonical_instruments` | DuckDB table | small reference dimension |
| `canonical_trading_calendar` | DuckDB table | foundational lookup table |
| `canonical_rebalance_calendar` | DuckDB table | foundational monthly lookup |
| `canonical_daily_market_fact` | Parquet dataset | larger append-heavy history |
| `canonical_daily_rates_fact` | Parquet dataset | append-heavy daily time series |
| `canonical_slow_field_fact` | Parquet dataset | heterogeneous long-form slow fields |
| `canonical_curve_fact` | Parquet dataset | potentially large dense point set |
| `monthly_feature_snapshot` | Parquet dataset | rebuildable derived layer |

##### DuckDB DDL targets

**1. `etf_aw_ingestion_runs`**

```sql
CREATE TABLE IF NOT EXISTS etf_aw_ingestion_runs (
    run_id BIGINT PRIMARY KEY,
    job_name VARCHAR NOT NULL,
    dataset_name VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    trigger_mode VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    request_start DATE,
    request_end DATE,
    partitions_written INTEGER DEFAULT 0,
    records_discovered BIGINT DEFAULT 0,
    records_inserted BIGINT DEFAULT 0,
    records_updated BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    error_message TEXT,
    code_version VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**2. `etf_aw_raw_batches`**

```sql
CREATE TABLE IF NOT EXISTS etf_aw_raw_batches (
    raw_batch_id BIGINT PRIMARY KEY,
    run_id BIGINT NOT NULL,
    dataset_name VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    source_endpoint VARCHAR,
    storage_path VARCHAR NOT NULL,
    file_format VARCHAR NOT NULL,
    compression VARCHAR,
    partition_year INTEGER,
    partition_month INTEGER,
    window_start DATE,
    window_end DATE,
    row_count BIGINT DEFAULT 0,
    content_hash VARCHAR,
    fetched_at TIMESTAMP NOT NULL,
    schema_version VARCHAR NOT NULL,
    is_fallback_source BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_name, source_name, storage_path)
);
```

**3. `etf_aw_validation_results`**

```sql
CREATE TABLE IF NOT EXISTS etf_aw_validation_results (
    validation_id BIGINT PRIMARY KEY,
    run_id BIGINT NOT NULL,
    raw_batch_id BIGINT,
    dataset_name VARCHAR NOT NULL,
    check_name VARCHAR NOT NULL,
    check_level VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    subject_key VARCHAR,
    metric_value DOUBLE,
    threshold_value DOUBLE,
    details_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**4. `etf_aw_source_watermarks`**

```sql
CREATE TABLE IF NOT EXISTS etf_aw_source_watermarks (
    dataset_name VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    latest_available_date DATE,
    latest_fetched_date DATE,
    latest_successful_run_id BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_name, source_name)
);
```

**5. `canonical_sleeves`**

```sql
CREATE TABLE IF NOT EXISTS canonical_sleeves (
    sleeve_code VARCHAR PRIMARY KEY,
    sleeve_role VARCHAR NOT NULL,
    sleeve_name VARCHAR NOT NULL,
    listing_exchange VARCHAR NOT NULL,
    benchmark_name VARCHAR,
    list_date DATE,
    exposure_note TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**6. `canonical_instruments`**

```sql
CREATE TABLE IF NOT EXISTS canonical_instruments (
    instrument_code VARCHAR PRIMARY KEY,
    instrument_name VARCHAR NOT NULL,
    instrument_type VARCHAR NOT NULL,
    exchange VARCHAR,
    benchmark_name VARCHAR,
    list_date DATE,
    delist_date DATE,
    source_name VARCHAR NOT NULL,
    metadata_json TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**7. `canonical_trading_calendar`**

```sql
CREATE TABLE IF NOT EXISTS canonical_trading_calendar (
    exchange VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    pretrade_date DATE,
    calendar_source VARCHAR NOT NULL,
    PRIMARY KEY (exchange, trade_date)
);
```

**8. `canonical_rebalance_calendar`**

```sql
CREATE TABLE IF NOT EXISTS canonical_rebalance_calendar (
    rebalance_date DATE PRIMARY KEY,
    calendar_month VARCHAR NOT NULL,
    rule_name VARCHAR NOT NULL,
    anchor_day INTEGER NOT NULL,
    previous_rebalance_date DATE,
    calendar_source VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

##### Parquet dataset schemas

For Parquet-backed datasets, uniqueness is enforced by the normalization job and validation checks rather than by the file format itself. DuckDB should query these datasets via `read_parquet(...)` views or staged temp tables.

**9. `canonical_daily_market_fact`**
- partition columns: `dataset_year`, `dataset_month`
- recommended path: `normalized/daily_market/dataset_year=YYYY/dataset_month=MM/*.parquet`
- columns and logical types:
  - `instrument_code VARCHAR`
  - `trade_date DATE`
  - `open DOUBLE`
  - `high DOUBLE`
  - `low DOUBLE`
  - `close DOUBLE`
  - `adj_close DOUBLE`
  - `pct_chg DOUBLE`
  - `adj_pct_chg DOUBLE`
  - `vol DOUBLE`
  - `amount DOUBLE`
  - `source_name VARCHAR`
  - `source_trade_date DATE`
  - `raw_batch_id BIGINT`
  - `quality_status VARCHAR`
  - `dataset_year INTEGER`
  - `dataset_month INTEGER`

**10. `canonical_daily_rates_fact`**
- partition columns: `dataset_year`, `dataset_month`
- recommended path: `normalized/daily_rates/dataset_year=YYYY/dataset_month=MM/*.parquet`
- columns:
  - `field_name VARCHAR`
  - `trade_date DATE`
  - `value DOUBLE`
  - `unit VARCHAR`
  - `source_name VARCHAR`
  - `raw_batch_id BIGINT`
  - `revision_note VARCHAR`
  - `quality_status VARCHAR`
  - `dataset_year INTEGER`
  - `dataset_month INTEGER`

**11. `canonical_slow_field_fact`**
- partition columns: `field_name`, `dataset_year`
- recommended path: `normalized/slow_fields/field_name=<field>/dataset_year=YYYY/*.parquet`
- columns:
  - `field_name VARCHAR`
  - `period_label VARCHAR`
  - `period_type VARCHAR`
  - `value DOUBLE`
  - `unit VARCHAR`
  - `release_date DATE`
  - `effective_date DATE`
  - `revision_note VARCHAR`
  - `definition_regime VARCHAR`
  - `regime_note TEXT`
  - `source_name VARCHAR`
  - `raw_batch_id BIGINT`
  - `quality_status VARCHAR`
  - `dataset_year INTEGER`

**12. `canonical_curve_fact`**
- partition columns: `dataset_year`, `dataset_month`
- recommended path: `normalized/curve/dataset_year=YYYY/dataset_month=MM/*.parquet`
- columns:
  - `curve_code VARCHAR`
  - `curve_date DATE`
  - `curve_type VARCHAR`
  - `tenor_years DOUBLE`
  - `yield_value DOUBLE`
  - `source_name VARCHAR`
  - `raw_batch_id BIGINT`
  - `quality_status VARCHAR`
  - `dataset_year INTEGER`
  - `dataset_month INTEGER`

**13. `monthly_feature_snapshot`**
- partition columns: `rebalance_year`
- recommended path: `derived/monthly_feature_snapshot/rebalance_year=YYYY/*.parquet`
- reserved columns for next stage:
  - `rebalance_date DATE`
  - `schema_version VARCHAR`
  - `feature_payload_json TEXT`
  - `source_run_set_json TEXT`
  - `created_at TIMESTAMP`
  - `rebalance_year INTEGER`

##### Raw-layer file conventions

Raw landed files should remain source-native where practical.

- API JSON payloads: prefer compressed JSON lines or raw JSON blobs
- tabular fetches already materialized as DataFrames: allow Parquet raw snapshots if source fidelity is preserved in metadata
- every raw file must be discoverable through `etf_aw_raw_batches`

Recommended raw naming pattern:
- `<dataset_name>__<source_name>__<window_start>__<window_end>__<raw_batch_id>.parquet`
- or `.json.gz` for non-tabular payloads

##### DuckDB query-surface recommendation

Do not duplicate all Parquet facts into DuckDB base tables in stage 1.

Instead, create lightweight DuckDB views later such as:
- `vw_canonical_daily_market_fact`
- `vw_canonical_daily_rates_fact`
- `vw_canonical_slow_field_fact`
- `vw_canonical_curve_fact`

Those views can read the partitioned Parquet datasets directly and keep the control plane small.

#### Schema direction

For stage 1, use different physical models for different data families:

- **Daily market data**: long fact tables keyed by `instrument_code + trade_date`
- **Slow macro/rates fields**: long fact tables keyed by `field_name + period_label + source_name`
- **Derived research features**: later wide monthly tables keyed by `rebalance_date`

This is important because slow fields have heterogeneous timing metadata that do not fit cleanly into a single wide market-style table.

#### Ingestion execution model

Do not extend the current `tradepilot/ingestion/service.py` direct-to-DuckDB pattern for ETF all-weather stage 1.

Current generic sync behavior is suitable for lightweight app tables, but ETF all-weather needs:
- raw persistence before normalization
- dataset-family-specific quality checks
- explicit release/effective-date encoding
- windowed extraction for fragile sources such as curve history

Recommended execution boundary:
- a dedicated ETF all-weather ingestion module orchestrates dataset-family jobs
- each job runs independently and writes a run manifest
- downstream normalization jobs can be replayed from raw files without refetching sources

#### Stage-1 job blueprint

##### Job 1: trading calendar sync
- source priority: Tushare `trade_cal`
- output:
  - raw batch in `raw/trade_calendar/...`
  - normalized `canonical_trading_calendar`
  - refreshed `canonical_rebalance_calendar`
- validation:
  - duplicate `(exchange, trade_date)` check
  - continuity and pretrade linkage check
  - repeatability check on sampled windows
- notes:
  - this is foundational and should run before all monthly feature jobs

##### Job 2: sleeve daily market sync
- scope:
  - `510300.SH`
  - `159845.SZ`
  - `511010.SH`
  - `518850.SH`
  - `159001.SZ`
- source priority:
  - Tushare primary
  - fallback explicitly disabled for ETF prices unless a tested fallback becomes acceptable in this environment
- output:
  - raw batches in `raw/fund_daily/...`
  - normalized `canonical_daily_market_fact`
- validation:
  - missing trade dates vs canonical calendar
  - duplicate-row check
  - zero close / zero vol anomalies
  - extreme pct-change sanity
  - repeatability check on sampled windows
- notes:
  - use incremental date windows for routine sync and bounded backfill windows for historical catch-up
  - because return semantics are adjustment-aware, normalization must reserve both raw close and adjusted basis fields where available

##### Job 3: benchmark index sync
- scope:
  - HS300 benchmark series
  - ZZ1000 benchmark series
- source priority:
  - Tushare primary
- output:
  - raw batches in `raw/index_daily/...`
  - normalized `canonical_daily_market_fact` for index instruments
- validation:
  - continuity vs calendar
  - duplicate-row check
  - basic return sanity
- purpose:
  - supports `hs300_vs_zz1000_20d` later in derived layer

##### Job 4: slow macro sync
- scope:
  - PMI
  - CPI/PPI
  - M1/M2
  - TSF / credit pulse inputs
  - industrial / retail / FAI / exports if promoted into v1 confirmation set
- source priority:
  - Tushare primary backbone
  - official-rule timing overlay mandatory
  - AKShare selective fallback for eligible macro series only
- output:
  - raw batches per field family in `raw/macro/...`
  - normalized `canonical_slow_field_fact`
- normalization rules:
  - map coded source columns to canonical field names
  - compute `period_label`
  - assign `release_date` from frozen conservative rules
  - compute `effective_date = next open trading day on or after release_date`
  - attach `revision_note`
  - attach `definition_regime` and `regime_note` for M1-family fields
- validation:
  - duplicate `(field_name, period_label, source_name)` check
  - monotonic period coverage check
  - missing release/effective date rejection
  - definition regime presence for required fields

##### Job 5: rates and liquidity sync
- scope:
  - Shibor family
  - LPR family
  - other accepted daily liquidity fields if promoted
- source priority:
  - Tushare primary
  - AKShare fallback allowed for validated rates families
- output:
  - raw batches in `raw/rates/...`
  - normalized:
    - `canonical_daily_rates_fact` for Shibor-like daily fields
    - `canonical_slow_field_fact` for LPR monthly fields
- validation:
  - duplicate key checks
  - null quote checks
  - sampled cross-source agreement checks where fallback exists
  - explicit date semantics for LPR

##### Job 6: government curve sync
- scope:
  - China government curve points needed for `1Y` and `10Y`
- source priority:
  - Tushare `yc_cb` / official-anchor-informed extractor
- output:
  - raw batches in `raw/curve/...`
  - normalized `canonical_curve_fact`
  - later derived endpoint series in next stage
- extraction rule:
  - do not request long naive windows
  - fetch by bounded calendar windows to avoid row-cap truncation
  - preserve `curve_type` explicitly
- validation:
  - per-window completeness checks
  - exact tenor extraction check for `1.0` and `10.0`
  - duplicate point checks
  - overlap-window reconciliation check
- notes:
  - this job is operationally separate because curve history is the most extraction-fragile dataset in v1

##### Job 7: instrument metadata sync
- scope:
  - sleeve metadata
  - benchmark / identity / exchange / list-date fields
- source priority:
  - Tushare primary
  - exchange / fund-page cross-check later as needed
- output:
  - raw batches in `raw/instrument_metadata/...`
  - normalized `canonical_sleeves` and `canonical_instruments`
- validation:
  - one-row-per-instrument identity check
  - benchmark mapping presence check
  - exchange consistency check

##### Replay and backfill rules
- every normalization job must accept `raw_batch_id` or partition range as input
- re-normalization should not require refetching upstream sources
- historical backfill should run in bounded windows, not one giant request
- derived datasets must be rebuildable from normalized datasets alone

#### Revalidated stage-1 data scope against research docs

After re-checking the upstream research notes, the current stage-1 plan should stay tightly scoped to the frozen v1 minimum panel plus the metadata required to keep it honest.

##### What stage 1 must ingest

| Layer | Required data | Primary source | Validation / fallback | Validation focus |
|------|---------------|----------------|-----------------------|------------------|
| Calendar | `trade_date`, `pretrade_date`, `rebalance_date_monthly` | Tushare `trade_cal` | local derivation only for emergency fallback | repeatability, continuity, holiday mapping, rebalance rule correctness |
| Sleeve market | `510300.SH`, `159845.SZ`, `511010.SH`, `518850.SH`, `159001.SZ` daily price history plus adjustment basis | Tushare `fund_daily` + `fund_adj` | exchange/fund-page identity checks; ETF price fallback currently not operationally trusted | identity, gaps, duplicates, zero/stale values, extreme jumps, adjusted-return consistency |
| Benchmark market | HS300 and ZZ1000 daily history | Tushare `index_daily` | official index pages / CSIndex-style validation surface | continuity, duplicate rows, derived-field reproducibility |
| Slow macro primary | `official_pmi`, `official_pmi_mom`, `ppi_yoy`, `m1_yoy`, `m2_yoy`, `m1_m2_spread`, `tsf_yoy` / `credit_impulse_proxy` | Tushare macro backbone | NBS / PBOC official release pages; AKShare selective fallback where already validated | release/effective date rules, revision note, semantic regime tagging, cross-source spot checks |
| Slow macro confirmatory | `cpi_yoy`, `industrial_production_yoy`, `retail_sales_yoy`, `fixed_asset_investment_ytd`, `exports_yoy`, `new_loans_total` | Tushare backbone | NBS / PBOC official pages; AKShare selective fallback | timing honesty, role clarity, revision-risk downgrade where needed |
| Rates / liquidity | `shibor_1w`, `lpr_1y`, `lpr_5y` | Tushare `shibor`, `shibor_lpr` | AKShare fallback for validated rate families; official pages for confirmation | date semantics, unit consistency, null checks, cross-source agreement |
| Curve | `cn_gov_10y_yield`, `cn_gov_1y_yield`, slope inputs | Tushare `yc_cb` windowed extractor | ChinaBond official curve site, Chinamoney as awkward recovery path | bounded-window completeness, exact tenor extraction, overlap reconciliation |
| Derived stage-1 inputs | `hs300_vs_zz1000_20d`, `bond_trend_20d`, `gold_trend_20d`, `realized_vol_20d_*` | derived from normalized daily facts | none | reproducibility and prior-data-only construction |

##### What stage 1 should not expand into yet
- AUM / `fund_share` reconciliation as core ingestion dependency
- breadth layer as root v1 dependency
- overseas overlay as stage-1 blocker
- options sentiment, credit spread system, CTA/commodity/overseas core sleeves

##### Where the sources live
- **Tushare backbone**: structured ETF, index, calendar, macro, rates, and curve interfaces used as the practical ingestion backbone
- **Official NBS anchor**: `https://www.stats.gov.cn/sj/zxfb/` for macro release confirmation and recovery
- **Official PBOC anchor**: `https://www.pbc.gov.cn/diaochatongjisi/116219/index.html` for money / credit / LPR-related release confirmation and recovery
- **Official ChinaBond anchor**: `https://yield.chinabond.com.cn/` for curve verification and recovery
- **Official Chinamoney anchor**: `https://www.chinamoney.com.cn/chinese/bkcurv/` as a partially reachable, high-friction backup surface
- **Official Shibor anchor**: `https://www.shibor.org/shibor/web/html/shibor.html` as a reference anchor, but not a dependable direct path in the current environment
- **Exchange / fund-page validation surfaces**: SSE / SZSE / fund pages for instrument identity and benchmark checks

##### Revalidation impact on the current plan
- keep the stage-1 plan focused on the minimum serious panel rather than widening schema scope
- keep adjusted / total-return-like ETF treatment mandatory in normalized market facts
- keep `AKShare` as selective macro/rates fallback, not as trusted ETF price fallback
- keep `M1 / M1-M2 / TSF` in scope, but require `definition_regime` and caution labels in normalized storage
- keep the curve pipeline as a dedicated bounded-window job rather than folding it into generic daily-rates sync

#### Stage-1 required data contract

##### Contract table

| Contract item | Required? | Scope | Primary source | Validation / fallback | Normalized target | Required validation |
|---|---|---|---|---|---|---|
| Trading calendar | Must | `trade_date`, `pretrade_date` | Tushare `trade_cal` | local derivation only for emergency continuity checks | `canonical_trading_calendar` | repeatability, duplicate key, continuity, holiday mapping |
| Monthly rebalance calendar | Must | `rebalance_date_monthly` under post-20th rule | derived from canonical trading calendar | none | `canonical_rebalance_calendar` | rule correctness, month-by-month deterministic reconstruction |
| Sleeve identity registry | Must | `510300.SH`, `159845.SZ`, `511010.SH`, `518850.SH`, `159001.SZ` | Tushare `fund_basic`-style metadata backbone | SSE / SZSE / fund pages | `canonical_sleeves`, `canonical_instruments` | code-name consistency, benchmark mapping, exchange consistency, exposure note presence |
| Sleeve daily prices | Must | daily OHLCV-like history for 5 frozen sleeves | Tushare `fund_daily` | exchange/fund-page spot checks; ETF price fallback not trusted currently | `canonical_daily_market_fact` | gaps vs calendar, duplicate rows, zero/stale values, extreme-jump sanity, repeatability |
| Sleeve adjustment basis | Must | adjustment-aware return basis for 5 sleeves | Tushare `fund_adj` | none beyond consistency checks | `canonical_daily_market_fact` | adjusted-vs-raw consistency, adj factor availability, canonical return-basis enforcement |
| Benchmark index prices | Must | HS300, ZZ1000 daily history | Tushare `index_daily` | official index pages / CSIndex-like validation surface | `canonical_daily_market_fact` | continuity, duplicate rows, reproducible relative-strength derivation |
| Slow macro primary: PMI | Must | `official_pmi`, `official_pmi_mom` | Tushare macro backbone | NBS release page | `canonical_slow_field_fact` | release rule, effective-date rule, month-end leakage prevention, coded-column mapping correctness |
| Slow macro primary: pricing | Must | `ppi_yoy` | Tushare macro backbone | NBS release page | `canonical_slow_field_fact` | release/effective date, revision note, unit consistency |
| Slow macro primary: money | Must | `m1_yoy`, `m2_yoy`, `m1_m2_spread` | Tushare `cn_m`-style backbone | PBOC official pages, AKShare selective fallback | `canonical_slow_field_fact` | release/effective date, revision note, `definition_regime`, `regime_note`, cross-source spot checks |
| Slow macro primary: credit | Must | `tsf_yoy` or `credit_impulse_proxy` | Tushare money/credit backbone | PBOC official pages, AKShare selective fallback | `canonical_slow_field_fact` | explicit construction rule, release/effective date, revision note, caution label |
| Slow macro confirmatory | Should | `cpi_yoy`, `industrial_production_yoy`, `retail_sales_yoy`, `fixed_asset_investment_ytd`, `exports_yoy`, `new_loans_total` | Tushare backbone | NBS / PBOC official pages, AKShare selective fallback | `canonical_slow_field_fact` | timing honesty, revision-risk note, role clarity as confirmatory |
| Shibor daily rates | Must | `shibor_1w` and optionally supporting tenors | Tushare `shibor` | AKShare validated fallback; official Shibor page as reference anchor only | `canonical_daily_rates_fact` | null checks, duplicate key, unit/tenor consistency, cross-source agreement |
| LPR monthly rates | Must | `lpr_1y` | Tushare `shibor_lpr` | AKShare validated fallback; PBOC official confirmation path | `canonical_slow_field_fact` | explicit date semantics, duplicate check, cross-source agreement |
| LPR confirmatory | Should | `lpr_5y` | Tushare `shibor_lpr` | AKShare validated fallback; PBOC official confirmation path | `canonical_slow_field_fact` | explicit date semantics, role clarity as confirmatory |
| Government curve raw points | Must | tenor-point history needed to derive 1Y and 10Y yields | Tushare `yc_cb` bounded-window extractor | ChinaBond official site, Chinamoney awkward backup | `canonical_curve_fact` | bounded-window completeness, curve type retention, duplicate points, overlap reconciliation |
| Government curve endpoints | Must | `cn_gov_1y_yield`, `cn_gov_10y_yield` | derived from normalized curve points | official-anchor verification | later derived from `canonical_curve_fact` | exact tenor extraction, date alignment, unit consistency |
| Market confirmation derived fields | Must | `hs300_vs_zz1000_20d`, `bond_trend_20d`, `gold_trend_20d` | derived from normalized market facts | none | later derived layer / feature snapshot | reproducibility, prior-data-only construction |
| Execution-only vol fields | Must | `realized_vol_20d_*` for 5 sleeves | derived from normalized market facts | none | later derived layer / feature snapshot | reproducibility, prior-data-only construction |
| Fund scale / shares metadata | Later | `aum`, `fund_share`, `shares_outstanding` | Tushare / fund pages / Eastmoney-like validation surfaces | cross-source reconciliation | not stage-1 core | validation-only until hardened |
| Overseas overlay / breadth / options / credit spread system | Defer | non-v1 expansion set | not part of stage-1 scope | none | none in stage 1 | explicitly out of scope |

##### Contract rules
- `Must` items are required before notebook MVP and backtest work starts.
- `Should` items belong in stage 1 schema design, but ingestion hardening can be phased after the must-have path is stable.
- `Later` items may be represented in schema extension points, but they must not block the minimum v1 pipeline.
- `Defer` items stay out of the stage-1 ingestion contract entirely.

##### Contract-level source policy
- Tushare is the practical structured backbone for stage 1.
- Official sources are confirmation and recovery anchors, not assumed low-friction bulk feeds.
- AKShare is selective fallback only where stage-01 validation showed operational value.
- ETF price fallback is not considered reliable outside the Tushare path in the current environment.

#### Stage-1 pipeline implementation architecture

##### Recommended module layout

```text
tradepilot/etf_all_weather/
  __init__.py
  config.py                 # ETF all-weather data root, partition policy, schema versions
  models.py                 # Pydantic models for runs, batches, validation findings, publish results
  registry.py               # canonical sleeve list, field families, source priorities, dataset registry

  orchestration/
    pipeline.py             # top-level runner / job orchestration
    context.py              # run context, run ids, clock, publish mode
    publish.py              # commit / promote normalized partitions after validation passes

  sources/
    base.py                 # source adapter contracts
    tushare_market.py       # trade_cal, fund_daily, fund_adj, index_daily
    tushare_macro.py        # PMI/CPI/PPI/M1/M2/TSF/LPR/Shibor adapters
    tushare_curve.py        # bounded-window yc_cb extractor
    official_anchors.py     # release-date / recovery confirmation helpers
    akshare_fallback.py     # selective validated fallback routes only

  landing/
    raw_writer.py           # write immutable raw batches + manifests
    manifest_store.py       # save raw_batch metadata into DuckDB
    watermark_store.py      # save/update source watermarks

  normalize/
    market.py               # normalize ETF/index daily facts
    slow_fields.py          # normalize macro/rates slow fields and timing metadata
    daily_rates.py          # normalize Shibor-like daily rate rows
    curve.py                # normalize curve points
    calendars.py            # canonical trade/rebalance calendar builders
    instruments.py          # canonical sleeves/instrument reference normalization

  validate/
    base.py                 # validation result model and helper framework
    calendars.py            # continuity / rebalance-rule checks
    market.py               # gaps / duplicates / stale / jump checks
    slow_fields.py          # timing / revision / regime checks
    rates.py                # unit / duplicate / null / agreement checks
    curve.py                # completeness / overlap / tenor checks

  storage/
    duckdb_meta.py          # create/read/write metadata tables via existing DuckDB file
    parquet_layout.py       # partition path builders
    parquet_writer.py       # partition write / replace semantics
    parquet_views.py        # optional DuckDB read_parquet views

  jobs/
    trade_calendar.py
    instruments.py
    sleeve_market.py
    benchmark_index.py
    slow_macro.py
    rates_liquidity.py
    government_curve.py
```

##### Boundary with existing TradePilot modules

Reuse:
- `tradepilot/config.py` for repo-level storage roots and environment variables
- `tradepilot/db.py` for the existing DuckDB connection lifecycle
- existing Tushare client utilities where they already exist and are stable

Do not reuse directly for stage 1:
- `tradepilot/ingestion/service.py` as the main orchestration path
- the generic market-sync `INSERT OR REPLACE` pattern for ETF all-weather facts

Reason:
- stage 1 needs raw landing, publish gating, partition replacement, and dataset-specific validation before promotion

##### Core execution flow

One job run should execute in this order:

1. create `run_id` and insert `etf_aw_ingestion_runs`
2. fetch source data for one bounded window
3. land raw payload/files through `raw_writer.py`
4. insert/update `etf_aw_raw_batches`
5. normalize raw batch into canonical rows
6. execute dataset validators
7. if validators pass publish threshold, write/replace normalized Parquet partitions
8. if publish succeeds, update source watermark
9. finalize run status and validation summaries in DuckDB metadata tables

Important design rule:
- **raw landing happens before validation**
- **publish happens only after validation**

##### Writer and publisher boundaries

**Raw writer**
- responsibility: persist source-native or source-faithful payloads only
- never mutates prior raw files
- returns `raw_batch_id`, storage path, row count, content hash

**Normalizer**
- responsibility: map one raw batch into canonical rows
- cannot write directly into final published partitions
- outputs an in-memory DataFrame or staged temporary Parquet chunk for validation

**Validator**
- responsibility: inspect canonical rows and emit structured findings
- does not mutate data
- returns statuses like `pass`, `pass_with_caveat`, `fail`

**Publisher**
- responsibility: atomically promote validated normalized partitions
- performs partition replacement only after validator acceptance
- records which partition paths were written

##### Ingestion rules

###### 1. Windowing rules
- trading calendar: monthly or quarterly windows
- sleeve / benchmark daily data: monthly routine windows, larger bounded windows for backfill
- slow macro fields: by period range, typically monthly blocks
- Shibor / daily rates: monthly windows
- curve: strict bounded windows only; never a giant multi-year request

###### 2. Idempotency rules
- a rerun for the same dataset + source + window may create a new raw batch, but must not create duplicated published normalized rows
- normalized publish uses partition replacement, not append-blind merge
- duplicate keys are a validation failure unless the job defines a deterministic dedupe rule

###### 3. Publish rules
- publish granularity follows dataset partitioning, not arbitrary row-level overwrite
- if one partition fails validation, that partition is not promoted
- successful partitions may still be published for other windows in the same run only if the job supports partial publish explicitly
- default stage-1 behavior should be conservative: single-window job, single publish decision

###### 4. Replay rules
- replay reads by `raw_batch_id` or partition window
- replay never refetches from upstream by default
- replay can be used after schema mapping fixes, timing-rule fixes, or validator improvements

###### 5. Fallback rules
- fallback is source-family-specific and declared in `registry.py`
- fallback may be used only when:
  - the contract marks it as allowed
  - primary fetch failed or primary coverage is missing
  - the run metadata records fallback activation
- ETF price jobs must not auto-fallback to AKShare in stage 1

###### 6. Validation failure rules
- raw batch stays preserved
- validation findings are persisted
- normalized publish is blocked for failed partitions
- run status may be `partial` or `failed` depending on job policy
- no silent downgrade from failed validation to published curated data

###### 7. Watermark rules
- watermark advances only after successful publish
- failed or validation-blocked runs must not advance freshness state
- watermarks are tracked per `dataset_name + source_name`

##### First runnable slice

The first runnable slice should prove the architecture with the smallest end-to-end path, not the full v1 data universe.

###### Slice A — minimum pipeline bring-up
1. `trade_calendar` job
2. `instruments` job for the 5 frozen sleeves
3. `sleeve_market` raw+normalized path for `fund_daily`
4. `sleeve_market` adjustment path for `fund_adj`
5. publish `canonical_daily_market_fact` with adjustment-aware fields

###### Why this slice first
- it validates the full `extract -> raw -> normalize -> validate -> publish` loop
- it covers the frozen sleeve set immediately
- it encodes the adjustment-aware return requirement from research
- it avoids the curve and macro timing complexity until the backbone is proven

###### Slice A acceptance criteria
- raw files are written for all four job families
- DuckDB metadata tables capture runs, raw batches, and validation results
- `canonical_trading_calendar` and `canonical_rebalance_calendar` are populated
- `canonical_sleeves` / `canonical_instruments` are populated for the 5 sleeves
- `canonical_daily_market_fact` contains published partitions for the frozen sleeves with both raw-close and adjustment-aware fields
- validation catches duplicate/gap/stale anomalies and blocks publish when required
- rerunning one monthly window does not create duplicate published rows

##### Recommended implementation order

Phase 1:
1. `storage/duckdb_meta.py`
2. `landing/raw_writer.py`
3. `storage/parquet_layout.py` and `storage/parquet_writer.py`
4. `validate/base.py`
5. `orchestration/pipeline.py`

Phase 2:
1. `jobs/trade_calendar.py`
2. `normalize/calendars.py`
3. `validate/calendars.py`
4. `jobs/instruments.py`
5. `normalize/instruments.py`

Phase 3:
1. `sources/tushare_market.py`
2. `jobs/sleeve_market.py`
3. `normalize/market.py`
4. `validate/market.py`
5. first runnable slice end-to-end dry run

Phase 4:
1. `sources/tushare_macro.py`
2. `jobs/rates_liquidity.py`
3. `normalize/daily_rates.py`
4. `normalize/slow_fields.py`
5. `validate/rates.py` and `validate/slow_fields.py`

Phase 5:
1. `sources/tushare_curve.py`
2. `jobs/government_curve.py`
3. `normalize/curve.py`
4. `validate/curve.py`

#### Large-volume preservation rule

The preservation rule should be:
- **raw is immutable and append-only**
- **normalized is reproducible and replaceable by partition**
- **derived is disposable and rebuildable**

That rule gives the project safe reprocessing when source mappings or timing rules evolve.

## Design Decisions

### Decision 0: Build Stage 1 around raw → normalized storage before strategy logic
**Date**: 2026-04-16
**Status**: Decided

**Context**: The upstream research handoff explicitly says the project is now blocked by schema and pipeline engineering, not by missing field research. The most dangerous implementation failure is hidden timing leakage or source drift entering notebooks directly.

**Options considered**:
1. **Schema-and-pipeline first**: Design raw, normalized, and later derived layers before notebook/backtest work.
2. **Notebook-first exploration**: Fetch data ad hoc in research code and formalize storage later.

**Decision**: Build Stage 1 around raw → normalized storage before strategy logic.

**Consequences**: Timing rules, lineage, validation, and reprocessing become first-class concerns. Notebook work must consume curated tables instead of live source wrappers.

### Decision 1: Start with a read-only strategy surface
**Date**: 2026-04-16
**Status**: Decided

**Context**: The user wants a new capability while keeping APIs stable. The codebase is currently workflow-first, and the implementation blueprint argues for a small, transparent allocation engine before anything more ambitious.

**Options considered**:
1. **Read-only strategy summary first**: Expose strategy outputs and rationale without portfolio write actions.
2. **Full actionable workflow first**: Add allocation editing, rebalance actions, and portfolio integration immediately.

**Decision**: Start with a read-only strategy surface because it delivers the new capability with lower contract and workflow disruption, and it matches the blueprint's emphasis on transparency and phased rollout.

**Consequences**: The first phase should optimize for explanation, freshness, and stable presentation rather than trade execution.

### Decision 2: Use hybrid Parquet + DuckDB storage for stage-1 historical data
**Date**: 2026-04-16
**Status**: Decided

**Context**: The current repo already uses DuckDB, but ETF all-weather stage 1 needs immutable raw retention, large-history storage, and partition-level reprocessing.

**Options considered**:
1. **All-in DuckDB**: Store raw, normalized, and derived data in one expanding database file.
2. **Hybrid Parquet + DuckDB**: Store large raw/normalized facts in Parquet while using DuckDB for metadata and query access.
3. **Filesystem raw only, no structured curated layer yet**: Delay formal normalized storage.

**Decision**: Use hybrid Parquet + DuckDB storage for stage 1.

**Consequences**: Historical data remains easier to replay, partition-rebuild, and inspect. DuckDB stays valuable as the query and metadata control plane rather than becoming a monolithic raw dump.

### Decision 3: Keep metadata and small dimensions in DuckDB, but keep large facts in Parquet
**Date**: 2026-04-16
**Status**: Decided

**Context**: Stage 1 needs both transactional metadata semantics and large-history persistence. The current repo already initializes many application tables directly inside DuckDB.

**Options considered**:
1. **Everything in DuckDB tables**: simple to query, but raw and historical facts bloat the DB and make partition-level rebuilds harder.
2. **Everything in Parquet**: simple files, but weaker control-plane semantics for runs, manifests, and quality tracking.
3. **Split by data role**: metadata/dimensions in DuckDB, large facts in Parquet.

**Decision**: Split by data role. Keep metadata and small dimensions in DuckDB, but keep large fact histories in Parquet.

**Consequences**: Stage 1 gets transactional control for orchestration state and low-friction analytics for historical facts, while preserving replay and partition rebuild capabilities.

### Decision 4: Prefer additive integration with the workflow-first shell
**Date**: 2026-04-16
**Status**: Decided

**Context**: The current frontend is explicitly workflow-first, and legacy pages are not the default landing zone for new features.

**Options considered**:
1. **Integrate into Dashboard first**: Surface ETF all-weather as an additive workflow-compatible panel or section.
2. **Create a separate new primary page immediately**: Build a standalone experience from day one.

**Decision**: Prefer additive integration with the existing Dashboard-first shell for the first slice, while keeping open the option of a dedicated page later if the feature expands.

**Consequences**: The initial design should optimize for compact summaries and drill-downs instead of a large new navigation branch.

## Phases

### Phase 1: Data foundation
- [x] Define raw / normalized / derived storage boundaries for ETF all-weather data
- [x] Define canonical dataset groups and table/file layout
- [x] Define large-history storage strategy and partitioning rules
- [x] Define ingestion lineage and validation model

### Phase 2: Strategy-serving boundary
- [ ] Identify where ETF all-weather data and analysis should live in the backend
- [ ] Identify the smallest useful frontend surface for presenting the strategy
- [ ] Define the minimum stable contract between backend and frontend

### Phase 3: Delivery shape
- [ ] Break the feature into implementation-ready increments
- [ ] Identify dependencies, risks, and open questions

## Open Questions

- [ ] Should raw source payloads be stored as source-native JSON/CSV snapshots, normalized Parquet only, or both?
- [ ] Should stage-1 normalized tables live fully inside DuckDB, or should DuckDB primarily query Parquet partitions?
- [ ] What retention and compaction rules should apply to raw daily fetch batches?
- [ ] Do we want to ingest the confirmatory slow fields in stage 1 immediately, or only after the primary slow-field path is stable?
- [ ] Should stage-1 index ingestion include only the minimum HS300 / ZZ1000 pair, or also reserve extra benchmark series now?
- [ ] What user workflow should trigger or surface the ETF all-weather view?
- [ ] Is this feature research-only, advisory, or portfolio-actionable?
- [ ] What inputs are user-configurable versus system-derived?
- [ ] Should this capability be attached to workflow, portfolio, research, or a new page?

## Session Log

- **2026-04-16**: Initial design session started for a new full-stack ETF all-weather capability. User selected design-only mode, full-stack scope, new capability outcome, and API stability as the primary constraint.
- **2026-04-16**: Reviewed current workflow-first backend/frontend architecture and linked the ETF all-weather design to the existing implementation blueprint. Recorded initial decisions to keep the first slice read-only and integrate additively with the Dashboard-first shell.
- **2026-04-16**: Pivoted the immediate design focus to Stage 1 data engineering. Reviewed the ETF all-weather data-source handoff documents and existing TradePilot ingestion/storage patterns. Recorded a Stage-1 recommendation to build a dedicated raw → normalized pipeline with hybrid Parquet + DuckDB storage for large historical data.
- **2026-04-16**: Expanded the design with a stage-1 schema blueprint and per-job ingestion blueprint covering trading calendar, sleeve daily data, benchmark indexes, slow macro fields, rates/liquidity, government curve extraction, and instrument metadata.
- **2026-04-16**: Expanded the storage design to DDL level, including DuckDB metadata tables, Parquet-backed fact dataset schemas, storage placement rules, and raw-file naming conventions.
- **2026-04-16**: Revalidated the stage-1 plan against the upstream data research notes and added a concrete required-data matrix covering what to ingest, how to validate it, and where each source family lives.
- **2026-04-16**: Added a stage-1 required data contract that classifies each dataset group as Must / Should / Later / Defer and ties it to source policy, validation requirements, and normalized storage targets.
- **2026-04-16**: Added the stage-1 pipeline implementation architecture, including module layout, execution flow, ingestion rules, publisher boundaries, and the first runnable slice needed to bring the data pipeline up end-to-end.
