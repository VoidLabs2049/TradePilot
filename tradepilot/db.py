"""DuckDB connection management and schema initialization."""

import threading

import duckdb

from tradepilot.config import DB_PATH

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_thread_local = threading.local()
_init_lock = threading.Lock()
_initialized = False

_STAGE_B_SEQUENCES = {
    "etl_ingestion_runs_run_id_seq": ("etl_ingestion_runs", "run_id"),
    "etl_raw_batches_raw_batch_id_seq": ("etl_raw_batches", "raw_batch_id"),
    "etl_validation_results_validation_id_seq": (
        "etl_validation_results",
        "validation_id",
    ),
}


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a thread-local DuckDB connection.

    The app serves HTTP requests and scheduler jobs concurrently. Reusing one
    global DuckDB connection across threads can leave pending results on the
    shared cursor/connection and trigger runtime errors. Each thread therefore
    gets its own connection, while schema initialization remains process-wide.
    """
    global _initialized
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = duckdb.connect(str(DB_PATH))
        _thread_local.conn = conn
    if not _initialized:
        with _init_lock:
            if not _initialized:
                _init_tables(conn)
                _initialized = True
    return conn


def _init_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_daily (
            stock_code VARCHAR, date DATE,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, amount DOUBLE, turnover DOUBLE,
            PRIMARY KEY (stock_code, date)
        );
        CREATE TABLE IF NOT EXISTS index_daily (
            index_code VARCHAR, date DATE,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, amount DOUBLE,
            PRIMARY KEY (index_code, date)
        );
        CREATE TABLE IF NOT EXISTS etf_flow (
            etf_code VARCHAR, date DATE,
            net_inflow DOUBLE, volume BIGINT,
            PRIMARY KEY (etf_code, date)
        );
        CREATE TABLE IF NOT EXISTS margin_data (
            date DATE, stock_code VARCHAR,
            margin_balance DOUBLE, margin_buy DOUBLE,
            PRIMARY KEY (date, stock_code)
        );
        CREATE TABLE IF NOT EXISTS northbound_flow (
            date DATE,
            net_buy DOUBLE, buy_amount DOUBLE, sell_amount DOUBLE,
            PRIMARY KEY (date)
        );
        CREATE TABLE IF NOT EXISTS stock_valuation (
            stock_code VARCHAR, date DATE,
            pe_ttm DOUBLE, pb DOUBLE, ps DOUBLE, market_cap DOUBLE,
            PRIMARY KEY (stock_code, date)
        );
        CREATE TABLE IF NOT EXISTS sector_data (
            sector VARCHAR, date DATE,
            avg_pe DOUBLE, avg_pb DOUBLE,
            change_1d DOUBLE, change_5d DOUBLE, change_20d DOUBLE, change_60d DOUBLE,
            PRIMARY KEY (sector, date)
        );
        CREATE TABLE IF NOT EXISTS stock_weekly (
            stock_code VARCHAR, date DATE,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, amount DOUBLE, turnover DOUBLE,
            PRIMARY KEY (stock_code, date)
        );
        CREATE TABLE IF NOT EXISTS stock_monthly (
            stock_code VARCHAR, date DATE,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, amount DOUBLE, turnover DOUBLE,
            PRIMARY KEY (stock_code, date)
        );
        CREATE TABLE IF NOT EXISTS sector_stocks (
            sector VARCHAR, stock_code VARCHAR, stock_name VARCHAR, as_of_date DATE,
            PRIMARY KEY (sector, stock_code, as_of_date)
        );
        CREATE TABLE IF NOT EXISTS stock_sector_map (
            stock_code VARCHAR, sector VARCHAR, as_of_date DATE,
            PRIMARY KEY (stock_code, sector, as_of_date)
        );
        CREATE TABLE IF NOT EXISTS news_items (
            source VARCHAR, source_item_id VARCHAR, title VARCHAR, content VARCHAR,
            category VARCHAR, published_at TIMESTAMP, url VARCHAR, collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            content_hash VARCHAR, processing_status VARCHAR DEFAULT 'pending',
            processing_error VARCHAR, processed_at TIMESTAMP,
            PRIMARY KEY (source, source_item_id)
        );
        CREATE TABLE IF NOT EXISTS video_content (
            source VARCHAR, source_item_id VARCHAR, title VARCHAR, video_url VARCHAR,
            file_path VARCHAR, published_at TIMESTAMP, collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            content_hash VARCHAR, processing_status VARCHAR DEFAULT 'pending',
            processing_error VARCHAR, processed_at TIMESTAMP,
            PRIMARY KEY (source, source_item_id)
        );
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            id BIGINT PRIMARY KEY,
            job_name VARCHAR, source_type VARCHAR, trigger_mode VARCHAR,
            status VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
            records_discovered BIGINT DEFAULT 0, records_inserted BIGINT DEFAULT 0,
            records_updated BIGINT DEFAULT 0, records_failed BIGINT DEFAULT 0,
            error_message VARCHAR
        );
        CREATE TABLE IF NOT EXISTS daily_scan_results (
            id BIGINT PRIMARY KEY,
            scan_date DATE NOT NULL,
            stock_code VARCHAR NOT NULL,
            stock_name VARCHAR,
            action VARCHAR,
            urgency VARCHAR,
            score DOUBLE,
            reasons VARCHAR,
            risk_alerts VARCHAR,
            suggested_price DOUBLE,
            suggested_stop_loss DOUBLE,
            suggested_take_profit VARCHAR,
            UNIQUE (scan_date, stock_code)
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            alert_type VARCHAR NOT NULL,
            urgency VARCHAR DEFAULT 'medium',
            stock_code VARCHAR,
            sector VARCHAR,
            title VARCHAR NOT NULL,
            message TEXT,
            read_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS trading_calendar (
            exchange VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            is_open BOOLEAN NOT NULL,
            pretrade_date DATE,
            PRIMARY KEY (exchange, trade_date)
        );
        CREATE TABLE IF NOT EXISTS market_daily_stats (
            trade_date DATE NOT NULL,
            market_code VARCHAR NOT NULL,
            market_name VARCHAR,
            listed_count INTEGER,
            total_share DOUBLE,
            float_share DOUBLE,
            total_mv DOUBLE,
            float_mv DOUBLE,
            amount DOUBLE,
            vol DOUBLE,
            trans_count DOUBLE,
            pe DOUBLE,
            turnover_rate DOUBLE,
            PRIMARY KEY (trade_date, market_code)
        );
        CREATE TABLE IF NOT EXISTS scheduler_history (
            id BIGINT PRIMARY KEY,
            job_name VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status VARCHAR,
            records_affected INTEGER,
            error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS workflow_runs (
            id BIGINT PRIMARY KEY,
            workflow_date DATE NOT NULL,
            phase VARCHAR NOT NULL,
            triggered_by VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            summary_json TEXT NOT NULL,
            error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS workflow_insights (
            id BIGINT PRIMARY KEY,
            workflow_run_id BIGINT NOT NULL,
            workflow_date DATE NOT NULL,
            phase VARCHAR NOT NULL,
            producer VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            schema_version VARCHAR NOT NULL,
            producer_version VARCHAR NOT NULL,
            source_run_id BIGINT NOT NULL,
            source_context_schema_version VARCHAR NOT NULL,
            insight_json TEXT,
            error_message TEXT,
            generated_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (workflow_date, phase, producer)
        );
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY,
            stock_code VARCHAR, stock_name VARCHAR,
            buy_date DATE, buy_price DOUBLE, quantity INTEGER,
            status VARCHAR DEFAULT 'open'
        );
        CREATE TABLE IF NOT EXISTS etl_ingestion_runs (
            run_id BIGINT PRIMARY KEY,
            job_name VARCHAR,
            dataset_name VARCHAR,
            source_name VARCHAR,
            trigger_mode VARCHAR,
            status VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            request_start DATE,
            request_end DATE,
            records_discovered BIGINT DEFAULT 0,
            records_inserted BIGINT DEFAULT 0,
            records_updated BIGINT DEFAULT 0,
            records_failed BIGINT DEFAULT 0,
            partitions_written INTEGER DEFAULT 0,
            error_message TEXT,
            code_version VARCHAR
        );
        CREATE TABLE IF NOT EXISTS etl_raw_batches (
            raw_batch_id BIGINT PRIMARY KEY,
            run_id BIGINT,
            dataset_name VARCHAR,
            source_name VARCHAR,
            source_endpoint VARCHAR,
            storage_path VARCHAR,
            file_format VARCHAR,
            compression VARCHAR,
            partition_year INTEGER,
            partition_month INTEGER,
            window_start DATE,
            window_end DATE,
            row_count BIGINT DEFAULT 0,
            content_hash VARCHAR,
            fetched_at TIMESTAMP,
            schema_version VARCHAR,
            is_fallback_source BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE IF NOT EXISTS etl_validation_results (
            validation_id BIGINT PRIMARY KEY,
            run_id BIGINT,
            raw_batch_id BIGINT,
            dataset_name VARCHAR,
            check_name VARCHAR,
            check_level VARCHAR,
            status VARCHAR,
            subject_key VARCHAR,
            metric_value DOUBLE,
            threshold_value DOUBLE,
            details_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS etl_source_watermarks (
            dataset_name VARCHAR,
            source_name VARCHAR,
            latest_available_date DATE,
            latest_fetched_date DATE,
            latest_successful_run_id BIGINT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (dataset_name, source_name)
        );
        CREATE TABLE IF NOT EXISTS canonical_instruments (
            instrument_id VARCHAR PRIMARY KEY,
            source_instrument_id VARCHAR,
            instrument_name VARCHAR,
            instrument_type VARCHAR,
            exchange VARCHAR,
            list_date DATE,
            delist_date DATE,
            is_active BOOLEAN DEFAULT TRUE,
            source_name VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS canonical_trading_calendar (
            exchange VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            is_open BOOLEAN NOT NULL,
            pretrade_date DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (exchange, trade_date)
        );
        CREATE TABLE IF NOT EXISTS canonical_rebalance_calendar (
            calendar_name VARCHAR NOT NULL,
            calendar_month VARCHAR,
            rebalance_date DATE NOT NULL,
            effective_date DATE,
            notes TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (calendar_name, rebalance_date)
        );
        CREATE TABLE IF NOT EXISTS canonical_sleeves (
            sleeve_code VARCHAR PRIMARY KEY,
            sleeve_name VARCHAR,
            sleeve_type VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS source_registry (
            source_name VARCHAR PRIMARY KEY,
            source_type VARCHAR,
            source_role VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            base_note TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    instrument_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info('canonical_instruments')").fetchall()
    }
    if "source_instrument_id" not in instrument_columns:
        conn.execute(
            "ALTER TABLE canonical_instruments ADD COLUMN source_instrument_id VARCHAR"
        )
    if "list_date" not in instrument_columns:
        conn.execute("ALTER TABLE canonical_instruments ADD COLUMN list_date DATE")
    if "delist_date" not in instrument_columns:
        conn.execute("ALTER TABLE canonical_instruments ADD COLUMN delist_date DATE")
    if "source_name" not in instrument_columns:
        conn.execute("ALTER TABLE canonical_instruments ADD COLUMN source_name VARCHAR")
    sleeve_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info('canonical_sleeves')").fetchall()
    }
    if "sleeve_role" not in sleeve_columns:
        conn.execute("ALTER TABLE canonical_sleeves ADD COLUMN sleeve_role VARCHAR")
    if "listing_exchange" not in sleeve_columns:
        conn.execute(
            "ALTER TABLE canonical_sleeves ADD COLUMN listing_exchange VARCHAR"
        )
    if "benchmark_name" not in sleeve_columns:
        conn.execute("ALTER TABLE canonical_sleeves ADD COLUMN benchmark_name VARCHAR")
    if "list_date" not in sleeve_columns:
        conn.execute("ALTER TABLE canonical_sleeves ADD COLUMN list_date DATE")
    if "exposure_note" not in sleeve_columns:
        conn.execute("ALTER TABLE canonical_sleeves ADD COLUMN exposure_note TEXT")
    if "created_at" not in sleeve_columns:
        conn.execute("ALTER TABLE canonical_sleeves ADD COLUMN created_at TIMESTAMP")
    rebalance_columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info('canonical_rebalance_calendar')"
        ).fetchall()
    }
    if "calendar_month" not in rebalance_columns:
        conn.execute(
            "ALTER TABLE canonical_rebalance_calendar ADD COLUMN calendar_month VARCHAR"
        )
    news_columns = {
        row[1] for row in conn.execute("PRAGMA table_info('news_items')").fetchall()
    }
    if "url" not in news_columns:
        conn.execute("ALTER TABLE news_items ADD COLUMN url VARCHAR")
    ensure_stage_b_sequences(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            date DATE, stock_code VARCHAR, stock_name VARCHAR,
            direction VARCHAR, price DOUBLE, quantity INTEGER,
            reason VARCHAR
        );
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY,
            date DATE, stock_code VARCHAR,
            signal_type VARCHAR, signal_name VARCHAR,
            direction VARCHAR, strength INTEGER,
            description VARCHAR
        );
        CREATE TABLE IF NOT EXISTS trade_plan (
            id INTEGER PRIMARY KEY,
            stock_code VARCHAR NOT NULL,
            stock_name VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR DEFAULT 'planning',
            entry_target_price DOUBLE,
            entry_quantity INTEGER,
            entry_reason VARCHAR,
            entry_conditions VARCHAR,
            entry_triggered_at DATE,
            entry_actual_price DOUBLE,
            stop_loss_price DOUBLE,
            stop_loss_pct DOUBLE DEFAULT -10,
            stop_loss_conditions VARCHAR,
            take_profit_price DOUBLE,
            take_profit_pct DOUBLE DEFAULT 30,
            take_profit_conditions VARCHAR,
            risk_reward_ratio DOUBLE,
            composite_score DOUBLE,
            signal_summary VARCHAR
        );
    """)


def ensure_stage_b_sequences(conn: duckdb.DuckDBPyConnection) -> None:
    """Create DuckDB sequences used to allocate Stage B metadata IDs."""

    for sequence_name, (table_name, column_name) in _STAGE_B_SEQUENCES.items():
        exists = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM duckdb_sequences()
                WHERE sequence_name = ?
                """,
                [sequence_name],
            ).fetchone()[0]
        )
        if exists:
            continue
        start_value = int(
            conn.execute(
                f"SELECT COALESCE(MAX({column_name}), 0) + 1 FROM {table_name}"
            ).fetchone()[0]
        )
        conn.execute(
            f"CREATE SEQUENCE IF NOT EXISTS {sequence_name} START WITH {start_value}"
        )
