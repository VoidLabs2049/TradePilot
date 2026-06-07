"""Tests for the ETF all-weather update CLI planning."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import duckdb
import pandas as pd
from click.testing import CliRunner

from tradepilot.etl import update_etf_aw_data as module
from tradepilot.etl.update_etf_aw_data import build_update_plan, main


class UpdateEtfAwDataTests(unittest.TestCase):
    """Verify daily update planning without network access."""

    def test_build_update_plan_uses_watermarks_and_repair_days(self) -> None:
        conn = duckdb.connect(":memory:")
        self._create_watermark_table(conn)
        conn.execute("""
            INSERT INTO etl_source_watermarks (
                dataset_name, latest_fetched_date, updated_at
            ) VALUES
                ('market.etf_daily', DATE '2026-05-29', CURRENT_TIMESTAMP),
                ('market.etf_adj_factor', DATE '2026-05-29', CURRENT_TIMESTAMP),
                ('reference.trading_calendar', DATE '2026-05-31', CURRENT_TIMESTAMP)
            """)

        plan = build_update_plan(
            conn=conn,
            end=date(2026, 6, 7),
            start=None,
            repair_days=7,
            codes=["510300.SH"],
        )

        self.assertEqual(plan[0].name, "reference.etf_aw_sleeves.frozen_v1")
        calendar = plan[1]
        self.assertEqual(calendar.name, "reference.trading_calendar")
        self.assertEqual(calendar.start, date(2016, 1, 1))
        self.assertEqual(calendar.end, date(2026, 6, 7))
        daily = plan[2]
        self.assertEqual(daily.name, "market.etf_daily")
        self.assertEqual(daily.start, date(2026, 5, 22))
        self.assertEqual(daily.context, {"instrument_ids": ["510300.SH"]})
        lpr = [item for item in plan if item.name == "rates.lpr"][0]
        self.assertEqual(lpr.start, date(2025, 1, 1))

    def test_build_update_plan_backfills_when_lakehouse_coverage_is_missing(
        self,
    ) -> None:
        conn = duckdb.connect(":memory:")
        self._create_watermark_table(conn)
        conn.execute("""
            INSERT INTO etl_source_watermarks (
                dataset_name, latest_fetched_date, updated_at
            ) VALUES
                ('market.etf_daily', DATE '2026-06-01', CURRENT_TIMESTAMP),
                ('market.etf_adj_factor', DATE '2026-06-01', CURRENT_TIMESTAMP),
                ('macro.slow_fields', DATE '2026-06-01', CURRENT_TIMESTAMP),
                ('rates.daily_rates', DATE '2026-06-01', CURRENT_TIMESTAMP),
                ('rates.lpr', DATE '2026-06-01', CURRENT_TIMESTAMP),
                ('rates.gov_curve_points', DATE '2026-06-01', CURRENT_TIMESTAMP),
                ('reference.trading_calendar', DATE '2026-06-01', CURRENT_TIMESTAMP)
            """)

        with TemporaryDirectory() as temp_dir:
            plan = build_update_plan(
                conn=conn,
                end=date(2026, 6, 7),
                start=None,
                repair_days=7,
                codes=["510300.SH"],
                lakehouse_root=Path(temp_dir) / "lakehouse",
            )

        daily = [item for item in plan if item.name == "market.etf_daily"][0]
        lpr = [item for item in plan if item.name == "rates.lpr"][0]
        derived = [
            item
            for item in plan
            if item.name == "derived.etf_aw_strategy_context.build"
        ][0]
        self.assertEqual(daily.start, date(2016, 1, 1))
        self.assertEqual(lpr.start, date(2025, 1, 1))
        self.assertEqual(derived.start, date(2016, 1, 1))

    def test_build_update_plan_uses_local_latest_when_watermark_is_ahead(
        self,
    ) -> None:
        conn = duckdb.connect(":memory:")
        self._create_watermark_table(conn)
        conn.execute("""
            INSERT INTO etl_source_watermarks (
                dataset_name, latest_fetched_date, updated_at
            ) VALUES
                ('market.etf_daily', DATE '2026-06-01', CURRENT_TIMESTAMP)
            """)

        with TemporaryDirectory() as temp_dir:
            lakehouse = Path(temp_dir) / "lakehouse"
            self._write_lakehouse_dates(
                lakehouse,
                "normalized",
                "market.etf_daily",
                "trade_date",
                self._month_dates(date(2016, 1, 4), date(2025, 12, 31)),
            )
            plan = build_update_plan(
                conn=conn,
                end=date(2026, 6, 7),
                start=None,
                repair_days=7,
                codes=["510300.SH"],
                lakehouse_root=lakehouse,
            )

        daily = [item for item in plan if item.name == "market.etf_daily"][0]
        self.assertEqual(daily.start, date(2025, 12, 24))

    def test_build_update_plan_repairs_first_missing_lakehouse_month(self) -> None:
        conn = duckdb.connect(":memory:")
        self._create_watermark_table(conn)
        conn.execute("""
            INSERT INTO etl_source_watermarks (
                dataset_name, latest_fetched_date, updated_at
            ) VALUES
                ('market.etf_daily', DATE '2026-06-01', CURRENT_TIMESTAMP)
            """)

        with TemporaryDirectory() as temp_dir:
            lakehouse = Path(temp_dir) / "lakehouse"
            self._write_lakehouse_dates(
                lakehouse,
                "normalized",
                "market.etf_daily",
                "trade_date",
                [date(2016, 1, 4), date(2016, 3, 1)],
            )
            plan = build_update_plan(
                conn=conn,
                end=date(2026, 6, 7),
                start=None,
                repair_days=7,
                codes=["510300.SH"],
                lakehouse_root=lakehouse,
            )

        daily = [item for item in plan if item.name == "market.etf_daily"][0]
        self.assertEqual(daily.start, date(2016, 1, 25))

    def test_build_update_plan_full_refresh_ignores_watermarks(self) -> None:
        conn = duckdb.connect(":memory:")
        self._create_watermark_table(conn)
        conn.execute("""
            INSERT INTO etl_source_watermarks (
                dataset_name, latest_fetched_date, updated_at
            ) VALUES
                ('market.etf_daily', DATE '2026-06-01', CURRENT_TIMESTAMP),
                ('rates.lpr', DATE '2026-06-01', CURRENT_TIMESTAMP)
            """)

        plan = build_update_plan(
            conn=conn,
            end=date(2026, 6, 7),
            start=None,
            repair_days=7,
            codes=["510300.SH"],
            full_refresh=True,
        )

        daily = [item for item in plan if item.name == "market.etf_daily"][0]
        lpr = [item for item in plan if item.name == "rates.lpr"][0]
        self.assertEqual(daily.start, date(2016, 1, 1))
        self.assertEqual(lpr.start, date(2025, 1, 1))

    def test_dry_run_prints_plan_without_downloading(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            self._create_watermark_table(conn)
            conn.close()

            result = CliRunner().invoke(
                main,
                [
                    "--dry-run",
                    "--end",
                    "2026-06-07",
                    "--repair-days",
                    "3",
                    "--codes",
                    "510300,159845",
                    "--db-path",
                    str(db_path),
                    "--lakehouse-root",
                    str(Path(temp_dir) / "lakehouse"),
                ],
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("ETF all-weather update plan:", result.output)
        self.assertIn("market.etf_daily", result.output)
        self.assertIn("2025-01-01..2026-06-07", result.output)
        self.assertIn("derived.etf_aw_strategy_context.build", result.output)

    def test_dry_run_works_without_existing_db(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "missing.duckdb"
            result = CliRunner().invoke(
                main,
                [
                    "--dry-run",
                    "--end",
                    "2026-06-07",
                    "--db-path",
                    str(db_path),
                    "--lakehouse-root",
                    str(Path(temp_dir) / "lakehouse"),
                ],
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("market.etf_daily 2016-01-01..2026-06-07", result.output)
        self.assertFalse(db_path.exists())

    def test_update_initializes_custom_db_schema_before_execution(self) -> None:
        original_execute = module.execute_update_plan

        def successful_execute(
            service: object, plan: list[module.UpdatePlanItem]
        ) -> list[dict[str, object]]:
            return [
                {
                    "kind": item.kind,
                    "name": item.name,
                    "status": "success",
                    "records_written": 0,
                    "error_message": None,
                }
                for item in plan
            ]

        module.execute_update_plan = successful_execute
        try:
            with TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "new" / "tradepilot.duckdb"
                result = CliRunner().invoke(
                    main,
                    [
                        "--end",
                        "2026-06-07",
                        "--db-path",
                        str(db_path),
                        "--lakehouse-root",
                        str(Path(temp_dir) / "lakehouse"),
                    ],
                )
                conn = duckdb.connect(str(db_path), read_only=True)
                tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
                conn.close()
        finally:
            module.execute_update_plan = original_execute

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("etl_source_watermarks", tables)
        self.assertIn("canonical_trading_calendar", tables)

    def _create_watermark_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute("""
            CREATE TABLE etl_source_watermarks (
                dataset_name VARCHAR,
                source_name VARCHAR,
                latest_available_date DATE,
                latest_fetched_date DATE,
                latest_successful_run_id BIGINT,
                updated_at TIMESTAMP
            )
            """)

    def _write_lakehouse_dates(
        self,
        lakehouse_root: Path,
        zone: str,
        dataset_name: str,
        date_column: str,
        values: list[date],
    ) -> None:
        frame = pd.DataFrame({date_column: values})
        for value in values:
            partition = (
                lakehouse_root
                / zone
                / dataset_name
                / f"{value.year:04d}"
                / f"{value.month:02d}"
            )
            partition.mkdir(parents=True, exist_ok=True)
            frame[frame[date_column] == value].to_parquet(
                partition / "part-00000.parquet",
                index=False,
            )

    def _month_dates(self, start: date, end: date) -> list[date]:
        values = []
        cursor = date(start.year, start.month, 1)
        final = date(end.year, end.month, 1)
        while cursor <= final:
            if cursor.year == start.year and cursor.month == start.month:
                values.append(start)
            elif cursor.year == end.year and cursor.month == end.month:
                values.append(end)
            else:
                values.append(cursor)
            cursor = (
                date(cursor.year + 1, 1, 1)
                if cursor.month == 12
                else date(cursor.year, cursor.month + 1, 1)
            )
        return values


if __name__ == "__main__":
    unittest.main()
