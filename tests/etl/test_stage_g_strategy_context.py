"""Stage G ETF all-weather strategy context tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl import read_models
from tradepilot.etl.models import IngestionRequest, RunStatus, SourceFetchResult
from tradepilot.etl.service import ETLService
from tradepilot.etl.sources.base import BaseSourceAdapter, SourceRole


class RaisingSourceAdapter(BaseSourceAdapter):
    """Source adapter fixture that fails if a Stage G builder fetches source data."""

    source_name = "fixture_source"
    source_role = SourceRole.PRIMARY

    def supports_dataset(self, dataset_name: str) -> bool:
        """Return true so accidental source fetches reach fetch()."""

        return True

    def fetch(self, dataset_name: str, request: IngestionRequest) -> SourceFetchResult:
        """Fail if a derived builder attempts source fetch."""

        raise AssertionError("Stage G builders must not call source adapters")


class StageGStrategyContextTests(unittest.TestCase):
    """Verify Stage G strategy context assembly and degradation rules."""

    def setUp(self) -> None:
        self._original_db_path = db.DB_PATH
        self._original_thread_local = db._thread_local
        self._original_initialized = db._initialized
        self._temp_dir = TemporaryDirectory()
        db.DB_PATH = Path(self._temp_dir.name) / "test.duckdb"
        db._thread_local = threading.local()
        db._initialized = False
        self.conn = db.get_conn()
        self.lakehouse_root = Path(self._temp_dir.name) / "lakehouse"
        self.service = ETLService(
            conn=self.conn,
            source_adapters=[RaisingSourceAdapter()],
            lakehouse_root=self.lakehouse_root,
        )

    def tearDown(self) -> None:
        conn = getattr(db._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        db._thread_local = self._original_thread_local
        db.DB_PATH = self._original_db_path
        db._initialized = self._original_initialized
        self._temp_dir.cleanup()

    def test_complete_market_context_is_research_ready_with_full_macro_rates(
        self,
    ) -> None:
        self.assertTrue(
            self.service.registry.has_dataset("derived.etf_aw_market_features")
        )
        self.assertTrue(
            self.service.registry.has_dataset("derived.etf_aw_strategy_context")
        )
        self.assertTrue(self.service.registry.has_dataset("rates.daily_rates"))
        self.assertTrue(self.service.registry.has_dataset("rates.lpr"))
        self.assertTrue(self.service.registry.has_dataset("macro.slow_fields"))
        self.assertTrue(self.service.registry.has_dataset("rates.gov_curve_points"))
        self.assertTrue(hasattr(read_models, "get_latest_etf_aw_macro_rates_context"))
        self._write_full_macro_rates_context()
        self._run_pipeline(self._complete_rows())

        result = self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertTrue(all(result["validation"].values()))
        row = self._read_context_file(2024, 7).iloc[0]
        self.assertEqual(row["context_status"], "complete")
        self.assertEqual(row["readiness_level"], "research_ready")
        self.assertEqual(row["context_basis"], "market_plus_macro_rates")
        self.assertEqual(row["market_context_status"], "complete")
        self.assertEqual(row["macro_rates_context_status"], "complete")
        self.assertEqual(json.loads(row["missing_primary_fields_json"]), [])
        self.assertEqual(json.loads(row["missing_confirmatory_fields_json"]), [])
        available_names = {
            field["field_name"] for field in json.loads(row["available_fields_json"])
        }
        self.assertEqual(
            available_names,
            {
                "official_pmi",
                "shibor_1w",
                "shibor_overnight",
                "lpr_1y",
                "lpr_5y",
                "cn_gov_1y_yield",
                "cn_gov_10y_yield",
                "cn_yield_curve_slope_10y_1y",
            },
        )
        notes = json.loads(row["point_in_time_notes_json"])
        self.assertFalse(notes["macro_fields_deferred"])
        self.assertFalse(notes["curve_fields_deferred"])
        self.assertTrue(notes["stage_f_audit"]["macro_rates_read_service_available"])
        self.assertTrue(notes["rates_primary_fields_available"])
        self.assertNotIn("target_weight", row.index)
        self.assertNotIn("trade_action", row.index)
        self.assertNotIn("order_instruction", row.index)

    def test_missing_primary_rates_keeps_market_only_context_partial(self) -> None:
        self._run_pipeline(self._complete_rows())

        self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        row = self._read_context_file(2024, 7).iloc[0]
        self.assertEqual(row["context_status"], "partial")
        self.assertEqual(row["readiness_level"], "degraded_research")
        self.assertEqual(row["context_basis"], "market_only")
        self.assertEqual(row["market_context_status"], "complete")
        self.assertEqual(row["macro_rates_context_status"], "unavailable")
        self.assertIn("shibor_1w", json.loads(row["missing_primary_fields_json"]))
        self.assertIn("lpr_1y", json.loads(row["missing_primary_fields_json"]))

    def test_missing_regime_score_makes_context_unavailable(self) -> None:
        self._write_snapshot(self._complete_rows())
        self.service.run_bootstrap(
            "derived.etf_aw_market_features.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        row = self._read_context_file(2024, 7).iloc[0]
        self.assertEqual(row["context_status"], "unavailable")
        self.assertEqual(row["readiness_level"], "not_ready")
        self.assertEqual(row["market_context_status"], "unavailable")
        self.assertEqual(row["macro_rates_context_status"], "unavailable")

    def test_stale_market_context_takes_priority_over_deferred_macro_rates(
        self,
    ) -> None:
        self._run_pipeline(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06, "stale"),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06, "stale"),
                self._row("511010.SH", "bond", -0.02, -0.04, -0.06, "stale"),
                self._row("518850.SH", "gold", -0.02, -0.04, -0.06, "stale"),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0, "stale"),
            ]
        )

        self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        row = self._read_context_file(2024, 7).iloc[0]
        self.assertEqual(row["context_status"], "stale")
        self.assertEqual(row["readiness_level"], "not_ready")
        self.assertEqual(row["market_context_status"], "stale")

    def test_stale_macro_rates_context_makes_strategy_context_partial(self) -> None:
        self._write_stale_macro_rates_context()
        self._run_pipeline(self._complete_rows())

        self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        row = self._read_context_file(2024, 7).iloc[0]
        self.assertEqual(row["context_status"], "partial")
        self.assertEqual(row["readiness_level"], "degraded_research")
        self.assertEqual(row["market_context_status"], "complete")
        self.assertEqual(row["macro_rates_context_status"], "stale")
        notes = json.loads(row["point_in_time_notes_json"])
        stale_names = {
            field["field_name"]
            for field in notes["macro_rates_quality_notes"]["stale_fields"]
        }
        self.assertIn("shibor_1w", stale_names)

    def test_partial_market_context_stays_degraded_research(self) -> None:
        self._write_full_macro_rates_context()
        self._run_pipeline(
            [
                self._row("510300.SH", "equity_large", 0.02, None, None, "partial"),
                self._row("159845.SZ", "equity_small", 0.02, None, None, "partial"),
                self._row("511010.SH", "bond", -0.02, None, None, "partial"),
                self._row("518850.SH", "gold", -0.02, None, None, "partial"),
                self._row("159001.SZ", "cash", 0.0, None, None, "partial"),
            ]
        )

        self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        row = self._read_context_file(2024, 7).iloc[0]
        self.assertEqual(row["context_status"], "partial")
        self.assertEqual(row["readiness_level"], "degraded_research")
        self.assertEqual(row["market_context_status"], "partial")
        self.assertEqual(row["context_basis"], "market_plus_macro_rates")

    def test_repeat_rebuild_upserts_without_duplicate_business_keys(self) -> None:
        self._run_pipeline(self._complete_rows())
        self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )
        result = self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_context_file(2024, 7)
        self.assertEqual(result["records_updated"], 1)
        self.assertEqual(len(frame), 1)
        self.assertFalse(
            frame.duplicated(
                ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"]
            ).any()
        )

    def _run_pipeline(self, rows: list[dict]) -> None:
        self._write_snapshot(rows)
        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )
        self.service.run_bootstrap(
            "derived.etf_aw_market_features.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

    def _complete_rows(self) -> list[dict]:
        return [
            self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06),
            self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
            self._row("511010.SH", "bond", -0.02, -0.04, -0.06),
            self._row("518850.SH", "gold", -0.02, -0.04, -0.06),
            self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
        ]

    def _row(
        self,
        sleeve_code: str,
        sleeve_role: str,
        return_1m: float | None,
        return_3m: float | None,
        return_6m: float | None,
        data_status: str = "complete",
    ) -> dict:
        return {
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": "2024-07",
            "rebalance_date": date(2024, 7, 22),
            "effective_date": date(2024, 7, 22),
            "sleeve_code": sleeve_code,
            "sleeve_role": sleeve_role,
            "close": 10.0 if data_status != "missing" else None,
            "adj_factor": 1.0 if data_status != "missing" else None,
            "adj_close": 10.0 if data_status != "missing" else None,
            "return_1m": return_1m,
            "return_3m": return_3m,
            "return_6m": return_6m,
            "volatility_3m": 0.10 if data_status != "missing" else None,
            "max_drawdown_6m": 0.0 if data_status != "missing" else None,
            "data_status": data_status,
            "quality_notes": json.dumps({}),
            "source_max_trade_date": date(2024, 7, 22),
            "ingested_at": pd.Timestamp("2024-07-22"),
        }

    def _write_snapshot(self, rows: list[dict]) -> None:
        self.service._write_etf_aw_rebalance_snapshot(pd.DataFrame(rows))

    def _write_full_macro_rates_context(self) -> None:
        self.service._write_macro_slow_fields(
            self.service.registry.get_dataset("macro.slow_fields"),
            pd.DataFrame(
                [
                    {
                        "field_name": "official_pmi",
                        "period_label": "2024-06",
                        "period_type": "monthly",
                        "value": 50.8,
                        "unit": "index_point",
                        "field_role": "primary",
                        "release_date": date(2024, 7, 1),
                        "effective_date": date(2024, 7, 1),
                        "definition_regime": "",
                        "regime_note": "",
                        "source_name": "fixture",
                        "raw_batch_id": 3,
                        "ingested_at": pd.Timestamp("2024-07-01 09:00:00"),
                        "revision_note": "latest_history_only_unless_vintage_captured",
                        "source_caveat": "fixture_latest_history_caveat",
                        "quality_status": "pass_with_caveat",
                    }
                ]
            ),
        )
        self._write_required_rates()
        self._write_curve_points(date(2024, 7, 22))

    def _write_required_rates(self, effective_date: date = date(2024, 7, 22)) -> None:
        self.service._write_daily_rates(
            self.service.registry.get_dataset("rates.daily_rates"),
            pd.DataFrame(
                [
                    {
                        "field_name": "shibor_1w",
                        "trade_date": effective_date,
                        "value": 1.85,
                        "unit": "percent",
                        "field_role": "primary",
                        "release_date": effective_date,
                        "effective_date": effective_date,
                        "source_name": "fixture",
                        "raw_batch_id": 1,
                        "ingested_at": pd.Timestamp(effective_date),
                        "revision_note": "low_revision_risk",
                        "source_caveat": "fixture_same_day_availability_caveat",
                        "quality_status": "pass",
                    },
                    {
                        "field_name": "shibor_overnight",
                        "trade_date": effective_date,
                        "value": 1.72,
                        "unit": "percent",
                        "field_role": "confirmatory",
                        "release_date": effective_date,
                        "effective_date": effective_date,
                        "source_name": "fixture",
                        "raw_batch_id": 1,
                        "ingested_at": pd.Timestamp(effective_date),
                        "revision_note": "low_revision_risk",
                        "source_caveat": "fixture_same_day_availability_caveat",
                        "quality_status": "pass",
                    },
                ]
            ),
        )
        self.service._write_lpr(
            self.service.registry.get_dataset("rates.lpr"),
            pd.DataFrame(
                [
                    {
                        "field_name": "lpr_1y",
                        "quote_date": effective_date,
                        "value": 3.10,
                        "unit": "percent",
                        "field_role": "primary",
                        "release_date": effective_date,
                        "effective_date": effective_date,
                        "source_name": "fixture",
                        "raw_batch_id": 2,
                        "ingested_at": pd.Timestamp(effective_date),
                        "revision_note": "low_revision_risk_relative_to_other_slow_fields",
                        "source_caveat": "fixture_source_date_used",
                        "quality_status": "pass",
                    },
                    {
                        "field_name": "lpr_5y",
                        "quote_date": effective_date,
                        "value": 3.60,
                        "unit": "percent",
                        "field_role": "confirmatory",
                        "release_date": effective_date,
                        "effective_date": effective_date,
                        "source_name": "fixture",
                        "raw_batch_id": 2,
                        "ingested_at": pd.Timestamp(effective_date),
                        "revision_note": "low_revision_risk_relative_to_other_slow_fields",
                        "source_caveat": "fixture_source_date_used",
                        "quality_status": "pass",
                    },
                ]
            ),
        )

    def _write_curve_points(self, effective_date: date) -> None:
        self.service._write_gov_curve_points(
            self.service.registry.get_dataset("rates.gov_curve_points"),
            pd.DataFrame(
                [
                    {
                        "curve_code": "cn_gov_bond",
                        "curve_date": effective_date,
                        "tenor_years": 1.0,
                        "field_name": "cn_gov_1y_yield",
                        "value": 1.55,
                        "unit": "percent",
                        "field_role": "confirmatory",
                        "release_date": effective_date,
                        "effective_date": effective_date,
                        "source_name": "fixture",
                        "raw_batch_id": 4,
                        "ingested_at": pd.Timestamp(effective_date),
                        "revision_note": "extraction_method_risk_present",
                        "source_caveat": "fixture_curve_extraction_caveat",
                        "quality_status": "pass_with_caveat",
                    },
                    {
                        "curve_code": "cn_gov_bond",
                        "curve_date": effective_date,
                        "tenor_years": 10.0,
                        "field_name": "cn_gov_10y_yield",
                        "value": 2.35,
                        "unit": "percent",
                        "field_role": "primary",
                        "release_date": effective_date,
                        "effective_date": effective_date,
                        "source_name": "fixture",
                        "raw_batch_id": 4,
                        "ingested_at": pd.Timestamp(effective_date),
                        "revision_note": "extraction_method_risk_present",
                        "source_caveat": "fixture_curve_extraction_caveat",
                        "quality_status": "pass_with_caveat",
                    },
                ]
            ),
        )

    def _write_stale_macro_rates_context(self) -> None:
        self.service._write_macro_slow_fields(
            self.service.registry.get_dataset("macro.slow_fields"),
            pd.DataFrame(
                [
                    {
                        "field_name": "official_pmi",
                        "period_label": "2023-12",
                        "period_type": "monthly",
                        "value": 50.8,
                        "unit": "index_point",
                        "field_role": "primary",
                        "release_date": date(2024, 1, 1),
                        "effective_date": date(2024, 1, 1),
                        "definition_regime": "",
                        "regime_note": "",
                        "source_name": "fixture",
                        "raw_batch_id": 3,
                        "ingested_at": pd.Timestamp("2024-01-01 09:00:00"),
                        "revision_note": "latest_history_only_unless_vintage_captured",
                        "source_caveat": "fixture_latest_history_caveat",
                        "quality_status": "pass_with_caveat",
                    }
                ]
            ),
        )
        old_date = date(2024, 1, 22)
        self._write_required_rates(old_date)
        self._write_curve_points(old_date)

    def _read_context_file(self, year: int, month: int) -> pd.DataFrame:
        return pd.read_parquet(self._context_file_path(year, month))

    def _context_file_path(self, year: int, month: int) -> Path:
        return (
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_strategy_context"
            / str(year)
            / f"{month:02d}"
            / "part-00000.parquet"
        )


if __name__ == "__main__":
    unittest.main()
