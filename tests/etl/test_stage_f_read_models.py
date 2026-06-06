"""Stage F macro/rates read model tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.read_models import (
    get_latest_etf_aw_macro_rates_context,
    list_etf_aw_macro_rates_contexts,
)
from tradepilot.etl.service import ETLService


class StageFReadModelTests(unittest.TestCase):
    """Verify timing-safe macro/rates read model behavior."""

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
            source_adapters=[],
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

    def test_complete_context_selects_macro_rates_and_curve_fields(self) -> None:
        self._write_full_macro_rates_context()

        context = get_latest_etf_aw_macro_rates_context(
            as_of_date=date(2026, 4, 20),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context["schema_version"], "etf_aw_macro_rates_context_v1")
        self.assertEqual(context["rebalance_date"], "2026-04-20")
        self.assertEqual(context["context_status"], "complete")
        self.assertEqual(context["missing_primary_fields"], [])
        self.assertEqual(context["missing_confirmatory_fields"], [])
        fields = {field["field_name"]: field for field in context["available_fields"]}
        self.assertEqual(fields["official_pmi"]["value"], 50.8)
        self.assertEqual(fields["shibor_1w"]["value"], 1.85)
        self.assertEqual(fields["lpr_1y"]["value"], 3.10)
        self.assertEqual(fields["cn_gov_10y_yield"]["value"], 2.35)
        self.assertAlmostEqual(fields["cn_yield_curve_slope_10y_1y"]["value"], 0.80)
        self.assertFalse(context["quality_notes"]["macro_fields_deferred"])
        self.assertFalse(context["quality_notes"]["curve_fields_deferred"])
        self.assertEqual(len(context["source_caveats"]), 8)
        self.assertEqual(len(context["revision_caveats"]), 8)

    def test_old_required_fields_mark_context_stale(self) -> None:
        self._write_macro_fields(
            [self._macro("official_pmi", "2025-09", date(2025, 10, 1), 50.8)]
        )
        self._write_daily_rates(
            [
                self._daily_rate("shibor_1w", date(2025, 10, 20), 1.85, "primary"),
                self._daily_rate(
                    "shibor_overnight",
                    date(2025, 10, 20),
                    1.72,
                    "confirmatory",
                ),
            ]
        )
        self._write_lpr(
            [
                self._lpr("lpr_1y", date(2025, 10, 20), 3.10, "primary"),
                self._lpr("lpr_5y", date(2025, 10, 20), 3.60, "confirmatory"),
            ]
        )
        self._write_curve_points(
            [
                self._curve(
                    "cn_gov_1y_yield", 1.0, date(2025, 10, 20), 1.55, "confirmatory"
                ),
                self._curve(
                    "cn_gov_10y_yield", 10.0, date(2025, 10, 20), 2.35, "primary"
                ),
            ]
        )

        context = get_latest_etf_aw_macro_rates_context(
            as_of_date=date(2026, 4, 20),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context["context_status"], "stale")
        self.assertEqual(context["missing_primary_fields"], [])
        stale_names = {
            field["field_name"] for field in context["quality_notes"]["stale_fields"]
        }
        self.assertEqual(
            stale_names,
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

    def test_latest_context_selects_eligible_primary_rates(self) -> None:
        self._write_daily_rates(
            [
                self._daily_rate("shibor_1w", date(2026, 4, 19), 1.80, "primary"),
                self._daily_rate("shibor_1w", date(2026, 4, 20), 1.85, "primary"),
                self._daily_rate(
                    "shibor_overnight",
                    date(2026, 4, 20),
                    1.72,
                    "confirmatory",
                ),
            ]
        )
        self._write_lpr(
            [
                self._lpr("lpr_1y", date(2026, 4, 20), 3.10, "primary"),
                self._lpr("lpr_5y", date(2026, 4, 20), 3.60, "confirmatory"),
            ]
        )

        context = get_latest_etf_aw_macro_rates_context(
            as_of_date=date(2026, 4, 20),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context["schema_version"], "etf_aw_macro_rates_context_v1")
        self.assertEqual(context["rebalance_date"], "2026-04-20")
        self.assertEqual(context["context_status"], "unavailable")
        fields = {field["field_name"]: field for field in context["rates_fields"]}
        self.assertEqual(fields["shibor_1w"]["value"], 1.85)
        self.assertEqual(fields["lpr_1y"]["value"], 3.10)
        self.assertEqual(fields["lpr_1y"]["effective_date"], "2026-04-20")
        self.assertEqual(
            context["missing_primary_fields"],
            ["cn_gov_10y_yield", "official_pmi"],
        )
        self.assertEqual(
            context["missing_confirmatory_fields"],
            ["cn_gov_1y_yield", "cn_yield_curve_slope_10y_1y"],
        )
        self.assertTrue(context["quality_notes"]["rates_primary_fields_available"])
        self.assertEqual(len(context["source_caveats"]), 4)
        self.assertEqual(len(context["revision_caveats"]), 4)

    def test_future_effective_rows_are_excluded_and_recorded(self) -> None:
        self._write_daily_rates(
            [self._daily_rate("shibor_1w", date(2026, 4, 20), 1.85, "primary")]
        )
        self._write_lpr([self._lpr("lpr_1y", date(2026, 4, 21), 3.10, "primary")])

        context = get_latest_etf_aw_macro_rates_context(
            as_of_date=date(2026, 4, 20),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context["context_status"], "unavailable")
        self.assertIn("lpr_1y", context["missing_primary_fields"])
        excluded = context["quality_notes"]["excluded_future_effective_fields"]
        self.assertEqual(len(excluded), 1)
        self.assertEqual(excluded[0]["field_name"], "lpr_1y")
        self.assertEqual(excluded[0]["effective_date"], "2026-04-21")

    def test_list_contexts_returns_window_contexts(self) -> None:
        self._write_daily_rates(
            [self._daily_rate("shibor_1w", date(2026, 4, 20), 1.85, "primary")]
        )
        self._write_lpr([self._lpr("lpr_1y", date(2026, 4, 20), 3.10, "primary")])

        contexts = list_etf_aw_macro_rates_contexts(
            date(2026, 4, 20),
            date(2026, 4, 20),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["rebalance_date"], "2026-04-20")
        self.assertEqual(contexts[0]["context_status"], "unavailable")

    def _write_full_macro_rates_context(self) -> None:
        self._write_macro_fields(
            [self._macro("official_pmi", "2026-03", date(2026, 4, 1), 50.8)]
        )
        self._write_daily_rates(
            [
                self._daily_rate("shibor_1w", date(2026, 4, 20), 1.85, "primary"),
                self._daily_rate(
                    "shibor_overnight",
                    date(2026, 4, 20),
                    1.72,
                    "confirmatory",
                ),
            ]
        )
        self._write_lpr(
            [
                self._lpr("lpr_1y", date(2026, 4, 20), 3.10, "primary"),
                self._lpr("lpr_5y", date(2026, 4, 20), 3.60, "confirmatory"),
            ]
        )
        self._write_curve_points(
            [
                self._curve(
                    "cn_gov_1y_yield", 1.0, date(2026, 4, 20), 1.55, "confirmatory"
                ),
                self._curve(
                    "cn_gov_10y_yield", 10.0, date(2026, 4, 20), 2.35, "primary"
                ),
            ]
        )

    def _write_daily_rates(self, rows: list[dict]) -> None:
        self.service._write_daily_rates(
            self.service.registry.get_dataset("rates.daily_rates"),
            pd.DataFrame(rows),
        )

    def _write_macro_fields(self, rows: list[dict]) -> None:
        self.service._write_macro_slow_fields(
            self.service.registry.get_dataset("macro.slow_fields"),
            pd.DataFrame(rows),
        )

    def _write_lpr(self, rows: list[dict]) -> None:
        self.service._write_lpr(
            self.service.registry.get_dataset("rates.lpr"),
            pd.DataFrame(rows),
        )

    def _write_curve_points(self, rows: list[dict]) -> None:
        self.service._write_gov_curve_points(
            self.service.registry.get_dataset("rates.gov_curve_points"),
            pd.DataFrame(rows),
        )

    def _macro(
        self,
        field_name: str,
        period_label: str,
        effective_date: date,
        value: float,
    ) -> dict:
        return {
            "field_name": field_name,
            "period_label": period_label,
            "period_type": "monthly",
            "value": value,
            "unit": "index_point",
            "field_role": "primary",
            "release_date": effective_date,
            "effective_date": effective_date,
            "definition_regime": "",
            "regime_note": "",
            "source_name": "fixture",
            "raw_batch_id": 3,
            "ingested_at": pd.Timestamp("2026-04-01 09:00:00"),
            "revision_note": "latest_history_only_unless_vintage_captured",
            "source_caveat": "fixture_latest_history_caveat",
            "quality_status": "pass_with_caveat",
        }

    def _daily_rate(
        self,
        field_name: str,
        trade_date: date,
        value: float,
        field_role: str,
    ) -> dict:
        return {
            "field_name": field_name,
            "trade_date": trade_date,
            "value": value,
            "unit": "percent",
            "field_role": field_role,
            "release_date": trade_date,
            "effective_date": trade_date,
            "source_name": "fixture",
            "raw_batch_id": 1,
            "ingested_at": pd.Timestamp("2026-04-20 09:00:00"),
            "revision_note": "low_revision_risk",
            "source_caveat": "fixture_same_day_availability_caveat",
            "quality_status": "pass",
        }

    def _lpr(
        self,
        field_name: str,
        effective_date: date,
        value: float,
        field_role: str,
    ) -> dict:
        return {
            "field_name": field_name,
            "quote_date": effective_date,
            "value": value,
            "unit": "percent",
            "field_role": field_role,
            "release_date": effective_date,
            "effective_date": effective_date,
            "source_name": "fixture",
            "raw_batch_id": 2,
            "ingested_at": pd.Timestamp("2026-04-20 09:00:00"),
            "revision_note": "low_revision_risk_relative_to_other_slow_fields",
            "source_caveat": "fixture_source_date_used",
            "quality_status": "pass",
        }

    def _curve(
        self,
        field_name: str,
        tenor_years: float,
        curve_date: date,
        value: float,
        field_role: str,
    ) -> dict:
        return {
            "curve_code": "cn_gov_bond",
            "curve_date": curve_date,
            "tenor_years": tenor_years,
            "field_name": field_name,
            "value": value,
            "unit": "percent",
            "field_role": field_role,
            "release_date": curve_date,
            "effective_date": curve_date,
            "source_name": "fixture",
            "raw_batch_id": 4,
            "ingested_at": pd.Timestamp("2026-04-20 09:00:00"),
            "revision_note": "extraction_method_risk_present",
            "source_caveat": "fixture_curve_extraction_caveat",
            "quality_status": "pass_with_caveat",
        }


if __name__ == "__main__":
    unittest.main()
