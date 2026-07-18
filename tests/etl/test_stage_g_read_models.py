"""Stage G ETF all-weather read model tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.read_models import (
    get_latest_etf_aw_market_features,
    get_latest_etf_aw_strategy_context,
    list_etf_aw_strategy_contexts,
)
from tradepilot.etl.service import ETLService


class StageGReadModelTests(unittest.TestCase):
    """Verify Stage G read service contracts."""

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

    def test_strategy_context_read_service_returns_api_friendly_contract(self) -> None:
        self._run_pipeline(date(2024, 7, 22))

        context = get_latest_etf_aw_strategy_context(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context["schema_version"], "etf_aw_strategy_context_v1")
        self.assertEqual(
            context["contract_version"], "etf_aw_strategy_context_contract_v1"
        )
        self.assertEqual(context["rebalance_date"], "2024-07-22")
        self.assertEqual(context["context_status"], "complete")
        self.assertEqual(context["readiness_level"], "research_ready")
        self.assertEqual(context["context_basis"], "market_plus_macro_rates")
        self.assertEqual(context["market"]["label"], "risk_on")
        self.assertEqual(context["macro_rates"]["status"], "complete")
        self.assertEqual(context["macro_rates"]["missing_primary_fields"], [])
        self.assertEqual(context["macro_rates"]["missing_confirmatory_fields"], [])
        available_names = {
            field["field_name"] for field in context["macro_rates"]["available_fields"]
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
        self.assertFalse(context["quality_notes"]["macro_fields_deferred"])
        self.assertFalse(context["quality_notes"]["curve_fields_deferred"])
        self.assertIn("sleeve", context["market_features"])
        self.assertNotIn("target_weight", context)
        self.assertNotIn("trade_action", context)
        self.assertNotIn("order_instruction", context)

    def test_market_features_read_service_returns_latest_feature_rows(self) -> None:
        self._run_pipeline(date(2024, 7, 22))

        features = get_latest_etf_aw_market_features(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(features)
        assert features is not None
        self.assertEqual(features["schema_version"], "etf_aw_market_features_v1")
        self.assertEqual(features["rebalance_date"], "2024-07-22")
        self.assertEqual(len(features["features"]), 43)

    def test_list_strategy_contexts_uses_latest_ingested_row_per_key(self) -> None:
        self._run_pipeline(date(2024, 7, 22))
        path = self._context_file_path(2024, 7)
        frame = pd.read_parquet(path)
        early = frame.iloc[0].copy()
        late = frame.iloc[0].copy()
        early["context_status"] = "unavailable"
        early["ingested_at"] = pd.Timestamp("2024-07-22 09:00:00")
        late["context_status"] = "partial"
        late["ingested_at"] = pd.Timestamp("2024-07-22 10:00:00")
        pd.DataFrame([early, late]).to_parquet(path, index=False)

        contexts = list_etf_aw_strategy_contexts(
            date(2024, 7, 1),
            date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["context_status"], "partial")

    def test_invalid_strategy_context_rebalance_date_is_ignored(self) -> None:
        path = self._context_file_path(2024, 7)
        path.parent.mkdir(parents=True, exist_ok=True)
        row = self._context_row(rebalance_date="bad-date")
        pd.DataFrame([row]).to_parquet(path, index=False)

        context = get_latest_etf_aw_strategy_context(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNone(context)

    def test_latest_strategy_context_skips_future_partitions(self) -> None:
        self._run_pipeline(date(2024, 7, 22))
        self._run_pipeline(date(2024, 8, 22))

        context = get_latest_etf_aw_strategy_context(
            as_of_date=date(2024, 8, 1),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context["rebalance_date"], "2024-07-22")

    def _run_pipeline(self, rebalance_date: date) -> None:
        self._write_full_macro_rates_context(rebalance_date)
        self._write_snapshot(self._complete_rows(rebalance_date))
        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(rebalance_date.year, rebalance_date.month, 1),
            end=date(rebalance_date.year, rebalance_date.month, 28),
        )
        self.service.run_bootstrap(
            "derived.etf_aw_market_features.build",
            start=date(rebalance_date.year, rebalance_date.month, 1),
            end=date(rebalance_date.year, rebalance_date.month, 28),
        )
        self.service.run_bootstrap(
            "derived.etf_aw_strategy_context.build",
            start=date(rebalance_date.year, rebalance_date.month, 1),
            end=date(rebalance_date.year, rebalance_date.month, 28),
        )

    def _complete_rows(self, rebalance_date: date) -> list[dict]:
        return [
            self._row(rebalance_date, "510300.SH", "equity_large", 0.02, 0.04, 0.06),
            self._row(rebalance_date, "159845.SZ", "equity_small", 0.02, 0.04, 0.06),
            self._row(
                rebalance_date,
                "513100.SH",
                "equity_overseas",
                0.02,
                0.04,
                0.06,
            ),
            self._row(rebalance_date, "511010.SH", "bond", -0.02, -0.04, -0.06),
            self._row(rebalance_date, "518850.SH", "gold", -0.02, -0.04, -0.06),
            self._row(rebalance_date, "159001.SZ", "cash", 0.0, 0.0, 0.0),
        ]

    def _row(
        self,
        rebalance_date: date,
        sleeve_code: str,
        sleeve_role: str,
        return_1m: float | None,
        return_3m: float | None,
        return_6m: float | None,
    ) -> dict:
        return {
            "calendar_name": "etf_aw_v2_monthly_post_20",
            "calendar_month": f"{rebalance_date.year:04d}-{rebalance_date.month:02d}",
            "rebalance_date": rebalance_date,
            "effective_date": rebalance_date,
            "sleeve_code": sleeve_code,
            "sleeve_role": sleeve_role,
            "close": 10.0,
            "adj_factor": 1.0,
            "adj_close": 10.0,
            "return_1m": return_1m,
            "return_3m": return_3m,
            "return_6m": return_6m,
            "volatility_3m": 0.10,
            "max_drawdown_6m": 0.0,
            "data_status": "complete",
            "quality_notes": json.dumps({}),
            "source_max_trade_date": rebalance_date,
            "ingested_at": pd.Timestamp(rebalance_date),
        }

    def _write_snapshot(self, rows: list[dict]) -> None:
        self.service._write_etf_aw_rebalance_snapshot(pd.DataFrame(rows))

    def _write_full_macro_rates_context(self, rebalance_date: date) -> None:
        self.service._write_macro_slow_fields(
            self.service.registry.get_dataset("macro.slow_fields"),
            pd.DataFrame(
                [
                    {
                        "field_name": "official_pmi",
                        "period_label": f"{rebalance_date.year:04d}-{rebalance_date.month:02d}",
                        "period_type": "monthly",
                        "value": 50.8,
                        "unit": "index_point",
                        "field_role": "primary",
                        "release_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "definition_regime": "",
                        "regime_note": "",
                        "source_name": "fixture",
                        "raw_batch_id": 3,
                        "ingested_at": pd.Timestamp(rebalance_date),
                        "revision_note": "latest_history_only_unless_vintage_captured",
                        "source_caveat": "fixture_latest_history_caveat",
                        "quality_status": "pass_with_caveat",
                    }
                ]
            ),
        )
        self._write_required_rates(rebalance_date)
        self.service._write_gov_curve_points(
            self.service.registry.get_dataset("rates.gov_curve_points"),
            pd.DataFrame(
                [
                    {
                        "curve_code": "cn_gov_bond",
                        "curve_date": rebalance_date,
                        "tenor_years": 1.0,
                        "field_name": "cn_gov_1y_yield",
                        "value": 1.55,
                        "unit": "percent",
                        "field_role": "confirmatory",
                        "release_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "source_name": "fixture",
                        "raw_batch_id": 4,
                        "ingested_at": pd.Timestamp(rebalance_date),
                        "revision_note": "extraction_method_risk_present",
                        "source_caveat": "fixture_curve_extraction_caveat",
                        "quality_status": "pass_with_caveat",
                    },
                    {
                        "curve_code": "cn_gov_bond",
                        "curve_date": rebalance_date,
                        "tenor_years": 10.0,
                        "field_name": "cn_gov_10y_yield",
                        "value": 2.35,
                        "unit": "percent",
                        "field_role": "primary",
                        "release_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "source_name": "fixture",
                        "raw_batch_id": 4,
                        "ingested_at": pd.Timestamp(rebalance_date),
                        "revision_note": "extraction_method_risk_present",
                        "source_caveat": "fixture_curve_extraction_caveat",
                        "quality_status": "pass_with_caveat",
                    },
                ]
            ),
        )

    def _write_required_rates(self, rebalance_date: date) -> None:
        self.service._write_daily_rates(
            self.service.registry.get_dataset("rates.daily_rates"),
            pd.DataFrame(
                [
                    {
                        "field_name": "shibor_1w",
                        "trade_date": rebalance_date,
                        "value": 1.85,
                        "unit": "percent",
                        "field_role": "primary",
                        "release_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "source_name": "fixture",
                        "raw_batch_id": 1,
                        "ingested_at": pd.Timestamp(rebalance_date),
                        "revision_note": "low_revision_risk",
                        "source_caveat": "fixture_same_day_availability_caveat",
                        "quality_status": "pass",
                    },
                    {
                        "field_name": "shibor_overnight",
                        "trade_date": rebalance_date,
                        "value": 1.72,
                        "unit": "percent",
                        "field_role": "confirmatory",
                        "release_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "source_name": "fixture",
                        "raw_batch_id": 1,
                        "ingested_at": pd.Timestamp(rebalance_date),
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
                        "quote_date": rebalance_date,
                        "value": 3.10,
                        "unit": "percent",
                        "field_role": "primary",
                        "release_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "source_name": "fixture",
                        "raw_batch_id": 2,
                        "ingested_at": pd.Timestamp(rebalance_date),
                        "revision_note": "low_revision_risk_relative_to_other_slow_fields",
                        "source_caveat": "fixture_source_date_used",
                        "quality_status": "pass",
                    },
                    {
                        "field_name": "lpr_5y",
                        "quote_date": rebalance_date,
                        "value": 3.60,
                        "unit": "percent",
                        "field_role": "confirmatory",
                        "release_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "source_name": "fixture",
                        "raw_batch_id": 2,
                        "ingested_at": pd.Timestamp(rebalance_date),
                        "revision_note": "low_revision_risk_relative_to_other_slow_fields",
                        "source_caveat": "fixture_source_date_used",
                        "quality_status": "pass",
                    },
                ]
            ),
        )

    def _context_row(self, rebalance_date: date | str) -> dict:
        return {
            "schema_version": "etf_aw_strategy_context_v1",
            "contract_version": "etf_aw_strategy_context_contract_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": "2024-07",
            "rebalance_date": rebalance_date,
            "effective_date": rebalance_date,
            "strategy_name": "etf_aw_v1",
            "strategy_version": "stage_g_v1",
            "context_status": "partial",
            "readiness_level": "degraded_research",
            "context_basis": "market_only",
            "market_context_status": "complete",
            "market_regime_label": "risk_on",
            "market_score": 70.0,
            "market_confidence_score": 0.70,
            "market_confidence_cap": 0.70,
            "macro_rates_context_status": "deferred",
            "missing_primary_fields_json": json.dumps(["official_pmi"]),
            "missing_confirmatory_fields_json": json.dumps([]),
            "available_fields_json": json.dumps([]),
            "source_caveats_json": json.dumps([]),
            "revision_caveats_json": json.dumps([]),
            "point_in_time_notes_json": json.dumps({"macro_fields_deferred": True}),
            "market_features_json": json.dumps({}),
            "source_snapshot_rebalance_date": None,
            "source_regime_rebalance_date": None,
            "source_macro_rates_rebalance_date": None,
            "ingested_at": pd.Timestamp("2024-07-22"),
        }

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
