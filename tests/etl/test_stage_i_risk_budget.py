"""Stage I ETF all-weather risk budget tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import duckdb
import pandas as pd

from tradepilot import db
from tradepilot.etl import update_etf_aw_data as update_module
from tradepilot.etl.models import RunStatus
from tradepilot.etl.read_models import (
    get_latest_etf_aw_risk_budget,
    list_etf_aw_risk_budgets,
)
from tradepilot.etl.service import ETLService


class StageIRiskBudgetTests(unittest.TestCase):
    """Verify ETF all-weather risk budget generation and read model contracts."""

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

    def test_risk_on_budget_tilts_equity_with_capped_confidence(self) -> None:
        self.assertTrue(self.service.registry.has_dataset("derived.etf_aw_risk_budget"))
        self._write_strategy_context(
            self._context_row(date(2024, 7, 22), "complete", "research_ready")
        )
        self._write_regime(self._regime_row(date(2024, 7, 22), "risk_on", 0.80))

        result = self.service.run_bootstrap(
            "derived.etf_aw_risk_budget.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertTrue(all(result["validation"].values()))
        frame = self._read_budget_file(2024, 7)
        self.assertEqual(len(frame), 5)
        self.assertAlmostEqual(float(frame["tilted_budget"].sum()), 1.0, places=6)
        by_role = frame.set_index("sleeve_role")
        self.assertEqual(by_role.loc["equity_large", "tilted_budget"], 0.235)
        self.assertEqual(by_role.loc["equity_small", "tilted_budget"], 0.235)
        self.assertEqual(by_role.loc["cash", "tilted_budget"], 0.165)
        notes = json.loads(by_role.loc["equity_large", "quality_notes_json"])
        self.assertEqual(notes["effective_confidence_score"], 0.7)

    def test_low_confidence_outputs_neutral_partial_budget(self) -> None:
        self._write_strategy_context(
            self._context_row(date(2024, 7, 22), "complete", "research_ready")
        )
        self._write_regime(self._regime_row(date(2024, 7, 22), "risk_on", 0.20))

        self.service.run_bootstrap(
            "derived.etf_aw_risk_budget.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_budget_file(2024, 7)
        self.assertEqual(set(frame["budget_status"]), {"partial"})
        self.assertEqual(set(frame["budget_basis"]), {"degraded_neutral_budget"})
        self.assertTrue((frame["tilted_budget"] == 0.2).all())
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertIn("low_or_missing_confidence", notes["reasons"])

    def test_future_source_date_fails_point_in_time_validation(self) -> None:
        frame = self.service._make_etf_aw_risk_budget_frame(
            pd.DataFrame(
                [self._context_row(date(2024, 7, 22), "complete", "research_ready")]
            ),
            pd.DataFrame([self._regime_row(date(2024, 7, 22), "risk_on", 0.60)]),
        )
        frame["source_strategy_context_rebalance_date"] = date(2024, 8, 22)

        from tradepilot.etl import service as etl_service

        validation = etl_service._validate_risk_budget_frame(frame)

        self.assertFalse(validation["point_in_time_sources"])

    def test_risk_budget_read_models_return_latest_grouped_contract(self) -> None:
        self._write_strategy_context(
            self._context_row(date(2024, 7, 22), "complete", "research_ready")
        )
        self._write_regime(self._regime_row(date(2024, 7, 22), "hedge_bid", 0.50))
        self.service.run_bootstrap(
            "derived.etf_aw_risk_budget.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        latest = get_latest_etf_aw_risk_budget(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )
        listed = list_etf_aw_risk_budgets(
            date(2024, 7, 1),
            date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest["schema_version"], "etf_aw_risk_budget_v1")
        self.assertEqual(latest["contract_version"], "etf_aw_risk_budget_contract_v1")
        self.assertEqual(latest["rebalance_date"], "2024-07-22")
        self.assertEqual(len(latest["budgets"]), 5)
        self.assertAlmostEqual(latest["tilted_budget_sum"], 1.0, places=6)
        self.assertEqual(len(listed), 1)

    def test_update_plan_includes_risk_budget_after_strategy_context(self) -> None:
        conn = duckdb.connect(":memory:")
        self._create_watermark_table(conn)

        plan = update_module.build_update_plan(
            conn=conn,
            end=date(2026, 6, 7),
            start=date(2026, 6, 1),
            repair_days=7,
            codes=["510300.SH"],
            lakehouse_root=self.lakehouse_root,
        )

        names = [item.name for item in plan]
        self.assertLess(
            names.index("derived.etf_aw_strategy_context.build"),
            names.index("derived.etf_aw_risk_budget.build"),
        )

    def _context_row(
        self, rebalance_date: date, context_status: str, readiness_level: str
    ) -> dict:
        return {
            "schema_version": "etf_aw_strategy_context_v1",
            "contract_version": "etf_aw_strategy_context_contract_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": rebalance_date.strftime("%Y-%m"),
            "rebalance_date": rebalance_date,
            "effective_date": rebalance_date,
            "strategy_name": "etf_aw_v1",
            "strategy_version": "stage_g_v1",
            "context_status": context_status,
            "readiness_level": readiness_level,
            "context_basis": "market_plus_macro_rates",
            "market_context_status": "complete",
            "market_regime_label": "risk_on",
            "market_score": 65.0,
            "market_confidence_score": 0.55,
            "market_confidence_cap": 0.70,
            "macro_rates_context_status": "complete",
            "missing_primary_fields_json": "[]",
            "missing_confirmatory_fields_json": "[]",
            "available_fields_json": "[]",
            "source_caveats_json": "[]",
            "revision_caveats_json": "[]",
            "point_in_time_notes_json": "{}",
            "market_features_json": "{}",
            "source_snapshot_rebalance_date": rebalance_date,
            "source_regime_rebalance_date": rebalance_date,
            "source_macro_rates_rebalance_date": rebalance_date,
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }

    def _regime_row(
        self, rebalance_date: date, label: str, confidence_score: float
    ) -> dict:
        return {
            "schema_version": "etf_aw_regime_score_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": rebalance_date.strftime("%Y-%m"),
            "rebalance_date": rebalance_date,
            "scorer_name": "etf_aw_market_only_regime",
            "scorer_version": "v1",
            "input_snapshot_status": "complete",
            "market_regime_label": label,
            "market_score": 65.0,
            "confidence_score": confidence_score,
            "confidence_level": "medium",
            "confidence_cap": 0.70,
            "scoring_status": "complete",
            "signal_summary": label,
            "signals_json": "[]",
            "quality_notes": "{}",
            "source_snapshot_rebalance_date": rebalance_date,
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }

    def _write_strategy_context(self, row: dict) -> None:
        self.service._write_etf_aw_strategy_context(pd.DataFrame([row]))

    def _write_regime(self, row: dict) -> None:
        self.service._write_etf_aw_regime_score(pd.DataFrame([row]))

    def _read_budget_file(self, year: int, month: int) -> pd.DataFrame:
        return pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_risk_budget"
            / f"{year:04d}"
            / f"{month:02d}"
            / "part-00000.parquet"
        )

    def _create_watermark_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute("""
            CREATE TABLE etl_source_watermarks (
                dataset_name VARCHAR PRIMARY KEY,
                latest_fetched_date DATE,
                updated_at TIMESTAMP
            )
        """)


if __name__ == "__main__":
    unittest.main()
