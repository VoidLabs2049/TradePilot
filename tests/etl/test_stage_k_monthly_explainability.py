"""Stage K ETF all-weather monthly explainability tests."""

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
from tradepilot.etl.datasets import (
    build_derived_etf_aw_monthly_explainability_dataset,
)
from tradepilot.etl.etf_aw_universe import (
    ETF_AW_SLEEVE_CODE_BY_ROLE,
    ETF_AW_SLEEVE_ROLE_ORDER,
)
from tradepilot.etl.models import RunStatus
from tradepilot.etl.service import ETLService
from tradepilot.etl import update_etf_aw_data as update_module


class StageKMonthlyExplainabilityTests(unittest.TestCase):
    """Verify monthly explanation rows over frozen ETF all-weather artifacts."""

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

    def test_monthly_explainability_summarizes_frozen_inputs(self) -> None:
        rebalance_date = date(2024, 7, 22)
        self.service._write_etf_aw_strategy_context(
            pd.DataFrame([self._context_row(rebalance_date)])
        )
        self.service._write_etf_aw_risk_budget(
            pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            )
        )
        self.service._write_etf_aw_target_weight(
            pd.DataFrame(
                [
                    self._target_weight_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            )
        )
        self.service._write_etf_aw_backtest_kernel(
            pd.DataFrame([self._turnover_row(rebalance_date)])
        )

        result = self.service.run_bootstrap(
            "derived.etf_aw_monthly_explainability.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertTrue(all(result["validation"].values()))
        frame = self._read_explainability_file(2024, 7)
        self.assertEqual(len(frame), 1)
        row = frame.iloc[0]
        self.assertEqual(row["market_regime_label"], "risk_on")
        self.assertEqual(row["source_context_strategy_version"], "stage_g_v1")
        self.assertEqual(row["source_risk_budget_strategy_version"], "risk_budget_v1")
        self.assertEqual(
            row["source_target_weight_strategy_version"],
            "target_weight_inverse_vol_v1",
        )
        self.assertTrue(bool(row["macro_rates_missing"]))
        self.assertEqual(row["risk_budget_status"], "partial")
        self.assertEqual(row["target_weight_status"], "partial")
        self.assertEqual(float(row["turnover_estimate"]), 0.123456)
        self.assertEqual(float(row["backtest_turnover"]), 0.12)
        constraints = json.loads(row["constraint_flags_json"])
        self.assertTrue(constraints["vol_floor_triggered"])
        self.assertTrue(constraints["cap_triggered"])
        self.assertTrue(constraints["no_trade_band_triggered"])
        target = json.loads(row["target_weight_explanation_json"])
        self.assertEqual(len(target["weights"]), 5)

    def test_monthly_explainability_ignores_other_context_strategy_version(
        self,
    ) -> None:
        rebalance_date = date(2024, 7, 22)
        context = self._context_row(rebalance_date)
        other_context = self._context_row(rebalance_date)
        other_context["strategy_version"] = "experimental_context_v2"
        other_context["market_regime_label"] = "defensive"
        other_context["ingested_at"] = pd.Timestamp("2024-07-22 16:00:00")

        frame = self.service._make_etf_aw_monthly_explainability_frame(
            strategy_context=pd.DataFrame([context, other_context]),
            risk_budget=pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            ),
            target_weight=pd.DataFrame(
                [
                    self._target_weight_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            ),
            backtest_kernel=pd.DataFrame([self._turnover_row(rebalance_date)]),
        )

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["market_regime_label"], "risk_on")
        self.assertEqual(frame.iloc[0]["source_context_strategy_version"], "stage_g_v1")

    def test_monthly_explainability_missing_required_columns_returns_empty(
        self,
    ) -> None:
        rebalance_date = date(2024, 7, 22)
        context = pd.DataFrame([self._context_row(rebalance_date)]).drop(
            columns=["ingested_at"]
        )

        frame = self.service._make_etf_aw_monthly_explainability_frame(
            strategy_context=context,
            risk_budget=pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            ),
            target_weight=pd.DataFrame(
                [
                    self._target_weight_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            ),
            backtest_kernel=pd.DataFrame([self._turnover_row(rebalance_date)]),
        )

        self.assertTrue(frame.empty)

    def test_monthly_explainability_requires_backtest_rebalance_evidence(
        self,
    ) -> None:
        rebalance_date = date(2024, 7, 22)
        metric_row = self._turnover_row(rebalance_date)
        metric_row["observation_type"] = "metric"
        metric_row["metric_name"] = "total_return"
        metric_row["quality_notes_json"] = "{}"

        frame = self.service._make_etf_aw_monthly_explainability_frame(
            strategy_context=pd.DataFrame([self._context_row(rebalance_date)]),
            risk_budget=pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            ),
            target_weight=pd.DataFrame(
                [
                    self._target_weight_row(rebalance_date, role)
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            ),
            backtest_kernel=pd.DataFrame([metric_row]),
        )

        self.assertTrue(frame.empty)

    def test_monthly_explainability_declares_frozen_artifact_dependencies(self) -> None:
        definition = build_derived_etf_aw_monthly_explainability_dataset()

        self.assertIn("derived.etf_aw_strategy_context", definition.dependencies)
        self.assertIn("derived.etf_aw_risk_budget", definition.dependencies)
        self.assertIn("derived.etf_aw_target_weight", definition.dependencies)
        self.assertIn("derived.etf_aw_backtest_kernel", definition.dependencies)

    def test_update_plan_runs_explainability_after_backtest_kernel(self) -> None:
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE etl_source_watermarks (
                dataset_name VARCHAR PRIMARY KEY,
                latest_fetched_date DATE,
                updated_at TIMESTAMP
            )
        """)

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
            names.index("derived.etf_aw_backtest_kernel.build"),
            names.index("derived.etf_aw_monthly_explainability.build"),
        )

    def _context_row(self, rebalance_date: date) -> dict:
        return {
            "schema_version": "etf_aw_strategy_context_v1",
            "contract_version": "etf_aw_strategy_context_contract_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": rebalance_date.strftime("%Y-%m"),
            "rebalance_date": rebalance_date,
            "effective_date": rebalance_date,
            "strategy_name": "etf_aw_v1",
            "strategy_version": "stage_g_v1",
            "context_status": "partial",
            "readiness_level": "degraded_research",
            "context_basis": "market_only",
            "market_context_status": "complete",
            "market_regime_label": "risk_on",
            "market_score": 65.0,
            "market_confidence_score": 0.55,
            "market_confidence_cap": 0.70,
            "macro_rates_context_status": "partial",
            "missing_primary_fields_json": '["macro.cpi_yoy"]',
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

    def _budget_row(self, rebalance_date: date, role: str) -> dict:
        return {
            "schema_version": "etf_aw_risk_budget_v1",
            "contract_version": "etf_aw_risk_budget_contract_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "rebalance_date": rebalance_date,
            "strategy_name": "etf_aw_v1",
            "strategy_version": "risk_budget_v1",
            "confidence_score": 0.5,
            "effective_confidence_score": 0.35,
            "market_regime_label": "risk_on",
            "budget_status": "partial",
            "budget_basis": "market_regime_tilt",
            "quality_notes_json": json.dumps(
                {
                    "reasons": ["strategy_context_partial"],
                    "source_context_status": "partial",
                    "source_readiness_level": "degraded_research",
                    "source_regime_status": "complete",
                },
                sort_keys=True,
            ),
            "source_strategy_context_rebalance_date": rebalance_date,
            "source_regime_rebalance_date": rebalance_date,
            "sleeve_role": role,
            "base_budget": 0.2,
            "delta_budget": 0.0,
            "tilted_budget": 0.2,
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }

    def _target_weight_row(self, rebalance_date: date, role: str) -> dict:
        raw = 0.50 if role == "equity_large" else 0.125
        constrained = 0.45 if role == "equity_large" else 0.1375
        target = constrained + 0.001 if role == "bond" else constrained
        return {
            "schema_version": "etf_aw_target_weight_v1",
            "contract_version": "etf_aw_target_weight_contract_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "rebalance_date": rebalance_date,
            "effective_date": rebalance_date,
            "strategy_name": "etf_aw_v1",
            "strategy_version": "target_weight_inverse_vol_v1",
            "sleeve_code": ETF_AW_SLEEVE_CODE_BY_ROLE[role],
            "sleeve_role": role,
            "risk_budget": 0.2,
            "volatility_estimate": 0.005 if role == "gold" else 0.01,
            "volatility_floor": 0.005,
            "raw_target_weight": raw,
            "constrained_target_weight": constrained,
            "target_weight": target,
            "target_weight_status": "partial",
            "optimizer_name": "budgeted_inverse_vol",
            "optimizer_basis": "fixture",
            "turnover_estimate": 0.123456,
            "quality_notes_json": json.dumps(
                {
                    "reasons": (["volatility_floor_applied"] if role == "gold" else []),
                    "source_budget_status": "partial",
                },
                sort_keys=True,
            ),
            "source_risk_budget_rebalance_date": rebalance_date,
            "source_sleeve_daily_max_trade_date": rebalance_date,
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }

    def _turnover_row(self, rebalance_date: date) -> dict:
        return {
            "schema_version": "etf_aw_backtest_kernel_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "strategy_name": "etf_aw_v1",
            "strategy_version": "target_weight_inverse_vol_v1",
            "observation_type": "turnover",
            "observation_date": rebalance_date,
            "metric_name": "monthly_turnover",
            "metric_value": 0.12,
            "net_value": None,
            "portfolio_return": None,
            "quality_notes_json": json.dumps(
                {
                    "rebalance_date": rebalance_date.isoformat(),
                    "turnover_basis": "previous_target_weight",
                },
                sort_keys=True,
            ),
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }

    def _read_explainability_file(self, year: int, month: int) -> pd.DataFrame:
        return pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_monthly_explainability"
            / f"{year:04d}"
            / f"{month:02d}"
            / "part-00000.parquet"
        )


if __name__ == "__main__":
    unittest.main()
