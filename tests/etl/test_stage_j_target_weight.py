"""Stage J ETF all-weather target weight tests."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import duckdb
import pandas as pd

from tradepilot import db
from tradepilot.etl import service as etl_service
from tradepilot.etl import update_etf_aw_data as update_module
from tradepilot.etl.datasets import build_derived_etf_aw_backtest_kernel_dataset
from tradepilot.etl.etf_aw_universe import (
    ETF_AW_SLEEVE_CODE_BY_ROLE,
    ETF_AW_SLEEVE_ROLE_ORDER,
)
from tradepilot.etl.models import RunStatus, StorageZone
from tradepilot.etl.read_models import (
    get_latest_etf_aw_target_weight,
    list_etf_aw_target_weights,
)
from tradepilot.etl.service import ETLService


class StageJTargetWeightTests(unittest.TestCase):
    """Verify ETF all-weather inverse-vol target weight generation."""

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

    def test_documented_inverse_vol_fixture(self) -> None:
        budgets = {
            "equity_large": 0.235,
            "equity_small": 0.235,
            "bond": 0.186,
            "gold": 0.179,
            "cash": 0.165,
        }
        vol = {
            "equity_large": 0.012,
            "equity_small": 0.016,
            "bond": 0.006,
            "gold": 0.010,
            "cash": 0.005,
        }

        raw = etl_service._budgeted_inverse_vol_weights(budgets, vol)

        self.assertAlmostEqual(raw["equity_large"], 0.1686, places=4)
        self.assertAlmostEqual(raw["equity_small"], 0.1264, places=4)
        self.assertAlmostEqual(raw["bond"], 0.2668, places=4)
        self.assertAlmostEqual(raw["gold"], 0.1541, places=4)
        self.assertAlmostEqual(raw["cash"], 0.2841, places=4)
        self.assertAlmostEqual(sum(raw.values()), 1.0, places=6)

    def test_caps_limit_cash_and_non_cash_then_redistribute(self) -> None:
        cash_heavy = {
            "equity_large": 0.10,
            "equity_small": 0.10,
            "bond": 0.10,
            "gold": 0.10,
            "cash": 0.60,
        }
        constrained = etl_service._apply_target_weight_caps(cash_heavy)

        self.assertAlmostEqual(constrained["cash"], 0.35, places=6)
        self.assertAlmostEqual(sum(constrained.values()), 1.0, places=6)
        self.assertTrue(
            all(value <= 0.45 for role, value in constrained.items() if role != "cash")
        )

        bond_heavy = {
            "equity_large": 0.10,
            "equity_small": 0.10,
            "bond": 0.60,
            "gold": 0.10,
            "cash": 0.10,
        }
        constrained = etl_service._apply_target_weight_caps(bond_heavy)

        self.assertAlmostEqual(constrained["bond"], 0.45, places=6)
        self.assertAlmostEqual(sum(constrained.values()), 1.0, places=6)

    def test_single_insufficient_volatility_outputs_partial_rows(self) -> None:
        rebalance_date = date(2024, 7, 22)
        budget = pd.DataFrame(
            [
                self._budget_row(rebalance_date, role, 0.2, "complete")
                for role in ETF_AW_SLEEVE_ROLE_ORDER
            ]
        )
        panel = self._panel(rebalance_date, missing_role="cash", observations=63)

        frame = self.service._make_etf_aw_target_weight_frame(budget, panel)

        self.assertEqual(len(frame), 5)
        by_role = frame.set_index("sleeve_role")
        self.assertEqual(by_role.loc["cash", "target_weight_status"], "partial")
        self.assertEqual(
            set(
                by_role.loc[
                    ["equity_large", "equity_small", "bond", "gold"],
                    "target_weight_status",
                ]
            ),
            {"complete"},
        )
        cash_notes = json.loads(by_role.loc["cash", "quality_notes_json"])
        self.assertIn("insufficient_volatility_observations", cash_notes["reasons"])

    def test_multiple_insufficient_volatility_blocks_write(self) -> None:
        rebalance_date = date(2024, 7, 22)
        budget = pd.DataFrame(
            [
                self._budget_row(rebalance_date, role, 0.2, "complete")
                for role in ETF_AW_SLEEVE_ROLE_ORDER
            ]
        )
        panel = self._panel(rebalance_date, missing_role=None, observations=20)

        frame = self.service._make_etf_aw_target_weight_frame(budget, panel)

        self.assertTrue(frame.empty)

    def test_no_trade_band_keeps_small_diffs_but_not_large_diffs(self) -> None:
        constrained = {
            "equity_large": 0.201,
            "equity_small": 0.199,
            "bond": 0.20,
            "gold": 0.20,
            "cash": 0.20,
        }
        previous = {
            ETF_AW_SLEEVE_CODE_BY_ROLE["equity_large"]: 0.20,
            ETF_AW_SLEEVE_CODE_BY_ROLE["equity_small"]: 0.20,
            ETF_AW_SLEEVE_CODE_BY_ROLE["bond"]: 0.20,
            ETF_AW_SLEEVE_CODE_BY_ROLE["gold"]: 0.20,
            ETF_AW_SLEEVE_CODE_BY_ROLE["cash"]: 0.20,
        }

        target, drift = etl_service._apply_no_trade_band(constrained, previous)
        self.assertEqual(target[ETF_AW_SLEEVE_CODE_BY_ROLE["equity_large"]], 0.20)
        self.assertAlmostEqual(drift, 0.0)

        constrained["equity_large"] = 0.21
        constrained["cash"] = 0.191
        target, drift = etl_service._apply_no_trade_band(constrained, previous)
        self.assertNotAlmostEqual(
            target[ETF_AW_SLEEVE_CODE_BY_ROLE["equity_large"]],
            0.20,
            places=6,
        )
        self.assertAlmostEqual(sum(target.values()), 1.0, places=6)
        self.assertGreaterEqual(drift, 0.0)

    def test_no_trade_band_recaps_final_target_weight(self) -> None:
        constrained = {
            "equity_large": 0.148,
            "equity_small": 0.10,
            "bond": 0.202,
            "gold": 0.45,
            "cash": 0.10,
        }
        previous = {
            ETF_AW_SLEEVE_CODE_BY_ROLE["equity_large"]: 0.148,
            ETF_AW_SLEEVE_CODE_BY_ROLE["equity_small"]: 0.10,
            ETF_AW_SLEEVE_CODE_BY_ROLE["bond"]: 0.20,
            ETF_AW_SLEEVE_CODE_BY_ROLE["gold"]: 0.45,
            ETF_AW_SLEEVE_CODE_BY_ROLE["cash"]: 0.10,
        }

        target, drift = etl_service._apply_no_trade_band(constrained, previous)

        snapped = dict(previous)
        expected_drift = sum(abs(target[code] - snapped[code]) for code in snapped)
        self.assertAlmostEqual(sum(target.values()), 1.0, places=6)
        self.assertLessEqual(target[ETF_AW_SLEEVE_CODE_BY_ROLE["gold"]], 0.450001)
        self.assertAlmostEqual(drift, expected_drift, places=12)

    def test_bootstrap_writes_and_read_model_returns_latest_contract(self) -> None:
        rebalance_date = date(2024, 7, 22)
        self.service._write_etf_aw_risk_budget(
            pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role, 0.2, "complete")
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            )
        )
        self.service._write_etf_aw_sleeve_daily(
            self._panel(rebalance_date, missing_role=None, observations=70)
        )
        self._insert_rebalance(rebalance_date)

        result = self.service.run_bootstrap(
            "derived.etf_aw_target_weight.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertTrue(all(result["validation"].values()))
        latest = get_latest_etf_aw_target_weight(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest["schema_version"], "etf_aw_target_weight_v1")
        self.assertEqual(len(latest["weights"]), 5)
        self.assertAlmostEqual(latest["target_weight_sum"], 1.0, places=6)

    def test_read_model_filters_strategy_and_handles_mixed_status(self) -> None:
        rebalance_date = date(2024, 7, 22)
        default_frame = self._target_weight_frame(
            rebalance_date,
            {
                "equity_large": 0.19,
                "equity_small": 0.21,
                "bond": 0.20,
                "gold": 0.20,
                "cash": 0.20,
            },
        )
        other_frame = self._target_weight_frame(
            rebalance_date,
            {
                "equity_large": 0.20,
                "equity_small": 0.20,
                "bond": 0.20,
                "gold": 0.20,
                "cash": 0.20,
            },
            strategy_version="experimental_v2",
        )
        default_frame.loc[
            default_frame["sleeve_role"] == "cash", "target_weight_status"
        ] = "partial"
        self.service._write_etf_aw_target_weight(
            pd.concat([default_frame, other_frame], ignore_index=True)
        )

        latest = get_latest_etf_aw_target_weight(
            as_of_date=rebalance_date,
            strategy_name="etf_aw_v1",
            strategy_version="target_weight_inverse_vol_v1",
            lakehouse_root=self.lakehouse_root,
        )
        listed = list_etf_aw_target_weights(
            rebalance_date,
            rebalance_date,
            strategy_version="experimental_v2",
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest["strategy_version"], "target_weight_inverse_vol_v1")
        self.assertEqual(latest["target_weight_status"], "partial")
        self.assertIn("cash", latest["quality_notes"]["sleeves"])
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["strategy_version"], "experimental_v2")

    def test_latest_target_weight_requires_unambiguous_group(self) -> None:
        rebalance_date = date(2024, 7, 22)
        default_frame = self._target_weight_frame(
            rebalance_date,
            self._equal_role_weights(),
        )
        other_frame = self._target_weight_frame(
            rebalance_date,
            self._equal_role_weights(),
        )
        other_frame["calendar_name"] = "etf_aw_v2_monthly_post_20"
        self.service._write_etf_aw_target_weight(
            pd.concat([default_frame, other_frame], ignore_index=True)
        )

        ambiguous = get_latest_etf_aw_target_weight(
            as_of_date=rebalance_date,
            lakehouse_root=self.lakehouse_root,
        )
        selected = get_latest_etf_aw_target_weight(
            as_of_date=rebalance_date,
            calendar_name="etf_aw_v1_monthly_post_20",
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNone(ambiguous)
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["calendar_name"], "etf_aw_v1_monthly_post_20")

    def test_read_model_rejects_target_weight_missing_contract_columns(self) -> None:
        frame = self._target_weight_frame(date(2024, 7, 22), self._equal_role_weights())
        frame = frame.drop(columns=["raw_target_weight"])
        self.service._write_year_month_partition_upsert(
            dataset_name="derived.etf_aw_target_weight",
            zone=StorageZone.DERIVED,
            canonical=frame,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "sleeve_code",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "sleeve_role",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

        latest = get_latest_etf_aw_target_weight(
            as_of_date=date(2024, 7, 22),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNone(latest)

    def test_bootstrap_requires_calendar_aligned_risk_budget(self) -> None:
        rebalance_date = date(2024, 7, 22)
        self.service._write_etf_aw_risk_budget(
            pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role, 0.2, "complete")
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            )
        )
        self.service._write_etf_aw_sleeve_daily(
            self._panel(rebalance_date, missing_role=None, observations=70)
        )

        result = self.service.run_bootstrap(
            "derived.etf_aw_target_weight.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.FAILED.value)
        self.assertEqual(
            result["error_message"], "canonical rebalance calendar is missing"
        )

    def test_bootstrap_uses_previous_artifact_for_first_window_rebalance(self) -> None:
        previous_date = date(2024, 6, 24)
        rebalance_date = date(2024, 7, 22)
        previous_weights = {
            "equity_large": 0.19,
            "equity_small": 0.21,
            "bond": 0.20,
            "gold": 0.20,
            "cash": 0.20,
        }
        self.service._write_etf_aw_target_weight(
            self._target_weight_frame(previous_date, previous_weights)
        )
        self.service._write_etf_aw_risk_budget(
            pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role, 0.2, "complete")
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            )
        )
        self.service._write_etf_aw_sleeve_daily(
            self._panel(rebalance_date, missing_role=None, observations=70)
        )
        self._insert_rebalance(rebalance_date)

        result = self.service.run_bootstrap(
            "derived.etf_aw_target_weight.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = self._read_target_weight_file(2024, 7)
        self.assertTrue(frame["turnover_estimate"].notna().all())
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertNotIn("first_period_turnover_not_observable", notes["reasons"])

    def test_future_risk_budget_source_date_blocks_target_weight(self) -> None:
        rebalance_date = date(2024, 7, 22)
        budget = pd.DataFrame(
            [
                self._budget_row(rebalance_date, role, 0.2, "complete")
                for role in ETF_AW_SLEEVE_ROLE_ORDER
            ]
        )
        budget["source_regime_rebalance_date"] = date(2024, 7, 23)
        panel = self._panel(rebalance_date, missing_role=None, observations=70)

        frame = self.service._make_etf_aw_target_weight_frame(budget, panel)

        self.assertTrue(frame.empty)

    def test_previous_target_weight_does_not_cross_strategy_version(self) -> None:
        previous_date = date(2024, 6, 24)
        rebalance_date = date(2024, 7, 22)
        previous = self._target_weight_frame(
            previous_date,
            self._equal_role_weights(),
            strategy_version="experimental_v2",
        )
        budget = pd.DataFrame(
            [
                self._budget_row(rebalance_date, role, 0.2, "complete")
                for role in ETF_AW_SLEEVE_ROLE_ORDER
            ]
        )
        panel = self._panel(rebalance_date, missing_role=None, observations=70)

        frame = self.service._make_etf_aw_target_weight_frame(
            budget,
            panel,
            previous_target_weight=previous,
        )

        self.assertEqual(len(frame), 5)
        self.assertTrue(frame["turnover_estimate"].isna().all())
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertIn("first_period_turnover_not_observable", notes["reasons"])

    def test_backtest_kernel_consumes_target_weight_artifact(self) -> None:
        rebalance_date = date(2024, 7, 22)
        self.service._write_etf_aw_risk_budget(
            pd.DataFrame(
                [
                    self._budget_row(rebalance_date, role, 0.2, "complete")
                    for role in ETF_AW_SLEEVE_ROLE_ORDER
                ]
            )
        )
        self.service._write_etf_aw_sleeve_daily(
            self._panel(rebalance_date, missing_role=None, observations=70)
        )
        self._insert_rebalance(rebalance_date)
        self.service.run_bootstrap(
            "derived.etf_aw_target_weight.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        result = self.service.run_bootstrap(
            "derived.etf_aw_backtest_kernel.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_backtest_kernel"
            / "2024"
            / "07"
            / "part-00000.parquet"
        )
        self.assertEqual(set(frame["strategy_name"]), {"etf_aw_v1"})
        self.assertEqual(
            set(frame["strategy_version"]),
            {"target_weight_inverse_vol_v1"},
        )
        self.assertEqual(set(frame["weight_source_type"]), {"target_weight"})
        self.assertEqual(
            set(frame["source_weight_dataset"]), {"derived.etf_aw_target_weight"}
        )

    def test_update_plan_runs_target_weight_after_risk_budget(self) -> None:
        conn = duckdb.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE etl_source_watermarks (
                dataset_name VARCHAR PRIMARY KEY,
                latest_fetched_date DATE,
                updated_at TIMESTAMP
            )
        """
        )

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
            names.index("derived.etf_aw_risk_budget.build"),
            names.index("derived.etf_aw_target_weight.build"),
        )
        self.assertLess(
            names.index("derived.etf_aw_target_weight.build"),
            names.index("derived.etf_aw_baseline_weight.build"),
        )
        self.assertLess(
            names.index("derived.etf_aw_baseline_weight.build"),
            names.index("derived.etf_aw_backtest_kernel.build"),
        )
        self.assertLess(
            names.index("derived.etf_aw_backtest_kernel.build"),
            names.index("derived.etf_aw_backtest_kernel.baseline.build"),
        )
        self.assertLess(
            names.index("derived.etf_aw_backtest_kernel.baseline.build"),
            names.index("derived.etf_aw_monthly_explainability.build"),
        )

    def test_update_plan_executes_baseline_backtest_kernel(self) -> None:
        class FakeService:
            def __init__(self) -> None:
                self.bootstrap_names: list[str] = []
                self.baseline_calls: list[tuple[date, date, str]] = []

            def run_bootstrap(
                self, name: str, start: date | None = None, end: date | None = None
            ) -> dict:
                self.bootstrap_names.append(name)
                return {"status": RunStatus.SUCCESS.value, "records_written": 1}

            def _build_etf_aw_backtest_kernel(
                self,
                start: date,
                end: date,
                *,
                weight_source_type: str = "target_weight",
            ) -> dict:
                self.baseline_calls.append((start, end, weight_source_type))
                return {"status": RunStatus.SUCCESS.value, "records_written": 1}

        service = FakeService()
        plan = [
            update_module.UpdatePlanItem(
                kind="profile",
                name="derived.etf_aw_backtest_kernel.baseline.build",
                start=date(2024, 7, 1),
                end=date(2024, 7, 31),
            )
        ]

        results = update_module.execute_update_plan(service, plan)  # type: ignore[arg-type]

        self.assertEqual(results[0]["status"], RunStatus.SUCCESS.value)
        self.assertEqual(
            service.baseline_calls,
            [(date(2024, 7, 1), date(2024, 7, 31), "baseline")],
        )
        self.assertEqual(service.bootstrap_names, [])

    def test_backtest_kernel_declares_weight_dependencies(self) -> None:
        definition = build_derived_etf_aw_backtest_kernel_dataset()

        self.assertIn("derived.etf_aw_target_weight", definition.dependencies)
        self.assertIn("derived.etf_aw_baseline_weight", definition.dependencies)

    def _budget_row(
        self, rebalance_date: date, sleeve_role: str, tilted_budget: float, status: str
    ) -> dict:
        return {
            "schema_version": "etf_aw_risk_budget_v1",
            "contract_version": "etf_aw_risk_budget_contract_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "rebalance_date": rebalance_date,
            "strategy_name": "etf_aw_v1",
            "strategy_version": "risk_budget_v1",
            "confidence_score": 0.5,
            "effective_confidence_score": 0.5,
            "market_regime_label": "mixed",
            "budget_status": status,
            "budget_basis": "market_regime_tilt",
            "quality_notes_json": "{}",
            "source_strategy_context_rebalance_date": rebalance_date,
            "source_regime_rebalance_date": rebalance_date,
            "sleeve_role": sleeve_role,
            "base_budget": 0.2,
            "delta_budget": 0.0,
            "tilted_budget": tilted_budget,
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }

    def _panel(
        self,
        rebalance_date: date,
        *,
        missing_role: str | None,
        observations: int,
    ) -> pd.DataFrame:
        rows = []
        start = rebalance_date - timedelta(days=observations)
        dates = [start + timedelta(days=offset) for offset in range(observations + 11)]
        for role in ETF_AW_SLEEVE_ROLE_ORDER:
            count = 10 if role == missing_role else len(dates)
            for index, trade_date in enumerate(dates[:count]):
                rows.append(
                    {
                        "sleeve_code": ETF_AW_SLEEVE_CODE_BY_ROLE[role],
                        "sleeve_role": role,
                        "instrument_id": ETF_AW_SLEEVE_CODE_BY_ROLE[role],
                        "trade_date": trade_date,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0 + index * 0.01,
                        "adj_factor": 1.0,
                        "adj_close": 1.0 + index * 0.01,
                        "adj_pct_chg": 0.8 + (index % 5) * 0.1,
                        "vol": 1.0,
                        "amount": 1.0,
                        "source_name": "fixture",
                        "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
                        "quality_status": "pass",
                    }
                )
        return pd.DataFrame(rows)

    def _target_weight_frame(
        self,
        rebalance_date: date,
        weights_by_role: dict[str, float],
        *,
        strategy_version: str = "target_weight_inverse_vol_v1",
    ) -> pd.DataFrame:
        rows = []
        for role in ETF_AW_SLEEVE_ROLE_ORDER:
            rows.append(
                {
                    "schema_version": "etf_aw_target_weight_v1",
                    "contract_version": "etf_aw_target_weight_contract_v1",
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": rebalance_date,
                    "effective_date": rebalance_date,
                    "strategy_name": "etf_aw_v1",
                    "strategy_version": strategy_version,
                    "sleeve_code": ETF_AW_SLEEVE_CODE_BY_ROLE[role],
                    "sleeve_role": role,
                    "risk_budget": 0.2,
                    "volatility_estimate": 0.01,
                    "volatility_floor": 0.005,
                    "raw_target_weight": weights_by_role[role],
                    "constrained_target_weight": weights_by_role[role],
                    "target_weight": weights_by_role[role],
                    "target_weight_status": "complete",
                    "optimizer_name": "budgeted_inverse_vol",
                    "optimizer_basis": "fixture",
                    "turnover_estimate": None,
                    "quality_notes_json": "{}",
                    "source_risk_budget_rebalance_date": rebalance_date,
                    "source_sleeve_daily_max_trade_date": rebalance_date,
                    "ingested_at": pd.Timestamp("2024-06-24 15:00:00"),
                }
            )
        return pd.DataFrame(rows)

    def _equal_role_weights(self) -> dict[str, float]:
        return {role: 0.2 for role in ETF_AW_SLEEVE_ROLE_ORDER}

    def _read_target_weight_file(self, year: int, month: int) -> pd.DataFrame:
        return pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_target_weight"
            / f"{year:04d}"
            / f"{month:02d}"
            / "part-00000.parquet"
        )

    def _insert_rebalance(self, rebalance_date: date) -> None:
        self.conn.execute(
            """
            INSERT INTO canonical_rebalance_calendar (
                calendar_name, calendar_month, rebalance_date, effective_date, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                "etf_aw_v1_monthly_post_20",
                f"{rebalance_date.year:04d}-{rebalance_date.month:02d}",
                rebalance_date,
                rebalance_date,
                "{}",
            ],
        )


if __name__ == "__main__":
    unittest.main()
