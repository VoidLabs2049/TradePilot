"""ETF all-weather CLI tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from click.testing import CliRunner
import duckdb
import pandas as pd

from tradepilot import db
from tradepilot.etf_aw.cli import main
from tradepilot.etl.models import StorageZone
from tradepilot.etl.service import ETLService
from tradepilot.etl.storage import write_dataset_parquet


class EtfAwCliTests(unittest.TestCase):
    """Verify CLI boundaries over frozen ETF all-weather artifacts."""

    def setUp(self) -> None:
        self._temp_dir = TemporaryDirectory()
        self.root = Path(self._temp_dir.name)
        self.db_path = self.root / "test.duckdb"
        self.lakehouse_root = self.root / "lakehouse"
        self.conn = duckdb.connect(str(self.db_path))
        db.initialize_schema(self.conn)
        self.service = ETLService(conn=self.conn, lakehouse_root=self.lakehouse_root)
        self.runner = CliRunner()

    def tearDown(self) -> None:
        self.conn.close()
        self._temp_dir.cleanup()

    def test_build_risk_budget_writes_artifact(self) -> None:
        rebalance_date = date(2024, 7, 22)
        self.service._write_etf_aw_strategy_context(
            pd.DataFrame([self._context_row(rebalance_date)])
        )
        self.service._write_etf_aw_regime_score(
            pd.DataFrame([self._regime_row(rebalance_date)])
        )

        result = self.runner.invoke(
            main,
            [
                "build-risk-budget",
                "--start-date",
                "2024-07-01",
                "--end-date",
                "2024-07-31",
                "--db-path",
                str(self.db_path),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("status=success", result.output)
        budget = pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_risk_budget"
            / "2024"
            / "07"
            / "part-00000.parquet"
        )
        self.assertEqual(len(budget), 5)

    def test_health_check_target_weight_fails_invalid_weight_sum(self) -> None:
        frame = self._target_weight_frame(date(2024, 7, 22))
        frame.loc[0, "target_weight"] = 0.4
        self.service._write_etf_aw_target_weight(frame)

        result = self.runner.invoke(
            main,
            [
                "health-check",
                "target-weight",
                "--start-date",
                "2024-07-01",
                "--end-date",
                "2024-07-31",
                "--db-path",
                str(self.db_path),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("FAIL weight_sums_valid", result.output)

    def test_backtest_report_outputs_json_summary(self) -> None:
        frame = pd.DataFrame(
            [
                self._backtest_row(
                    "daily_nav", date(2024, 7, 22), "net_value", 1.01, 1.01
                ),
                self._backtest_row(
                    "turnover", date(2024, 7, 22), "monthly_turnover", 0.0, None
                ),
                self._backtest_row(
                    "metric", date(2024, 7, 22), "total_return", 0.01, None
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 22),
                    "max_drawdown",
                    -0.02,
                    None,
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 22),
                    "annualized_volatility",
                    0.12,
                    None,
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 22),
                    "net_value",
                    1.005,
                    1.005,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "turnover",
                    date(2024, 7, 22),
                    "monthly_turnover",
                    0.0,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 22),
                    "total_return",
                    0.005,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 22),
                    "max_drawdown",
                    -0.03,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 22),
                    "annualized_volatility",
                    0.10,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
            ]
        )
        write_dataset_parquet(
            frame,
            "derived.etf_aw_backtest_kernel",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )

        result = self.runner.invoke(
            main,
            [
                "backtest-report",
                "--start-date",
                "2024-07-01",
                "--end-date",
                "2024-07-31",
                "--format",
                "json",
                "--db-path",
                str(self.db_path),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["strategy_name"], "etf_aw_v1")
        self.assertEqual(payload["daily_nav_rows"], 1)
        self.assertEqual(payload["metrics"]["total_return"], 0.01)
        self.assertEqual(len(payload["strategies"]), 2)
        self.assertAlmostEqual(payload["comparison"]["total_return_diff"], 0.005)
        self.assertAlmostEqual(
            payload["comparison"]["annualized_volatility_diff"], 0.02
        )

    def test_backtest_robustness_report_outputs_cost_scenarios(self) -> None:
        frame = pd.DataFrame(
            [
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 22),
                    "net_value",
                    1.01,
                    1.01,
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 23),
                    "net_value",
                    1.0201,
                    1.0201,
                ),
                self._backtest_row(
                    "turnover", date(2024, 7, 22), "monthly_turnover", 0.0, None
                ),
                self._backtest_row(
                    "turnover", date(2024, 7, 23), "monthly_turnover", 0.1, None
                ),
                self._backtest_row(
                    "metric", date(2024, 7, 23), "total_return", 0.0201, None
                ),
                self._backtest_row(
                    "metric", date(2024, 7, 31), "total_return", 9.0, None
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 23),
                    "annualized_volatility",
                    0.0,
                    None,
                ),
                self._backtest_row(
                    "metric", date(2024, 7, 23), "sharpe_ratio", 0.0, None
                ),
                self._backtest_row(
                    "metric", date(2024, 7, 23), "max_drawdown", 0.0, None
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 22),
                    "net_value",
                    1.01,
                    1.01,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 23),
                    "net_value",
                    1.0201,
                    1.0201,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "turnover",
                    date(2024, 7, 22),
                    "monthly_turnover",
                    0.0,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "turnover",
                    date(2024, 7, 23),
                    "monthly_turnover",
                    0.1,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 23),
                    "total_return",
                    0.0201,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 23),
                    "annualized_volatility",
                    0.0,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 23),
                    "sharpe_ratio",
                    0.0,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "metric",
                    date(2024, 7, 23),
                    "max_drawdown",
                    0.0,
                    None,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
            ]
        )
        write_dataset_parquet(
            frame,
            "derived.etf_aw_backtest_kernel",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )
        self.service._write_etf_aw_target_weight(
            self._target_weight_frame(date(2024, 7, 22))
        )
        self.service._write_etf_aw_baseline_weight(
            self._baseline_weight_frame(date(2024, 7, 22))
        )
        path = (
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_backtest_kernel"
            / "2024"
            / "07"
            / "part-00000.parquet"
        )
        stored = pd.read_parquet(path)
        stored.loc[
            (stored["observation_type"] == "daily_nav")
            & (stored["observation_date"] == date(2024, 7, 23))
            & (stored["weight_source_type"] == "target_weight"),
            "portfolio_return",
        ] = None
        stored.to_parquet(path, index=False)
        output = self.root / "robustness.json"

        result = self.runner.invoke(
            main,
            [
                "backtest-robustness-report",
                "--strategy-name",
                "etf_aw_v1",
                "--strategy-version",
                "target_weight_inverse_vol_v1",
                "--baseline-name",
                "static_inverse_vol",
                "--baseline-version",
                "static_inverse_vol_v1",
                "--start-date",
                "2024-07-01",
                "--end-date",
                "2024-07-31",
                "--format",
                "json",
                "--output",
                str(output),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(output.read_text())
        self.assertEqual(payload["report_status"], "complete")
        strategy = payload["strategies"][0]
        scenarios = {item["cost_scenario"]: item for item in strategy["scenarios"]}
        self.assertAlmostEqual(scenarios["gross"]["gross_total_return"], 0.0201)
        self.assertAlmostEqual(scenarios["gross"]["net_total_return"], 0.0201)
        self.assertAlmostEqual(
            scenarios["cost_10bps"]["estimated_cost_fraction_sum"], 0.0002
        )
        self.assertLess(
            scenarios["cost_10bps"]["net_total_return"],
            scenarios["gross"]["net_total_return"],
        )
        self.assertEqual(len(payload["comparisons"]), 4)

    def test_backtest_robustness_report_blocks_invalid_weight_sum(self) -> None:
        frame = pd.DataFrame(
            [
                self._backtest_row(
                    "daily_nav", date(2024, 7, 22), "net_value", 1.01, 1.01
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 22),
                    "net_value",
                    1.01,
                    1.01,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
            ]
        )
        write_dataset_parquet(
            frame,
            "derived.etf_aw_backtest_kernel",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )
        target_weight = self._target_weight_frame(date(2024, 7, 22))
        target_weight.loc[0, "target_weight"] = 0.3
        self.service._write_etf_aw_target_weight(target_weight)
        self.service._write_etf_aw_baseline_weight(
            self._baseline_weight_frame(date(2024, 7, 22))
        )
        output = self.root / "invalid-weight.json"

        result = self.runner.invoke(
            main,
            [
                "backtest-robustness-report",
                "--strategy-name",
                "etf_aw_v1",
                "--strategy-version",
                "target_weight_inverse_vol_v1",
                "--baseline-name",
                "static_inverse_vol",
                "--baseline-version",
                "static_inverse_vol_v1",
                "--start-date",
                "2024-07-01",
                "--end-date",
                "2024-07-31",
                "--format",
                "json",
                "--output",
                str(output),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        payload = json.loads(output.read_text())
        self.assertIn(
            "target weight is incomplete inside comparable range",
            payload["coverage"]["blocking_reasons"],
        )

    def test_backtest_robustness_report_blocks_missing_trade_date(self) -> None:
        frame = pd.DataFrame(
            [
                self._backtest_row(
                    "daily_nav", date(2024, 7, 22), "net_value", 1.01, 1.01
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 23),
                    "net_value",
                    1.0201,
                    1.0201,
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 22),
                    "net_value",
                    1.01,
                    1.01,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
                self._backtest_row(
                    "daily_nav",
                    date(2024, 7, 24),
                    "net_value",
                    1.0201,
                    1.0201,
                    strategy_name="static_inverse_vol",
                    strategy_version="static_inverse_vol_v1",
                    weight_source_type="baseline",
                    source_weight_dataset="derived.etf_aw_baseline_weight",
                ),
            ]
        )
        write_dataset_parquet(
            frame,
            "derived.etf_aw_backtest_kernel",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )
        self.service._write_etf_aw_target_weight(
            self._target_weight_frame(date(2024, 7, 22))
        )
        self.service._write_etf_aw_baseline_weight(
            self._baseline_weight_frame(date(2024, 7, 22))
        )
        output = self.root / "blocked.json"

        result = self.runner.invoke(
            main,
            [
                "backtest-robustness-report",
                "--strategy-name",
                "etf_aw_v1",
                "--strategy-version",
                "target_weight_inverse_vol_v1",
                "--baseline-name",
                "static_inverse_vol",
                "--baseline-version",
                "static_inverse_vol_v1",
                "--start-date",
                "2024-07-01",
                "--end-date",
                "2024-07-31",
                "--format",
                "json",
                "--output",
                str(output),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        payload = json.loads(output.read_text())
        self.assertEqual(payload["report_status"], "blocked")
        self.assertEqual(payload["comparisons"], [])
        self.assertIn(
            "strategy and baseline daily trade dates differ",
            payload["coverage"]["blocking_reasons"],
        )
        self.assertIsNone(payload["strategies"][0]["scenarios"][0]["net_total_return"])

    def test_backtest_robustness_report_help_omits_db_path(self) -> None:
        result = self.runner.invoke(main, ["backtest-robustness-report", "--help"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertNotIn("--db-path", result.output)

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
            "context_status": "complete",
            "readiness_level": "research_ready",
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

    def _regime_row(self, rebalance_date: date) -> dict:
        return {
            "schema_version": "etf_aw_regime_score_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": rebalance_date.strftime("%Y-%m"),
            "rebalance_date": rebalance_date,
            "scorer_name": "etf_aw_market_only_regime",
            "scorer_version": "v1",
            "input_snapshot_status": "complete",
            "market_regime_label": "risk_on",
            "market_score": 65.0,
            "confidence_score": 0.60,
            "confidence_level": "medium",
            "confidence_cap": 0.70,
            "scoring_status": "complete",
            "signal_summary": "risk_on",
            "signals_json": "[]",
            "quality_notes": "{}",
            "source_snapshot_rebalance_date": rebalance_date,
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }

    def _target_weight_frame(self, rebalance_date: date) -> pd.DataFrame:
        rows = []
        sleeves = [
            ("510300.SH", "equity_large"),
            ("159845.SZ", "equity_small"),
            ("511010.SH", "bond"),
            ("518850.SH", "gold"),
            ("159001.SZ", "cash"),
        ]
        for code, role in sleeves:
            rows.append(
                {
                    "schema_version": "etf_aw_target_weight_v1",
                    "contract_version": "etf_aw_target_weight_contract_v1",
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": rebalance_date,
                    "effective_date": rebalance_date,
                    "strategy_name": "etf_aw_v1",
                    "strategy_version": "target_weight_inverse_vol_v1",
                    "sleeve_code": code,
                    "sleeve_role": role,
                    "risk_budget": 0.2,
                    "volatility_estimate": 0.01,
                    "volatility_floor": 0.005,
                    "raw_target_weight": 0.2,
                    "constrained_target_weight": 0.2,
                    "target_weight": 0.2,
                    "target_weight_status": "complete",
                    "optimizer_name": "budgeted_inverse_vol",
                    "optimizer_basis": "fixture",
                    "turnover_estimate": None,
                    "quality_notes_json": "{}",
                    "source_risk_budget_rebalance_date": rebalance_date,
                    "source_sleeve_daily_max_trade_date": rebalance_date,
                    "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
                }
            )
        return pd.DataFrame(rows)

    def _baseline_weight_frame(self, rebalance_date: date) -> pd.DataFrame:
        rows = []
        sleeves = [
            ("510300.SH", "equity_large"),
            ("159845.SZ", "equity_small"),
            ("511010.SH", "bond"),
            ("518850.SH", "gold"),
            ("159001.SZ", "cash"),
        ]
        for code, role in sleeves:
            rows.append(
                {
                    "schema_version": "etf_aw_baseline_weight_v1",
                    "contract_version": "etf_aw_baseline_weight_contract_v1",
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": rebalance_date,
                    "effective_date": rebalance_date,
                    "baseline_name": "static_inverse_vol",
                    "baseline_version": "static_inverse_vol_v1",
                    "sleeve_code": code,
                    "sleeve_role": role,
                    "target_weight": 0.2,
                    "estimation_window_days": 63,
                    "min_observation_days": 42,
                    "volatility_estimate": 0.01,
                    "optimizer_name": "static_inverse_vol",
                    "optimizer_basis": "fixture",
                    "quality_notes_json": "{}",
                    "source_sleeve_daily_max_trade_date": rebalance_date,
                    "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
                }
            )
        return pd.DataFrame(rows)

    def _backtest_row(
        self,
        observation_type: str,
        observation_date: date,
        metric_name: str,
        metric_value: float,
        net_value: float | None,
        *,
        strategy_name: str = "etf_aw_v1",
        strategy_version: str = "target_weight_inverse_vol_v1",
        weight_source_type: str = "target_weight",
        source_weight_dataset: str = "derived.etf_aw_target_weight",
    ) -> dict:
        return {
            "schema_version": "etf_aw_backtest_kernel_v1",
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "weight_source_type": weight_source_type,
            "source_weight_dataset": source_weight_dataset,
            "observation_type": observation_type,
            "observation_date": observation_date,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "net_value": net_value,
            "portfolio_return": 0.01 if observation_type == "daily_nav" else None,
            "quality_notes_json": "{}",
            "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
        }


if __name__ == "__main__":
    unittest.main()
