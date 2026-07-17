"""Stage O shadow run CLI tests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from click.testing import CliRunner
import pandas as pd

from tradepilot.etf_aw.cli import main
from tradepilot.etf_aw.shadow_run import (
    ClosePriceItem,
    PriceSnapshotInput,
    build_shadow_observation_rows,
    derive_plan_status,
)
from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import write_dataset_parquet


class StageOShadowRunTests(unittest.TestCase):
    """Verify the Stage O shadow portfolio observation flow."""

    def setUp(self) -> None:
        self._temp_dir = TemporaryDirectory()
        self.root = Path(self._temp_dir.name)
        self.lakehouse_root = self.root / "lakehouse"
        self.output_dir = self.root / "out"
        self.runner = CliRunner()
        self.plan_id = "etf_aw_rp_stage_o_fixture"
        write_dataset_parquet(
            self._plan_frame(),
            "derived.etf_aw_rebalance_plan",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_shadow_run_cli_writes_observation_and_reports(self) -> None:
        account_path = self._write_json(
            "account.json",
            {
                "account_id": "paper-main",
                "snapshot_at": "2024-07-22T15:00:00+00:00",
                "cash": 1000.0,
                "total_asset": 11000.0,
                "positions": [
                    {
                        "symbol": "510300.SH",
                        "quantity": 1000,
                        "available_quantity": 1000,
                        "market_value": 1000.0,
                    },
                    {
                        "symbol": "159845.SZ",
                        "quantity": 1000,
                        "available_quantity": 1000,
                        "market_value": 2000.0,
                    },
                    {
                        "symbol": "513100.SH",
                        "quantity": 0,
                        "available_quantity": 0,
                        "market_value": 0.0,
                    },
                    {
                        "symbol": "511010.SH",
                        "quantity": 1000,
                        "available_quantity": 1000,
                        "market_value": 3000.0,
                    },
                    {
                        "symbol": "518850.SH",
                        "quantity": 1000,
                        "available_quantity": 1000,
                        "market_value": 2500.0,
                    },
                    {
                        "symbol": "159001.SZ",
                        "quantity": 1000,
                        "available_quantity": 1000,
                        "market_value": 1500.0,
                    },
                ],
            },
        )
        decision_path = self._write_json(
            "decision.json",
            {
                "plan_id": self.plan_id,
                "decision": "CONFIRMED",
                "decided_at": "2024-07-22T16:00:00+00:00",
                "operator": "tester",
                "note": "confirm fixture",
            },
        )
        fill_path = self._write_json(
            "fill.json",
            {
                "fill_id": "fill-001",
                "plan_id": self.plan_id,
                "symbol": "510300.SH",
                "order_side": "BUY",
                "fill_at": "2024-07-23T10:00:00+00:00",
                "fill_quantity": 100,
                "fill_price": 1.0,
                "source": "manual",
                "note": "fixture fill",
            },
        )
        price_path = self._write_json(
            "price.json",
            {
                "price_as_of": "2024-07-23T15:00:00+00:00",
                "prices": [
                    {
                        "symbol": "510300.SH",
                        "close_price": 1.1,
                        "price_trade_date": "2024-07-23",
                    },
                    {
                        "symbol": "159845.SZ",
                        "close_price": 2.0,
                        "price_trade_date": "2024-07-23",
                    },
                    {
                        "symbol": "513100.SH",
                        "close_price": 1.8,
                        "price_trade_date": "2024-07-23",
                    },
                    {
                        "symbol": "511010.SH",
                        "close_price": 3.0,
                        "price_trade_date": "2024-07-23",
                    },
                    {
                        "symbol": "518850.SH",
                        "close_price": 2.5,
                        "price_trade_date": "2024-07-23",
                    },
                    {
                        "symbol": "159001.SZ",
                        "close_price": 1.5,
                        "price_trade_date": "2024-07-23",
                    },
                ],
            },
        )
        baseline_path = self._write_json(
            "baseline.json",
            {
                "observation_date": "2024-07-23",
                "strategy_name": "static_inverse_vol",
                "strategy_version": "static_inverse_vol_v2",
                "baseline_daily_return": 0.005,
                "source_artifact": "derived.etf_aw_backtest_kernel",
            },
        )

        self._run_ok(
            [
                "initialize-shadow-account",
                "--plan-id",
                self.plan_id,
                "--account-snapshot",
                str(account_path),
            ]
        )
        self._run_ok(["record-paper-decision", "--decision", str(decision_path)])
        self._run_ok(["record-paper-fill", "--fill", str(fill_path)])
        self._run_ok(
            [
                "build-shadow-observation",
                "--account-id",
                "paper-main",
                "--observation-date",
                "2024-07-23",
                "--price-snapshot",
                str(price_path),
                "--baseline-observation",
                str(baseline_path),
                "--output-dir",
                str(self.output_dir),
            ]
        )

        observation = pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_shadow_observation"
            / "2024"
            / "07"
            / "part-00000.parquet"
        )
        self.assertEqual(len(observation), 6)
        self.assertAlmostEqual(float(observation.iloc[0]["cash"]), 900.0)
        self.assertAlmostEqual(float(observation.iloc[0]["total_asset"]), 11110.0)
        self.assertAlmostEqual(
            float(observation.iloc[0]["daily_return"]),
            11110.0 / 11000.0 - 1.0,
        )
        self.assertEqual(set(observation["derived_plan_status"]), {"PARTIALLY_FILLED"})

        self._run_ok(
            [
                "shadow-post-mortem",
                "--plan-id",
                self.plan_id,
                "--output-dir",
                str(self.output_dir),
            ]
        )
        post_mortem = json.loads(
            (self.output_dir / f"shadow-post-mortem-{self.plan_id}.json").read_text()
        )
        self.assertEqual(post_mortem["derived_status"], "PARTIALLY_FILLED")
        self.assertAlmostEqual(
            post_mortem["fill_quality"][0]["volume_weighted_fill_price"], 1.0
        )

        self._run_ok(
            [
                "shadow-performance-report",
                "--account-id",
                "paper-main",
                "--start-date",
                "2024-07-23",
                "--end-date",
                "2024-07-23",
                "--output-dir",
                str(self.output_dir),
            ]
        )
        report_json = (
            self.output_dir
            / "shadow-performance-report-paper-main-2024-07-23-2024-07-23.json"
        )
        report_html = report_json.with_suffix(".html")
        report = json.loads(report_json.read_text())
        self.assertEqual(report["header"]["mode"], "模拟盘")
        self.assertTrue(report["header"]["zero_fee_assumption"])
        self.assertEqual(report["integrity"]["observation_count"], 1)
        self.assertIn("shadow-performance-report", report_html.read_text())

    def test_duplicate_decision_is_blocked(self) -> None:
        account_path = self._write_minimal_account()
        decision_path = self._write_json(
            "decision.json",
            {
                "plan_id": self.plan_id,
                "decision": "CONFIRMED",
                "decided_at": "2024-07-22T16:00:00+00:00",
                "operator": "tester",
                "note": "",
            },
        )
        self._run_ok(
            [
                "initialize-shadow-account",
                "--plan-id",
                self.plan_id,
                "--account-snapshot",
                str(account_path),
            ]
        )
        self._run_ok(["record-paper-decision", "--decision", str(decision_path)])

        result = self.runner.invoke(
            main,
            [
                "record-paper-decision",
                "--decision",
                str(decision_path),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("duplicate_decision", result.stderr)

    def test_invalid_close_price_blocks_without_observation_write(self) -> None:
        account_path = self._write_minimal_account()
        decision_path = self._write_json(
            "decision.json",
            {
                "plan_id": self.plan_id,
                "decision": "CONFIRMED",
                "decided_at": "2024-07-22T16:00:00+00:00",
                "operator": "tester",
                "note": "",
            },
        )
        price_path = self._write_json(
            "bad-price.json",
            {
                "price_as_of": "2024-07-23T15:00:00+00:00",
                "prices": [
                    {"symbol": "510300.SH", "close_price": 1.1},
                ],
            },
        )
        self._run_ok(
            [
                "initialize-shadow-account",
                "--plan-id",
                self.plan_id,
                "--account-snapshot",
                str(account_path),
            ]
        )
        self._run_ok(["record-paper-decision", "--decision", str(decision_path)])

        result = self.runner.invoke(
            main,
            [
                "build-shadow-observation",
                "--account-id",
                "paper-main",
                "--observation-date",
                "2024-07-23",
                "--price-snapshot",
                str(price_path),
                "--output-dir",
                str(self.output_dir),
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("missing_or_invalid_close_price", result.stderr)
        self.assertFalse(
            (
                self.lakehouse_root / "derived" / "derived.etf_aw_shadow_observation"
            ).exists()
        )

    def test_update_local_shadow_writes_seed_and_missing_observations(self) -> None:
        self._write_local_shadow_inputs()

        self._run_ok(
            [
                "update-local-shadow",
                "--account-id",
                "local-paper",
                "--initial-asset",
                "100000",
                "--seed-date",
                "2024-07-22",
                "--end-date",
                "2024-07-24",
            ]
        )
        seed = pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_shadow_account_seed"
            / "local-paper"
            / "part-00000.parquet"
        )
        self.assertEqual(len(seed), 6)
        self.assertEqual(set(seed["account_id"]), {"local-paper"})
        self.assertEqual(
            set(seed["source_plan_id"]), {"local-target-weight:2024-07-22"}
        )
        self.assertAlmostEqual(float(seed.iloc[0]["total_asset"]), 100000.0)

        observation = pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_shadow_observation"
            / "2024"
            / "07"
            / "part-00000.parquet"
        )
        self.assertEqual(len(observation), 12)
        self.assertEqual(
            sorted(pd.to_datetime(observation["observation_date"]).dt.date.unique()),
            [date(2024, 7, 23), date(2024, 7, 24)],
        )
        self.assertFalse(observation["baseline_daily_return"].isna().any())

        result = self.runner.invoke(
            main,
            [
                "update-local-shadow",
                "--account-id",
                "local-paper",
                "--initial-asset",
                "100000",
                "--seed-date",
                "2024-07-22",
                "--end-date",
                "2024-07-24",
                "--lakehouse-root",
                str(self.lakehouse_root),
            ],
        )

        self.assertEqual(result.exit_code, 0, result.output + result.stderr)
        self.assertIn("observations_written=0", result.output)
        observation = pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_shadow_observation"
            / "2024"
            / "07"
            / "part-00000.parquet"
        )
        self.assertEqual(len(observation), 12)

    def test_observation_uses_price_as_of_for_active_plan(self) -> None:
        future_plan = self._plan_frame().copy()
        future_plan["plan_id"] = "future-plan"
        future_plan["target_weight"] = 0.4
        decisions = pd.DataFrame(
            [
                {
                    "plan_id": self.plan_id,
                    "account_id": "paper-main",
                    "decision": "CONFIRMED",
                    "decided_at": pd.Timestamp("2024-07-22 16:00:00+00:00"),
                },
                {
                    "plan_id": "future-plan",
                    "account_id": "paper-main",
                    "decision": "CONFIRMED",
                    "decided_at": pd.Timestamp("2024-07-24 10:00:00+00:00"),
                },
            ]
        )

        rows, _ = build_shadow_observation_rows(
            account_id="paper-main",
            observation_date=date(2024, 7, 23),
            price_snapshot=self._price_snapshot("2024-07-23T15:00:00+00:00"),
            baseline=None,
            note="",
            seed=self._seed_frame(),
            observations=pd.DataFrame(),
            decisions=decisions,
            fills=pd.DataFrame(),
            plans=pd.concat([self._plan_frame(), future_plan], ignore_index=True),
            generated_at=datetime(2024, 7, 25, tzinfo=timezone.utc),
        )

        self.assertEqual(set(rows["target_plan_id"]), {self.plan_id})

    def test_observation_fill_window_uses_previous_price_as_of(self) -> None:
        prior = self._prior_observation_frame(
            generated_at="2024-07-23T22:00:00+00:00",
            price_as_of="2024-07-23T15:00:00+00:00",
        )
        fills = pd.DataFrame(
            [
                {
                    "fill_id": "late-fill",
                    "account_id": "paper-main",
                    "plan_id": self.plan_id,
                    "symbol": "510300.SH",
                    "order_side": "BUY",
                    "fill_at": pd.Timestamp("2024-07-23 16:00:00+00:00"),
                    "fill_quantity": 100,
                    "fill_notional": 100.0,
                }
            ]
        )

        rows, review = build_shadow_observation_rows(
            account_id="paper-main",
            observation_date=date(2024, 7, 24),
            price_snapshot=self._price_snapshot("2024-07-24T15:00:00+00:00"),
            baseline=None,
            note="",
            seed=self._seed_frame(),
            observations=prior,
            decisions=pd.DataFrame(),
            fills=fills,
            plans=pd.DataFrame(),
        )

        equity_row = rows[rows["symbol"].astype(str).eq("510300.SH")].iloc[0]
        self.assertEqual(int(equity_row["quantity"]), 1100)
        self.assertAlmostEqual(float(equity_row["cash"]), 900.0)
        self.assertEqual(review["applied_fill_ids"], ["late-fill"])

    def test_cancelled_decision_derives_cancelled_status(self) -> None:
        decisions = pd.DataFrame(
            [
                {
                    "plan_id": self.plan_id,
                    "decision": "CANCELLED",
                    "decided_at": pd.Timestamp("2024-07-22 16:00:00+00:00"),
                }
            ]
        )

        self.assertEqual(
            derive_plan_status(self._plan_frame(), pd.DataFrame(), decisions),
            "CANCELLED",
        )

    def _run_ok(self, args: list[str]) -> None:
        full_args = [*args, "--lakehouse-root", str(self.lakehouse_root)]
        result = self.runner.invoke(main, full_args)
        self.assertEqual(result.exit_code, 0, result.output + result.stderr)

    def _write_json(self, name: str, payload: dict) -> Path:
        path = self.root / name
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return path

    def _write_minimal_account(self) -> Path:
        return self._write_json(
            "account.json",
            {
                "account_id": "paper-main",
                "snapshot_at": "2024-07-22T15:00:00+00:00",
                "cash": 1000.0,
                "total_asset": 11000.0,
                "positions": [
                    {
                        "symbol": symbol,
                        "quantity": 1000,
                        "available_quantity": 1000,
                        "market_value": value,
                    }
                    for symbol, value in [
                        ("510300.SH", 1000.0),
                        ("159845.SZ", 2000.0),
                        ("513100.SH", 0.0),
                        ("511010.SH", 3000.0),
                        ("518850.SH", 2500.0),
                        ("159001.SZ", 1500.0),
                    ]
                ],
            },
        )

    def _seed_frame(self) -> pd.DataFrame:
        rows = []
        for symbol, role, quantity, market_value in [
            ("510300.SH", "equity_large", 1000, 1000.0),
            ("159845.SZ", "equity_small", 1000, 2000.0),
            ("513100.SH", "equity_overseas", 0, 0.0),
            ("511010.SH", "bond", 1000, 3000.0),
            ("518850.SH", "gold", 1000, 2500.0),
            ("159001.SZ", "cash", 1000, 1500.0),
        ]:
            rows.append(
                {
                    "account_id": "paper-main",
                    "seed_at": pd.Timestamp("2024-07-22 15:00:00+00:00"),
                    "seed_date": date(2024, 7, 22),
                    "cash": 1000.0,
                    "total_asset": 11000.0,
                    "sleeve_role": role,
                    "symbol": symbol,
                    "quantity": quantity,
                    "market_value": market_value,
                }
            )
        return pd.DataFrame(rows)

    def _price_snapshot(self, price_as_of: str) -> PriceSnapshotInput:
        return PriceSnapshotInput(
            price_as_of=pd.Timestamp(price_as_of).to_pydatetime(),
            prices=[
                ClosePriceItem(symbol="510300.SH", close_price=1.0),
                ClosePriceItem(symbol="159845.SZ", close_price=2.0),
                ClosePriceItem(symbol="513100.SH", close_price=1.8),
                ClosePriceItem(symbol="511010.SH", close_price=3.0),
                ClosePriceItem(symbol="518850.SH", close_price=2.5),
                ClosePriceItem(symbol="159001.SZ", close_price=1.5),
            ],
        )

    def _prior_observation_frame(
        self, *, generated_at: str, price_as_of: str
    ) -> pd.DataFrame:
        rows = self._seed_frame().copy()
        rows["observation_date"] = date(2024, 7, 23)
        rows["generated_at"] = pd.Timestamp(generated_at)
        rows["review_metadata_json"] = json.dumps({"price_as_of": price_as_of})
        rows["close_price"] = [1.0, 2.0, 1.8, 3.0, 2.5, 1.5]
        rows["actual_weight"] = rows["market_value"] / 11000.0
        return rows

    def _write_local_shadow_inputs(self) -> None:
        target_rows = []
        sleeves = [
            ("510300.SH", "equity_large", 1.0),
            ("159845.SZ", "equity_small", 2.0),
            ("513100.SH", "equity_overseas", 1.8),
            ("511010.SH", "bond", 3.0),
            ("518850.SH", "gold", 2.5),
            ("159001.SZ", "cash", 1.5),
        ]
        for symbol, role, _ in sleeves:
            target_rows.append(
                {
                    "schema_version": "etf_aw_target_weight_v1",
                    "contract_version": "etf_aw_target_weight_contract_v1",
                    "calendar_name": "etf_aw_v2_monthly_post_20",
                    "rebalance_date": date(2024, 7, 22),
                    "effective_date": date(2024, 7, 22),
                    "strategy_name": "etf_aw_v2",
                    "strategy_version": "target_weight_inverse_vol_v2",
                    "sleeve_code": symbol,
                    "sleeve_role": role,
                    "target_weight": 0.0 if role == "equity_overseas" else 0.2,
                    "target_weight_status": "complete",
                    "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
                }
            )
        write_dataset_parquet(
            pd.DataFrame(target_rows),
            "derived.etf_aw_target_weight",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )

        daily_rows = []
        for trade_date, bump in [
            (date(2024, 7, 22), 0.0),
            (date(2024, 7, 23), 0.1),
            (date(2024, 7, 24), 0.2),
        ]:
            for symbol, role, close in sleeves:
                daily_rows.append(
                    {
                        "sleeve_code": symbol,
                        "sleeve_role": role,
                        "instrument_id": symbol,
                        "trade_date": trade_date,
                        "open": close + bump,
                        "high": close + bump,
                        "low": close + bump,
                        "close": close + bump,
                        "adj_factor": 1.0,
                        "adj_close": close + bump,
                        "pct_chg": 0.0,
                        "adj_pct_chg": 0.0,
                        "volume": 1000.0,
                        "amount": 1000.0,
                        "source_name": "test",
                        "ingested_at": pd.Timestamp("2024-07-24 15:00:00"),
                        "quality_status": "pass",
                    }
                )
        write_dataset_parquet(
            pd.DataFrame(daily_rows),
            "derived.etf_aw_sleeve_daily",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )

        baseline_rows = []
        for trade_date, portfolio_return, net_value in [
            (date(2024, 7, 23), 0.01, 1.01),
            (date(2024, 7, 24), 0.02, 1.0302),
        ]:
            baseline_rows.append(
                {
                    "schema_version": "etf_aw_backtest_kernel_v1",
                    "calendar_name": "etf_aw_v2_monthly_post_20",
                    "strategy_name": "static_inverse_vol",
                    "strategy_version": "static_inverse_vol_v2",
                    "observation_type": "daily_nav",
                    "observation_date": trade_date,
                    "metric_name": "net_value",
                    "metric_value": net_value,
                    "net_value": net_value,
                    "portfolio_return": portfolio_return,
                    "quality_notes_json": "{}",
                    "ingested_at": pd.Timestamp("2024-07-24 15:00:00"),
                    "weight_source_type": "baseline",
                    "source_weight_dataset": "derived.etf_aw_baseline_weight",
                }
            )
        write_dataset_parquet(
            pd.DataFrame(baseline_rows),
            "derived.etf_aw_backtest_kernel",
            StorageZone.DERIVED,
            [("year", 2024), ("month", "07")],
            lakehouse_root=self.lakehouse_root,
        )

    def _plan_frame(self) -> pd.DataFrame:
        rows = []
        sleeves = [
            ("510300.SH", "equity_large", "BUY", 200, 1.0),
            ("159845.SZ", "equity_small", "HOLD", 0, 2.0),
            ("513100.SH", "equity_overseas", "HOLD", 0, 1.8),
            ("511010.SH", "bond", "HOLD", 0, 3.0),
            ("518850.SH", "gold", "HOLD", 0, 2.5),
            ("159001.SZ", "cash", "SELL", 100, 1.5),
        ]
        for symbol, role, side, quantity, price in sleeves:
            rows.append(
                {
                    "schema_version": "etf_aw_rebalance_plan_v1",
                    "contract_version": "etf_aw_rebalance_plan_contract_v1",
                    "plan_id": self.plan_id,
                    "plan_date": date(2024, 7, 22),
                    "generated_at": pd.Timestamp("2024-07-22 15:30:00"),
                    "account_id": "paper-main",
                    "account_snapshot_at": "2024-07-22T15:00:00+00:00",
                    "price_as_of": "2024-07-22T15:00:00+00:00",
                    "target_weight_rebalance_date": date(2024, 7, 22),
                    "strategy_name": "etf_aw_v2",
                    "strategy_version": "target_weight_inverse_vol_v2",
                    "sleeve_role": role,
                    "symbol": symbol,
                    "target_weight": 0.0 if role == "equity_overseas" else 0.2,
                    "current_quantity": 1000,
                    "available_quantity": 1000,
                    "current_market_value": 1000.0,
                    "latest_price": price,
                    "target_notional": 2000.0,
                    "raw_delta_quantity": quantity,
                    "lot_size": 100,
                    "order_side": side,
                    "order_quantity": quantity,
                    "estimated_notional": quantity * price,
                    "cash_buffer_ratio": 0.01,
                    "plan_status": "DRAFT",
                    "blocking_reasons_json": "[]",
                    "warnings_json": "[]",
                }
            )
        return pd.DataFrame(rows)


if __name__ == "__main__":
    unittest.main()
