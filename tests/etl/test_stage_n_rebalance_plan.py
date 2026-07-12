"""Stage N ETF all-weather rebalance plan tests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from click.testing import CliRunner
import pandas as pd

from tradepilot.etf_aw.cli import main
from tradepilot.etf_aw.rebalance_plan import (
    AccountSnapshot,
    PriceSnapshot,
    PriceSnapshotItem,
    build_rebalance_plan,
)
from tradepilot.etl.datasets import build_derived_etf_aw_rebalance_plan_dataset
from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import write_dataset_parquet


class StageNRebalancePlanTests(unittest.TestCase):
    """Verify Stage N simulated rebalance plan behavior."""

    def test_build_rebalance_plan_generates_buy_sell_and_hold(self) -> None:
        frame, summary, diagnostics = build_rebalance_plan(
            target_weight=self._target_weight_frame(),
            account=self._account_snapshot(cash=30000.0),
            prices=self._price_snapshot(),
            plan_date=date(2024, 7, 23),
            generated_at=datetime(2024, 7, 23, 8, 0, tzinfo=timezone.utc),
        )

        self.assertFalse(diagnostics.blocked)
        self.assertEqual(summary["plan_id"], "etf_aw_rp_132ba92e529c28b2")
        by_symbol = frame.set_index("symbol")
        self.assertEqual(by_symbol.loc["510300.SH", "order_side"], "BUY")
        self.assertEqual(by_symbol.loc["510300.SH", "order_quantity"], 900)
        self.assertEqual(by_symbol.loc["511010.SH", "order_side"], "SELL")
        self.assertEqual(by_symbol.loc["511010.SH", "order_quantity"], 1000)
        self.assertEqual(by_symbol.loc["159845.SZ", "order_side"], "HOLD")
        self.assertIn(
            "below_lot_size",
            json.loads(by_symbol.loc["159845.SZ", "warnings_json"]),
        )
        self.assertAlmostEqual(summary["estimated_buy_notional"], 28000.0)
        self.assertAlmostEqual(summary["estimated_sell_proceeds"], 10000.0)
        self.assertAlmostEqual(summary["cash_after_plan"], 12000.0)
        self.assertTrue(frame["plan_status"].eq("DRAFT").all())

    def test_sell_above_available_quantity_blocks_whole_plan(self) -> None:
        account = self._account_snapshot(cash=30000.0)
        account.positions[2].available_quantity = 900

        frame, _, diagnostics = build_rebalance_plan(
            target_weight=self._target_weight_frame(),
            account=account,
            prices=self._price_snapshot(),
            plan_date=date(2024, 7, 23),
        )

        self.assertTrue(frame.empty)
        self.assertIn("insufficient_available_quantity", diagnostics.blocking_reasons)

    def test_cash_buffer_shortfall_blocks_whole_plan(self) -> None:
        frame, summary, diagnostics = build_rebalance_plan(
            target_weight=self._target_weight_frame(),
            account=self._account_snapshot(cash=1000.0),
            prices=self._price_snapshot(),
            plan_date=date(2024, 7, 23),
        )

        self.assertTrue(frame.empty)
        self.assertIn("insufficient_cash_buffer", diagnostics.blocking_reasons)
        self.assertLess(summary["cash_after_plan"], summary["required_cash_buffer"])

    def test_invalid_inputs_block_with_stable_reason_codes(self) -> None:
        weights = self._target_weight_frame().iloc[:-1].copy()
        bad_account = self._account_snapshot(cash=30000.0)
        bad_account.positions = bad_account.positions[:-1]
        bad_prices = PriceSnapshot(
            price_as_of="2024-07-23T15:00:00+08:00",
            items=[
                PriceSnapshotItem(
                    symbol="510300.SH", latest_price=10.0, source="fixture"
                )
            ],
        )

        frame, _, diagnostics = build_rebalance_plan(
            target_weight=weights,
            account=bad_account,
            prices=bad_prices,
            plan_date=date(2024, 7, 23),
        )

        self.assertTrue(frame.empty)
        self.assertIn("incomplete_target_weight", diagnostics.blocking_reasons)
        self.assertIn("missing_position", diagnostics.blocking_reasons)
        self.assertIn("missing_or_invalid_price", diagnostics.blocking_reasons)

    def test_market_value_mismatch_keeps_snapshot_value_and_warns(self) -> None:
        account = self._account_snapshot(cash=30000.0)
        account.positions[0].market_value = 9000.0

        frame, _, diagnostics = build_rebalance_plan(
            target_weight=self._target_weight_frame(),
            account=account,
            prices=self._price_snapshot(),
            plan_date=date(2024, 7, 23),
        )

        by_symbol = frame.set_index("symbol")
        self.assertEqual(by_symbol.loc["510300.SH", "current_market_value"], 9000.0)
        self.assertIn("market_value_price_mismatch", diagnostics.warnings)

    def test_cli_writes_artifact_review_files_and_blocks_duplicate(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            lakehouse_root = root / "lakehouse"
            output_dir = root / "review"
            account_path = root / "account.json"
            price_path = root / "prices.json"
            account_path.write_text(
                json.dumps(self._account_json(), ensure_ascii=False),
                encoding="utf-8",
            )
            price_path.write_text(
                json.dumps(self._price_json(), ensure_ascii=False),
                encoding="utf-8",
            )
            write_dataset_parquet(
                self._target_weight_frame(),
                "derived.etf_aw_target_weight",
                StorageZone.DERIVED,
                [("year", 2024), ("month", "07")],
                lakehouse_root=lakehouse_root,
            )
            runner = CliRunner()

            result = runner.invoke(
                main,
                [
                    "build-rebalance-plan",
                    "--account-snapshot",
                    str(account_path),
                    "--price-snapshot",
                    str(price_path),
                    "--plan-date",
                    "2024-07-23",
                    "--output-dir",
                    str(output_dir),
                    "--lakehouse-root",
                    str(lakehouse_root),
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn("status=DRAFT", result.output)
            artifact = pd.read_parquet(
                lakehouse_root
                / "derived"
                / "derived.etf_aw_rebalance_plan"
                / "2024"
                / "07"
                / "part-00000.parquet"
            )
            self.assertEqual(len(artifact), 5)
            self.assertTrue(artifact["plan_status"].eq("DRAFT").all())
            self.assertFalse(
                {"broker_account", "filled_quantity"} & set(artifact.columns)
            )
            json_files = list(output_dir.glob("*.json"))
            md_files = list(output_dir.glob("*.md"))
            self.assertEqual(len(json_files), 1)
            self.assertEqual(len(md_files), 1)
            payload = json.loads(json_files[0].read_text(encoding="utf-8"))
            self.assertEqual(payload["rows"][0]["plan_id"], artifact.iloc[0]["plan_id"])
            self.assertIn(
                "模拟盘草案，需人工判断，未提交订单",
                md_files[0].read_text(encoding="utf-8"),
            )

            duplicate = runner.invoke(
                main,
                [
                    "build-rebalance-plan",
                    "--account-snapshot",
                    str(account_path),
                    "--price-snapshot",
                    str(price_path),
                    "--plan-date",
                    "2024-07-23",
                    "--output-dir",
                    str(output_dir),
                    "--lakehouse-root",
                    str(lakehouse_root),
                ],
            )

            self.assertNotEqual(duplicate.exit_code, 0)
            self.assertIn("duplicate_active_plan", duplicate.output)
            stored_after_duplicate = pd.read_parquet(
                lakehouse_root
                / "derived"
                / "derived.etf_aw_rebalance_plan"
                / "2024"
                / "07"
                / "part-00000.parquet"
            )
            self.assertEqual(len(stored_after_duplicate), 5)

    def test_rebalance_plan_dataset_definition_is_frozen_draft_contract(self) -> None:
        definition = build_derived_etf_aw_rebalance_plan_dataset()

        self.assertEqual(definition.dataset_name, "derived.etf_aw_rebalance_plan")
        self.assertEqual(definition.canonical_schema_name, "etf_aw_rebalance_plan_v1")
        self.assertIn("derived.etf_aw_target_weight", definition.dependencies)
        self.assertIn("no broker", definition.timing_semantics)

    def _target_weight_frame(self) -> pd.DataFrame:
        sleeves = [
            ("510300.SH", "equity_large"),
            ("159845.SZ", "equity_small"),
            ("511010.SH", "bond"),
            ("518850.SH", "gold"),
            ("159001.SZ", "cash"),
        ]
        return pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": date(2024, 7, 22),
                    "strategy_name": "etf_aw_v1",
                    "strategy_version": "target_weight_inverse_vol_v1",
                    "sleeve_code": code,
                    "sleeve_role": role,
                    "target_weight": 0.2,
                }
                for code, role in sleeves
            ]
        )

    def _account_snapshot(self, cash: float) -> AccountSnapshot:
        return AccountSnapshot.model_validate(self._account_json(cash=cash))

    def _account_json(self, cash: float = 30000.0) -> dict:
        return {
            "account_id": "paper-account-1",
            "snapshot_at": "2024-07-23T15:00:00+08:00",
            "cash": cash,
            "total_asset": 100000.0,
            "positions": [
                self._position("510300.SH", 1000, 1000, 10000.0),
                self._position("159845.SZ", 2000, 2000, 20000.0),
                self._position("511010.SH", 3000, 3000, 30000.0),
                self._position("518850.SH", 1980, 1980, 19800.0),
                self._position("159001.SZ", 0, 0, 0.0),
            ],
        }

    def _position(
        self, symbol: str, quantity: int, available: int, market_value: float
    ) -> dict:
        return {
            "symbol": symbol,
            "quantity": quantity,
            "available_quantity": available,
            "market_value": market_value,
        }

    def _price_snapshot(self) -> PriceSnapshot:
        return PriceSnapshot.model_validate(self._price_json())

    def _price_json(self) -> dict:
        return {
            "price_as_of": "2024-07-23T15:00:00+08:00",
            "prices": [
                {"symbol": symbol, "latest_price": 10.0, "source": "fixture"}
                for symbol in [
                    "510300.SH",
                    "159845.SZ",
                    "511010.SH",
                    "518850.SH",
                    "159001.SZ",
                ]
            ],
        }


if __name__ == "__main__":
    unittest.main()
