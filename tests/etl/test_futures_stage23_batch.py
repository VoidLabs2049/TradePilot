"""Tests for the commodity futures Stage 2/3 batch runner."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from dataclasses import replace

import pandas as pd

from tradepilot.etl.futures_stage23_batch import (
    render_stage23_summary,
    run_stage23_batch,
)


class FuturesStage23BatchTests(unittest.TestCase):
    """Verify all-root Stage 2/3 artifact generation and failure capture."""

    def test_batch_writes_stage2_success_and_blocked_quality_cards(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_dir = root / "docs"
            _write_batch_fixture(lakehouse_root)

            results = run_stage23_batch(
                lakehouse_root=lakehouse_root,
                root_codes=["M.DCE", "TA.ZCE"],
                docs_dir=docs_dir,
            )
            summary = render_stage23_summary(
                generated_at=pd.Timestamp("2026-07-24T00:00:00Z").to_pydatetime(),
                lakehouse_root=lakehouse_root,
                results=results,
            )
            stage2_report_exists = (
                docs_dir
                / "reports"
                / "stage-2"
                / "commodity-futures-stage-2-m-continuous-contract-report.md"
            ).exists()
            blocked_card = (
                docs_dir
                / "reports"
                / "stage-3"
                / "quality-cards"
                / "commodity-futures-stage-3-ta-zce-quality-card.md"
            ).read_text(encoding="utf-8")
            blocked_card_json = (
                docs_dir
                / "reports"
                / "stage-3"
                / "quality-cards"
                / "commodity-futures-stage-3-ta-zce-quality-card.json"
            ).read_text(encoding="utf-8")

        self.assertEqual([result.root_code for result in results], ["M.DCE", "TA.ZCE"])
        self.assertEqual(results[0].stage2_status, "pass")
        self.assertEqual(results[0].stage3_decision, "reject")
        self.assertIn("below minimum", results[0].decision_reasons[0])
        self.assertEqual(results[1].stage2_status, "fail")
        self.assertEqual(results[1].stage3_decision, "reject")
        self.assertIn("not_ready_for_stage4", summary)
        self.assertIn("TA.ZCE", summary)
        self.assertTrue(stage2_report_exists)
        self.assertIn("Stage 2 failed", blocked_card)
        self.assertIn('"decision": "reject"', blocked_card_json)

    def test_summary_is_not_ready_when_stage3_rejects(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_dir = root / "docs"
            _write_batch_fixture(lakehouse_root)
            result = run_stage23_batch(
                lakehouse_root=lakehouse_root,
                root_codes=["M.DCE"],
                docs_dir=docs_dir,
            )[0]
            result = replace(result, stage2_status="pass", stage3_decision="reject")

            summary = render_stage23_summary(
                generated_at=pd.Timestamp("2026-07-24T00:00:00Z").to_pydatetime(),
                lakehouse_root=lakehouse_root,
                results=[result],
            )

        self.assertIn("not_ready_for_stage4", summary)
        self.assertIn("Stage 3 reject", summary)


def _write_batch_fixture(lakehouse_root: Path) -> None:
    """Write two-root fixtures where one root lacks old-contract roll rows."""

    dates = [
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 1, 3),
        date(2025, 1, 6),
        date(2025, 1, 7),
        date(2025, 1, 8),
    ]
    mapping = pd.DataFrame(
        {
            "root_code": ["M.DCE"] * len(dates) + ["TA.ZCE"] * len(dates),
            "trade_date": dates + dates,
            "active_contract": [
                "M2501.DCE",
                "M2501.DCE",
                "M2505.DCE",
                "M2505.DCE",
                "M2505.DCE",
                "M2505.DCE",
                "TA2501.ZCE",
                "TA2501.ZCE",
                "TA2505.ZCE",
                "TA2505.ZCE",
                "TA2505.ZCE",
                "TA2505.ZCE",
            ],
            "raw_batch_id": [11] * len(dates) + [12] * len(dates),
        }
    )
    daily = pd.DataFrame(
        [
            _daily_row("M2501.DCE", date(2025, 1, 1), 1000.0),
            _daily_row("M2501.DCE", date(2025, 1, 2), 1010.0),
            _daily_row("M2501.DCE", date(2025, 1, 3), 1000.0),
            _daily_row("M2505.DCE", date(2025, 1, 3), 1100.0),
            _daily_row("M2505.DCE", date(2025, 1, 6), 1110.0),
            _daily_row("M2505.DCE", date(2025, 1, 7), 1120.0),
            _daily_row("M2505.DCE", date(2025, 1, 8), 1130.0),
            _daily_row("TA2501.ZCE", date(2025, 1, 1), 5000.0),
            _daily_row("TA2501.ZCE", date(2025, 1, 2), 5010.0),
            _daily_row("TA2505.ZCE", date(2025, 1, 3), 5100.0),
            _daily_row("TA2505.ZCE", date(2025, 1, 6), 5110.0),
            _daily_row("TA2505.ZCE", date(2025, 1, 7), 5120.0),
            _daily_row("TA2505.ZCE", date(2025, 1, 8), 5130.0),
        ]
    )
    instruments = pd.DataFrame(
        {
            "contract_code": ["M2505.DCE", "TA2505.ZCE"],
            "multiplier": [10.0, 5.0],
            "trade_unit": ["吨", "吨"],
            "quote_unit": ["人民币元/吨", "人民币元/吨"],
        }
    )
    _write_parquet(
        lakehouse_root,
        "normalized/market.futures_mapping/2025/01/part-00000.parquet",
        mapping,
    )
    _write_parquet(
        lakehouse_root,
        "normalized/market.futures_contract_daily/2025/01/part-00000.parquet",
        daily,
    )
    _write_parquet(
        lakehouse_root,
        "normalized/reference.futures_instruments/2025/01/part-00000.parquet",
        instruments,
    )


def _daily_row(contract_code: str, trade_date: date, close: float) -> dict[str, object]:
    """Return one futures daily fixture row."""

    return {
        "contract_code": contract_code,
        "trade_date": trade_date,
        "close": close,
        "settle": close + 1.0,
        "volume": 1000.0,
        "oi": 2000.0,
        "raw_batch_id": 21,
    }


def _write_parquet(
    lakehouse_root: Path, relative_path: str, frame: pd.DataFrame
) -> None:
    """Write one parquet fixture."""

    path = lakehouse_root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


if __name__ == "__main__":
    unittest.main()
