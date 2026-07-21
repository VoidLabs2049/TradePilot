"""Tests for the commodity futures update CLI."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

import pandas as pd
from click.testing import CliRunner

from tradepilot.etl.models import RunStatus
from tradepilot.etl.update_futures_data import (
    FUTURES_ROOT_CODES,
    active_contract_codes,
    main,
    sync_futures_data,
)


class FakeETLService:
    """Persist a mapping fixture and record dataset sync requests."""

    def __init__(self, lakehouse_root: Path, *, empty_mapping: bool = False) -> None:
        self.lakehouse_root = lakehouse_root
        self.empty_mapping = empty_mapping
        self.calls: list[tuple[str, dict]] = []

    def run_dataset_sync(self, dataset_name: str, request) -> SimpleNamespace:
        self.calls.append((dataset_name, request.context))
        if dataset_name == "reference.futures_instruments":
            records = 1
        elif dataset_name == "market.futures_mapping":
            path = self.lakehouse_root / "normalized" / dataset_name / "2026" / "04"
            path.mkdir(parents=True, exist_ok=True)
            if self.empty_mapping:
                records = 0
            else:
                pd.DataFrame(
                    {
                        "root_code": ["M.DCE", "M.DCE"],
                        "trade_date": [date(2026, 4, 8), date(2026, 4, 9)],
                        "active_contract": ["M2605.DCE", "M2609.DCE"],
                    }
                ).to_parquet(path / "part-00000.parquet", index=False)
                records = 2
        else:
            records = 4
        return SimpleNamespace(
            dataset_name=dataset_name,
            status=RunStatus.SUCCESS,
            records_written=records,
            error_message=None,
        )


class UpdateFuturesDataTests(unittest.TestCase):
    """Verify futures sync planning and mapping-driven contract discovery."""

    def test_dry_run_defaults_to_nine_candidate_roots(self) -> None:
        result = CliRunner().invoke(
            main,
            ["--dry-run", "--end", "2026-04-09"],
        )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn(f"roots={','.join(FUTURES_ROOT_CODES)}", result.output)
        self.assertIn(
            "trade_cal -> fut_basic -> fut_mapping -> persisted active contracts -> fut_daily",
            result.output,
        )

    def test_cli_rejects_unknown_root(self) -> None:
        result = CliRunner().invoke(main, ["--dry-run", "--roots", "XX.DCE"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("unsupported roots: XX.DCE", result.output)

    def test_sync_uses_persisted_mapping_contracts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            service = FakeETLService(lakehouse_root)

            results = sync_futures_data(
                service=service,
                root_codes=["M.DCE"],
                start=date(2026, 4, 8),
                end=date(2026, 4, 9),
                lakehouse_root=lakehouse_root,
            )

            contracts = active_contract_codes(
                lakehouse_root=lakehouse_root,
                root_code="M.DCE",
                start=date(2026, 4, 8),
                end=date(2026, 4, 9),
            )

        self.assertEqual(contracts, ["M2605.DCE", "M2609.DCE"])
        self.assertEqual(
            service.calls,
            [
                ("reference.trading_calendar", {"exchanges": ["DCE"]}),
                ("reference.futures_instruments", {"exchanges": ["DCE"]}),
                ("market.futures_mapping", {"root_codes": ["M.DCE"]}),
                (
                    "market.futures_contract_daily",
                    {"contract_codes": ["M2605.DCE", "M2609.DCE"]},
                ),
            ],
        )
        self.assertEqual(
            [result["status"] for result in results],
            ["success", "success", "success", "success"],
        )
        self.assertEqual(results[-1]["contract_count"], 2)

    def test_sync_reports_failure_when_mapping_has_no_active_contracts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            service = FakeETLService(lakehouse_root, empty_mapping=True)

            results = sync_futures_data(
                service=service,
                root_codes=["M.DCE"],
                start=date(2026, 4, 8),
                end=date(2026, 4, 9),
                lakehouse_root=lakehouse_root,
            )

        self.assertEqual(results[-1]["status"], "failed")
        self.assertEqual(
            results[-1]["error_message"], "no active contracts found in mapping"
        )
        self.assertEqual(
            [call[0] for call in service.calls],
            [
                "reference.trading_calendar",
                "reference.futures_instruments",
                "market.futures_mapping",
            ],
        )

    def test_sync_covers_all_default_root_exchanges(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            service = FakeETLService(lakehouse_root)

            sync_futures_data(
                service=service,
                root_codes=list(FUTURES_ROOT_CODES),
                start=date(2026, 4, 8),
                end=date(2026, 4, 9),
                lakehouse_root=lakehouse_root,
            )

        self.assertEqual(
            service.calls[0],
            (
                "reference.trading_calendar",
                {"exchanges": ["SHFE", "DCE", "INE", "CZCE"]},
            ),
        )
        self.assertEqual(
            service.calls[1],
            (
                "reference.futures_instruments",
                {"exchanges": ["SHFE", "DCE", "INE", "CZCE"]},
            ),
        )

    def test_active_contract_codes_filters_root_and_cross_year_window(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            for year, rows in {
                "2025": {
                    "root_code": ["M.DCE", "CU.SHF"],
                    "trade_date": [date(2025, 12, 31), date(2025, 12, 31)],
                    "active_contract": ["M2605.DCE", "CU2602.SHF"],
                },
                "2026": {
                    "root_code": ["M.DCE", "M.DCE"],
                    "trade_date": [date(2026, 1, 2), date(2026, 2, 2)],
                    "active_contract": ["M2609.DCE", "M2609.DCE"],
                },
            }.items():
                path = (
                    lakehouse_root
                    / "normalized"
                    / "market.futures_mapping"
                    / year
                    / "01"
                )
                path.mkdir(parents=True, exist_ok=True)
                pd.DataFrame(rows).to_parquet(path / "part-00000.parquet", index=False)

            contracts = active_contract_codes(
                lakehouse_root=lakehouse_root,
                root_code="M.DCE",
                start=date(2025, 12, 30),
                end=date(2026, 1, 31),
            )

        self.assertEqual(contracts, ["M2605.DCE", "M2609.DCE"])


if __name__ == "__main__":
    unittest.main()
