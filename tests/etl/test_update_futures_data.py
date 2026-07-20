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

    def __init__(self, lakehouse_root: Path) -> None:
        self.lakehouse_root = lakehouse_root
        self.calls: list[tuple[str, dict]] = []

    def run_dataset_sync(self, dataset_name: str, request) -> SimpleNamespace:
        self.calls.append((dataset_name, request.context))
        if dataset_name == "market.futures_mapping":
            path = self.lakehouse_root / "normalized" / dataset_name / "2026" / "04"
            path.mkdir(parents=True, exist_ok=True)
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
            "fut_mapping -> persisted active contracts -> fut_daily", result.output
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
                ("market.futures_mapping", {"root_codes": ["M.DCE"]}),
                (
                    "market.futures_contract_daily",
                    {"contract_codes": ["M2605.DCE", "M2609.DCE"]},
                ),
            ],
        )
        self.assertEqual(
            [result["status"] for result in results], ["success", "success"]
        )
        self.assertEqual(results[-1]["contract_count"], 2)


if __name__ == "__main__":
    unittest.main()
