"""Tests for full-history external ETF source exports."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tools.etl_review import export_etf_aw_sources as source_module
from tools.etl_review.export_etf_aw_full_history import main
from tools.etl_review.export_etf_aw_sources import FetchContext


class ExportEtfAwFullHistoryTests(unittest.TestCase):
    """Verify full-history exports aggregate per-code comparisons."""

    def test_full_history_writes_aggregate_outputs(self) -> None:
        def source_a(context: FetchContext) -> pd.DataFrame:
            return self._source_frame(context, "a", 1.1, 100)

        def source_b(context: FetchContext) -> pd.DataFrame:
            return self._source_frame(context, "b", 1.2, 100)

        with TemporaryDirectory() as temp_dir:
            original = source_module._SOURCE_FETCHERS.copy()
            source_module._SOURCE_FETCHERS["a"] = source_a
            source_module._SOURCE_FETCHERS["b"] = source_b
            try:
                result = CliRunner().invoke(
                    main,
                    [
                        "--codes",
                        "511010,510300",
                        "--start",
                        "2026-06-01",
                        "--end",
                        "2026-06-07",
                        "--sources",
                        "a,b",
                        "--out-dir",
                        temp_dir,
                    ],
                )
            finally:
                source_module._SOURCE_FETCHERS.clear()
                source_module._SOURCE_FETCHERS.update(original)

            self.assertEqual(result.exit_code, 0, result.output)
            root = Path(temp_dir) / "2026-06-01_2026-06-07"
            summary = pd.read_csv(root / "all_codes_summary.csv")
            mismatch = pd.read_csv(root / "all_codes_mismatch_rows.csv")
            manifest = pd.read_csv(root / "run_manifest.csv")

        self.assertEqual(set(summary["etf_code"]), {"511010.SH", "510300.SH"})
        self.assertEqual(len(mismatch), 2)
        self.assertEqual(set(manifest["status"]), {"success"})

    def test_full_history_records_code_level_failure(self) -> None:
        def failing_source(context: FetchContext) -> pd.DataFrame:
            raise RuntimeError("source unavailable")

        with TemporaryDirectory() as temp_dir:
            original = source_module._SOURCE_FETCHERS.copy()
            source_module._SOURCE_FETCHERS["bad_source"] = failing_source
            try:
                result = CliRunner().invoke(
                    main,
                    [
                        "--codes",
                        "511010",
                        "--start",
                        "2026-06-01",
                        "--end",
                        "2026-06-07",
                        "--sources",
                        "bad_source",
                        "--out-dir",
                        temp_dir,
                    ],
                )
            finally:
                source_module._SOURCE_FETCHERS.clear()
                source_module._SOURCE_FETCHERS.update(original)

            self.assertNotEqual(result.exit_code, 0)
            errors = pd.read_csv(
                Path(temp_dir) / "2026-06-01_2026-06-07" / "all_codes_errors.csv"
            )

        self.assertEqual(errors["etf_code"].tolist(), ["511010.SH"])

    def _source_frame(
        self,
        context: FetchContext,
        source: str,
        close: float,
        volume: float,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "source": [source],
                "etf_code": [context.etf_code],
                "trade_date": ["2026-06-05"],
                "open": [1.0],
                "close": [close],
                "high": [1.3],
                "low": [0.9],
                "volume": [volume],
                "volume_unit": ["hand"],
                "amount": [1000.0],
                "amount_unit": ["CNY"],
                "pct_chg": [1.0],
                "source_url": ["https://example.test"],
                "source_note": ["test"],
            }
        )


if __name__ == "__main__":
    unittest.main()
