"""Tests for external ETF source CSV export helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tools.etl_review import export_etf_aw_sources as module
from tools.etl_review.export_etf_aw_sources import FetchContext, main


class ExportEtfAwSourcesTests(unittest.TestCase):
    """Verify source exports normalize rows into comparable CSV columns."""

    def test_export_sources_writes_per_source_and_combined_csv(self) -> None:
        def fake_fetcher(context: FetchContext) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "source": ["fake"],
                    "etf_code": [context.etf_code],
                    "trade_date": ["2026-06-05"],
                    "open": [1.0],
                    "close": [1.1],
                    "high": [1.2],
                    "low": [0.9],
                    "volume": [100.0],
                    "volume_unit": ["hand"],
                    "amount": [1000.0],
                    "amount_unit": ["CNY"],
                    "pct_chg": [1.0],
                    "source_url": ["https://example.test"],
                    "source_note": ["test"],
                }
            )

        with TemporaryDirectory() as temp_dir:
            original = module._SOURCE_FETCHERS.copy()
            module._SOURCE_FETCHERS["fake"] = fake_fetcher
            try:
                result = CliRunner().invoke(
                    main,
                    [
                        "511010",
                        "2026-06-01",
                        "2026-06-07",
                        "--sources",
                        "fake",
                        "--out-dir",
                        temp_dir,
                    ],
                )
            finally:
                module._SOURCE_FETCHERS.clear()
                module._SOURCE_FETCHERS.update(original)

            self.assertEqual(result.exit_code, 0, result.output)
            export_dir = Path(temp_dir) / "511010_SH_2026-06-01_2026-06-07"
            self.assertTrue(
                (export_dir / "fake_511010_SH_2026-06-01_2026-06-07.csv").exists()
            )
            combined = pd.read_csv(export_dir / "combined.csv")

        self.assertEqual(combined["source"].tolist(), ["fake"])
        self.assertEqual(combined["etf_code"].tolist(), ["511010.SH"])

    def test_parse_sources_rejects_unknown_source(self) -> None:
        result = CliRunner().invoke(
            main,
            ["511010", "2026-06-01", "2026-06-07", "--sources", "bad"],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("unknown sources", result.output)

    def test_source_helpers_normalize_code_and_units(self) -> None:
        self.assertEqual(module._normalize_etf_code("511010"), "511010.SH")
        self.assertEqual(module._market_symbol("159845.SZ"), "sz159845")
        self.assertEqual(module._eastmoney_secid("510300.SH"), "1.510300")
        self.assertEqual(module._parse_volume_text("2.96M"), 2_960_000)

    def test_date_windows_split_long_history_for_tencent(self) -> None:
        windows = module._date_windows(
            date(2026, 1, 1),
            date(2026, 1, 10),
            4,
        )

        self.assertEqual(
            windows,
            [
                (date(2026, 1, 1), date(2026, 1, 4)),
                (date(2026, 1, 5), date(2026, 1, 8)),
                (date(2026, 1, 9), date(2026, 1, 10)),
            ],
        )

    def test_local_lakehouse_source_reads_sleeve_daily(self) -> None:
        with TemporaryDirectory() as temp_dir:
            original_root = module.LAKEHOUSE_ROOT
            lakehouse = Path(temp_dir) / "lakehouse"
            dataset = (
                lakehouse / "derived" / "derived.etf_aw_sleeve_daily" / "2026" / "06"
            )
            dataset.mkdir(parents=True)
            pd.DataFrame(
                {
                    "trade_date": ["2026-06-05"],
                    "sleeve_code": ["511010.SH"],
                    "open": [141.512],
                    "close": [141.5],
                    "high": [141.565],
                    "low": [141.481],
                    "volume": [29553.0],
                    "amount": [418197.984],
                    "pct_chg": [0.0],
                }
            ).to_parquet(dataset / "part-00000.parquet", index=False)
            module.LAKEHOUSE_ROOT = lakehouse
            try:
                frame = module.fetch_local_lakehouse(
                    FetchContext(
                        etf_code="511010.SH",
                        start=date(2026, 6, 1),
                        end=date(2026, 6, 7),
                        timeout=1,
                    )
                )
            finally:
                module.LAKEHOUSE_ROOT = original_root

        self.assertEqual(frame["source"].tolist(), ["local"])
        self.assertEqual(frame["volume_unit"].tolist(), ["hand"])
        self.assertEqual(frame["amount_unit"].tolist(), ["thousand_CNY"])

    def test_finish_source_frame_filters_date_window(self) -> None:
        context = FetchContext(
            etf_code="511010.SH",
            start=date(2026, 6, 2),
            end=date(2026, 6, 5),
            timeout=1,
        )
        frame = pd.DataFrame(
            {
                "source": ["fake", "fake"],
                "etf_code": ["511010.SH", "511010.SH"],
                "trade_date": ["2026-06-01", "2026-06-05"],
                "open": [1.0, 1.1],
                "close": [1.0, 1.1],
                "high": [1.0, 1.1],
                "low": [1.0, 1.1],
                "volume": [1.0, 2.0],
                "volume_unit": ["hand", "hand"],
                "amount": [None, None],
                "amount_unit": [None, None],
                "pct_chg": [None, None],
                "source_url": ["u", "u"],
                "source_note": ["n", "n"],
            }
        )

        result = module._finish_source_frame(frame, "fake", context)

        self.assertEqual(result["trade_date"].tolist(), ["2026-06-05"])

    def test_comparison_files_capture_cross_source_differences(self) -> None:
        def source_a(context: FetchContext) -> pd.DataFrame:
            return self._source_frame(context, "a", 1.1, 100.0)

        def source_b(context: FetchContext) -> pd.DataFrame:
            return self._source_frame(context, "b", 1.2, 120.0)

        with TemporaryDirectory() as temp_dir:
            original = module._SOURCE_FETCHERS.copy()
            module._SOURCE_FETCHERS["a"] = source_a
            module._SOURCE_FETCHERS["b"] = source_b
            try:
                result = CliRunner().invoke(
                    main,
                    [
                        "511010",
                        "2026-06-01",
                        "2026-06-07",
                        "--sources",
                        "a,b",
                        "--out-dir",
                        temp_dir,
                    ],
                )
            finally:
                module._SOURCE_FETCHERS.clear()
                module._SOURCE_FETCHERS.update(original)

            self.assertEqual(result.exit_code, 0, result.output)
            export_dir = Path(temp_dir) / "511010_SH_2026-06-01_2026-06-07"
            comparison = pd.read_csv(export_dir / "comparison.csv")
            summary = pd.read_csv(export_dir / "summary.csv")

        self.assertEqual(comparison["close_a"].tolist(), [1.1])
        self.assertEqual(comparison["close_b"].tolist(), [1.2])
        close_summary = summary[summary["field"] == "close"].iloc[0]
        self.assertAlmostEqual(close_summary["max_abs_diff"], 0.1)
        self.assertEqual(close_summary["mismatch_days"], 1)

    def test_comparison_normalizes_share_volume_to_hands(self) -> None:
        combined = pd.DataFrame(
            {
                "source": ["sina", "tencent"],
                "etf_code": ["511010.SH", "511010.SH"],
                "trade_date": ["2026-06-05", "2026-06-05"],
                "open": [1.0, 1.0],
                "close": [1.0, 1.0],
                "high": [1.0, 1.0],
                "low": [1.0, 1.0],
                "volume": [2_955_300, 29_553],
                "volume_unit": ["share", "hand"],
                "amount": [None, None],
                "amount_unit": [None, None],
                "pct_chg": [None, None],
                "source_url": ["u", "u"],
                "source_note": ["n", "n"],
            }
        )

        comparison, summary = module.compare_source_frames(combined)

        self.assertEqual(comparison["volume_hand_sina"].tolist(), [29_553.0])
        self.assertEqual(comparison["volume_hand_tencent"].tolist(), [29_553.0])
        volume_summary = summary[summary["field"] == "volume_hand"].iloc[0]
        self.assertEqual(volume_summary["max_abs_diff"], 0)
        self.assertEqual(volume_summary["mismatch_days"], 0)

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
