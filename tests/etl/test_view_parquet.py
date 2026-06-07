"""Tests for the Parquet inspection CLI."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tradepilot.etl.view_parquet import main


class ViewParquetCliTests(unittest.TestCase):
    """Verify the Parquet viewer prints and exports selected rows."""

    def test_view_parquet_file_with_selected_columns(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.parquet"
            pd.DataFrame(
                {
                    "sleeve_code": ["510300.SH", "511010.SH"],
                    "close": [4.87, 141.27],
                    "ignored": ["x", "y"],
                }
            ).to_parquet(path, index=False)

            result = CliRunner().invoke(
                main,
                [str(path), "--columns", "sleeve_code,close", "--limit", "1"],
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("rows=2 columns=3 displayed=1", result.output)
        self.assertIn("510300.SH", result.output)
        self.assertNotIn("ignored", result.output)

    def test_view_parquet_directory_and_write_csv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "dataset"
            (root / "2026" / "05").mkdir(parents=True)
            pd.DataFrame({"trade_date": ["2026-05-20"], "value": [1.0]}).to_parquet(
                root / "2026" / "05" / "part-00000.parquet",
                index=False,
            )
            csv_path = Path(temp_dir) / "view.csv"

            result = CliRunner().invoke(
                main,
                [str(root), "--schema", "--csv", str(csv_path)],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn("rows=1 columns=2 displayed=1", result.output)
            self.assertIn("Schema:", result.output)
            self.assertTrue(csv_path.exists())
            exported = pd.read_csv(csv_path)
            self.assertEqual(exported["trade_date"].tolist(), ["2026-05-20"])


if __name__ == "__main__":
    unittest.main()
