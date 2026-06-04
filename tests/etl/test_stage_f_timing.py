"""Stage F timing helper tests."""

from __future__ import annotations

from datetime import date
import unittest

import pandas as pd

from tradepilot.etl.timing import next_common_open_date


class StageFTimingTests(unittest.TestCase):
    """Verify timing-safe effective date helpers."""

    def test_next_common_open_date_skips_partial_and_closed_days(self) -> None:
        calendar = pd.DataFrame(
            {
                "exchange": ["SH", "SZ", "SH", "SZ", "SH", "SZ"],
                "trade_date": [
                    date(2026, 5, 20),
                    date(2026, 5, 20),
                    date(2026, 5, 21),
                    date(2026, 5, 21),
                    date(2026, 5, 22),
                    date(2026, 5, 22),
                ],
                "is_open": [False, False, True, False, True, True],
            }
        )

        result = next_common_open_date(calendar, date(2026, 5, 20))

        self.assertEqual(result, date(2026, 5, 22))

    def test_next_common_open_date_accepts_same_day(self) -> None:
        calendar = pd.DataFrame(
            {
                "exchange": ["SH", "SZ"],
                "trade_date": [date(2026, 4, 20), date(2026, 4, 20)],
                "is_open": [True, True],
            }
        )

        result = next_common_open_date(calendar, date(2026, 4, 20))

        self.assertEqual(result, date(2026, 4, 20))


if __name__ == "__main__":
    unittest.main()
