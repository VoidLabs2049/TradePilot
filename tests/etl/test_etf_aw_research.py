"""ETF all-weather research helper tests."""

from __future__ import annotations

import unittest

import pandas as pd

from tradepilot.etf_aw.research import _grid_weight_candidates, _recent_return_frontier
from tradepilot.etl.etf_aw_universe import ETF_AW_SLEEVE_CODES


class EtfAwResearchTests(unittest.TestCase):
    """Verify research candidates follow the current sleeve universe."""

    def test_grid_candidate_has_complete_six_sleeve_weights(self) -> None:
        candidates = _grid_weight_candidates(
            returns=pd.DataFrame(columns=ETF_AW_SLEEVE_CODES),
            equal_weight_returns=pd.Series(dtype=float),
            segments=[],
        )

        self.assertEqual(len(candidates), 1)
        weights = candidates[0]["weights"]
        self.assertEqual(set(weights), set(ETF_AW_SLEEVE_CODES))
        self.assertAlmostEqual(sum(weights.values()), 1.0, places=10)
        self.assertLessEqual(weights["159001.SZ"], 0.35)

    def test_recent_return_frontier_respects_drawdown_limits(self) -> None:
        dates = pd.bdate_range("2025-01-02", periods=260).date
        returns = pd.DataFrame(
            {
                code: [0.0002 + index * 0.00005] * len(dates)
                for index, code in enumerate(ETF_AW_SLEEVE_CODES)
            },
            index=dates,
        )
        equal_weight = returns.mean(axis=1)
        segments = [
            {
                "segment_name": "近6个月",
                "segment_type": "recent_6m",
                "start_date": dates[-126],
                "end_date": dates[-1],
            }
        ]
        current = {code: 1.0 / len(ETF_AW_SLEEVE_CODES) for code in ETF_AW_SLEEVE_CODES}

        frontier = _recent_return_frontier(
            returns=returns,
            equal_weight_returns=equal_weight,
            segments=segments,
            current_weights=current,
        )

        self.assertEqual(len(frontier["solutions"]), 3)
        for item in frontier["solutions"]:
            solution = item["solution"]
            self.assertIsNotNone(solution)
            self.assertGreaterEqual(
                solution["recent_6m"]["max_drawdown"],
                -item["max_drawdown_limit"],
            )
            self.assertAlmostEqual(sum(solution["weights"].values()), 1.0)


if __name__ == "__main__":
    unittest.main()
