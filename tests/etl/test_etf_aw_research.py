"""ETF all-weather research helper tests."""

from __future__ import annotations

import unittest

import pandas as pd

from tradepilot.etf_aw.research import _grid_weight_candidates
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


if __name__ == "__main__":
    unittest.main()
