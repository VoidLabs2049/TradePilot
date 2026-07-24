"""Tests for commodity futures Stage 4 basket rule freeze."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tradepilot.etl.futures_stage4 import (
    BasketRule,
    _cap_and_normalize,
    _risk_contributions,
    build_basket_frame,
    build_stage4_report,
    main,
    render_stage4_report,
    write_basket_frame,
)


class FuturesStage4Tests(unittest.TestCase):
    """Verify Stage 4 commodity basket construction and preconditions."""

    def test_builds_equal_weight_and_equal_risk_baskets(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_root = root / "docs-root"
            _write_stage4_fixture(lakehouse_root=lakehouse_root, docs_root=docs_root)

            with _working_directory(docs_root):
                frame = build_basket_frame(
                    lakehouse_root=lakehouse_root,
                    root_codes=["M.DCE", "RB.SHF"],
                    volatility_window=4,
                    min_vol_observations=2,
                    weight_cap=0.70,
                )

        self.assertEqual(
            sorted(frame["basket_rule"].unique()),
            [BasketRule.EQUAL_RISK.value, BasketRule.EQUAL_WEIGHT.value],
        )
        self.assertEqual(set(frame["root_code"].unique()), {"M.DCE", "RB.SHF"})
        self.assertFalse(frame["basket_return"].isna().any())
        self.assertIn("weight_source", frame.columns)
        self.assertIn(
            "initial_equal_weight_until_vol_ready",
            set(frame["weight_source"]),
        )
        latest = frame[
            frame["basket_rule"].eq(BasketRule.EQUAL_RISK.value)
            & frame["trade_date"].eq(frame["trade_date"].max())
        ]
        self.assertAlmostEqual(float(latest["target_weight"].sum()), 1.0)
        self.assertLessEqual(float(latest["target_weight"].max()), 0.70)

    def test_report_includes_rule_freeze_and_au_control(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_root = root / "docs-root"
            _write_stage4_fixture(lakehouse_root=lakehouse_root, docs_root=docs_root)

            with _working_directory(docs_root):
                frame = build_basket_frame(
                    lakehouse_root=lakehouse_root,
                    root_codes=["M.DCE", "RB.SHF"],
                    volatility_window=4,
                    min_vol_observations=2,
                    weight_cap=0.70,
                )
                write_result = write_basket_frame(
                    frame=frame, lakehouse_root=lakehouse_root
                )
                report = build_stage4_report(
                    frame=frame,
                    lakehouse_root=lakehouse_root,
                    root_codes=["M.DCE", "RB.SHF"],
                    control_root_code="AU.SHF",
                    output_path=write_result.path,
                    volatility_window=4,
                    min_vol_observations=2,
                    weight_cap=0.70,
                )
                text = render_stage4_report(report)
                wrote_basket = write_result.path.exists()

        self.assertTrue(wrote_basket)
        self.assertIn("stage4_rule_frozen", text)
        self.assertIn("include AU.SHF", text)
        self.assertIn("Equal-risk initial equal-weight days", text)
        self.assertIn("不运行 ETF 基线增量回测", text)

    def test_rejects_missing_stage3_quality_card(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_root = root / "docs-root"
            _write_stage4_fixture(
                lakehouse_root=lakehouse_root,
                docs_root=docs_root,
                skip_quality_card="RB.SHF",
            )

            with _working_directory(docs_root):
                with self.assertRaisesRegex(ValueError, "missing Stage 3.*JSON"):
                    build_basket_frame(
                        lakehouse_root=lakehouse_root,
                        root_codes=["M.DCE", "RB.SHF"],
                        volatility_window=4,
                        min_vol_observations=2,
                        weight_cap=0.70,
                    )

    def test_rejects_stage3_reject_decision(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_root = root / "docs-root"
            _write_stage4_fixture(
                lakehouse_root=lakehouse_root,
                docs_root=docs_root,
                rejected_root="RB.SHF",
            )

            with _working_directory(docs_root):
                with self.assertRaisesRegex(ValueError, "Stage 3 rejected"):
                    build_basket_frame(
                        lakehouse_root=lakehouse_root,
                        root_codes=["M.DCE", "RB.SHF"],
                        volatility_window=4,
                        min_vol_observations=2,
                        weight_cap=0.70,
                    )

    def test_cli_writes_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_root = root / "docs-root"
            output = root / "stage4.md"
            _write_stage4_fixture(lakehouse_root=lakehouse_root, docs_root=docs_root)

            with _working_directory(docs_root):
                result = CliRunner().invoke(
                    main,
                    [
                        "--lakehouse-root",
                        str(lakehouse_root),
                        "--root-codes",
                        "M.DCE,RB.SHF",
                        "--control-root-code",
                        "AU.SHF",
                        "--volatility-window",
                        "4",
                        "--min-vol-observations",
                        "2",
                        "--weight-cap",
                        "0.70",
                        "--output",
                        str(output),
                    ],
                )
                output_text = output.read_text(encoding="utf-8")

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("snapshot_id=", result.output)
        self.assertIn("商品篮子规则冻结报告", output_text)

    def test_report_rejects_empty_equal_risk_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            docs_root = root / "docs-root"
            _write_stage4_fixture(lakehouse_root=lakehouse_root, docs_root=docs_root)
            frame = pd.DataFrame(
                {
                    "trade_date": [date(2025, 1, 1)],
                    "basket_rule": [BasketRule.EQUAL_WEIGHT.value],
                    "basket_return": [0.0],
                }
            )

            with _working_directory(docs_root):
                with self.assertRaisesRegex(ValueError, "no equal_risk rows"):
                    build_stage4_report(
                        frame=frame,
                        lakehouse_root=lakehouse_root,
                        root_codes=["M.DCE", "RB.SHF"],
                        control_root_code="AU.SHF",
                        output_path=root / "basket.parquet",
                        volatility_window=4,
                        min_vol_observations=2,
                        weight_cap=0.70,
                    )

    def test_cap_and_normalize_rejects_zero_total(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive total"):
            _cap_and_normalize(pd.Series([0.0, 0.0]), cap=0.70)

    def test_risk_contribution_sorts_returns_before_tail(self) -> None:
        dates = [date(2025, 1, 3), date(2025, 1, 1), date(2025, 1, 2)]
        returns = pd.DataFrame(
            {
                "M.DCE": [0.03, 0.01, 0.02],
                "RB.SHF": [0.01, 0.02, 0.03],
            },
            index=dates,
        )

        rows = _risk_contributions(
            returns=returns,
            weights={"M.DCE": 0.5, "RB.SHF": 0.5},
            volatility_window=2,
        )

        self.assertEqual({row["root_code"] for row in rows}, {"M.DCE", "RB.SHF"})


class _working_directory:
    """Temporarily change the process working directory."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.previous = Path.cwd()

    def __enter__(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)
        import os

        os.chdir(self.path)

    def __exit__(self, *_args: object) -> None:
        import os

        os.chdir(self.previous)


def _write_stage4_fixture(
    *,
    lakehouse_root: Path,
    docs_root: Path,
    skip_quality_card: str | None = None,
    rejected_root: str | None = None,
) -> None:
    """Write minimal continuous contracts and Stage 3 cards."""

    dates = [date(2025, 1, 1) + timedelta(days=offset) for offset in range(12)]
    _write_continuous_contract(
        lakehouse_root=lakehouse_root,
        root_code="M.DCE",
        dates=dates,
        returns=[
            pd.NA,
            0.01,
            -0.002,
            0.004,
            0.003,
            -0.001,
            0.002,
            0.004,
            -0.003,
            0.002,
            0.001,
            0.003,
        ],
    )
    _write_continuous_contract(
        lakehouse_root=lakehouse_root,
        root_code="RB.SHF",
        dates=dates,
        returns=[
            pd.NA,
            -0.004,
            0.006,
            -0.003,
            0.002,
            0.007,
            -0.005,
            0.003,
            0.004,
            -0.002,
            0.006,
            -0.001,
        ],
    )
    _write_continuous_contract(
        lakehouse_root=lakehouse_root,
        root_code="AU.SHF",
        dates=dates,
        returns=[
            pd.NA,
            0.002,
            0.001,
            -0.001,
            0.003,
            0.002,
            -0.002,
            0.001,
            0.002,
            0.001,
            -0.001,
            0.002,
        ],
    )
    for root_code in ["M.DCE", "RB.SHF"]:
        if root_code == skip_quality_card:
            continue
        decision = "reject" if root_code == rejected_root else "accept"
        _write_quality_card(docs_root=docs_root, root_code=root_code, decision=decision)


def _write_continuous_contract(
    *, lakehouse_root: Path, root_code: str, dates: list[date], returns: list[object]
) -> None:
    """Write one Stage 2 continuous-contract fixture."""

    adjusted_close = [1000.0]
    for item in returns[1:]:
        adjusted_close.append(adjusted_close[-1] * (1 + float(item)))
    frame = pd.DataFrame(
        {
            "trade_date": dates,
            "root_symbol": [root_code] * len(dates),
            "active_contract": [root_code.replace(".", "2505.")] * len(dates),
            "adjusted_close": adjusted_close,
            "continuous_return": returns,
        }
    )
    _write_parquet(
        lakehouse_root,
        f"derived/derived.futures_continuous_contract/{root_code}/part-00000.parquet",
        frame,
    )


def _write_quality_card(*, docs_root: Path, root_code: str, decision: str) -> None:
    """Write one minimal Stage 3 quality-card JSON fixture."""

    base = docs_root / "docs/futures-v2-design/reports/stage-3/quality-cards"
    if root_code == "M.DCE":
        path = base / "commodity-futures-stage-3-m-quality-card.json"
    else:
        path = (
            base
            / f"commodity-futures-stage-3-{root_code.lower().replace('.', '-')}-quality-card.json"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'{{"decision": "{decision}"}}\n', encoding="utf-8")


def _write_parquet(
    lakehouse_root: Path, relative_path: str, frame: pd.DataFrame
) -> None:
    """Write one parquet fixture."""

    path = lakehouse_root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


if __name__ == "__main__":
    unittest.main()
