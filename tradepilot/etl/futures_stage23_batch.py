"""Batch runner for commodity futures Stage 2 and Stage 3 artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import click

from tradepilot.config import LAKEHOUSE_ROOT
from tradepilot.etl.futures_stage2 import (
    build_continuous_contract,
    build_stage2_report,
    render_stage2_report,
    write_continuous_contract,
)
from tradepilot.etl.futures_stage3 import (
    FuturesQualityCard,
    build_quality_card,
    render_quality_card,
)

NON_GOLD_FUTURES_ROOT_CODES = (
    "AL.SHF",
    "CU.SHF",
    "RB.SHF",
    "I.DCE",
    "M.DCE",
    "P.DCE",
    "SC.INE",
    "TA.ZCE",
)
_DEFAULT_DOCS_DIR = Path("docs/futures-v2-design")
_DEFAULT_SUMMARY_OUTPUT = (
    _DEFAULT_DOCS_DIR / "commodity-futures-stage-3-quality-summary.md"
)
_ROOT_START_DATE_OVERRIDES = {
    "TA.ZCE": date(2008, 9, 16),
}


@dataclass(frozen=True)
class Stage23BatchResult:
    """One root's Stage 2 and Stage 3 batch result."""

    root_code: str
    stage2_status: str
    stage3_decision: str
    stage2_report_path: Path | None
    stage3_report_path: Path | None
    continuous_path: Path | None
    row_count: int | None
    start_date: object | None
    end_date: object | None
    roll_count: int | None
    annualized_return: float | None
    annualized_volatility: float | None
    max_drawdown: float | None
    decision_reasons: list[str]


@click.command()
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
@click.option(
    "--root-codes",
    default=",".join(NON_GOLD_FUTURES_ROOT_CODES),
    show_default=True,
    help="Comma-separated non-gold futures root codes.",
)
@click.option(
    "--docs-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=_DEFAULT_DOCS_DIR,
    show_default=True,
)
@click.option(
    "--summary-output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=_DEFAULT_SUMMARY_OUTPUT,
    show_default=True,
)
def main(
    lakehouse_root: Path,
    root_codes: str,
    docs_dir: Path,
    summary_output: Path,
) -> None:
    """Generate Stage 2/3 artifacts for non-gold commodity futures roots."""

    roots = _parse_root_codes(root_codes)
    results = run_stage23_batch(
        lakehouse_root=lakehouse_root,
        root_codes=roots,
        docs_dir=docs_dir,
    )
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.write_text(
        render_stage23_summary(
            generated_at=datetime.now(tz=UTC),
            lakehouse_root=lakehouse_root,
            results=results,
        ),
        encoding="utf-8",
    )
    click.echo(f"wrote {summary_output}")
    for result in results:
        click.echo(
            f"{result.root_code}: stage2={result.stage2_status} "
            f"stage3={result.stage3_decision}"
        )


def run_stage23_batch(
    *,
    lakehouse_root: Path,
    root_codes: list[str],
    docs_dir: Path,
) -> list[Stage23BatchResult]:
    """Generate Stage 2 and Stage 3 artifacts for each requested root."""

    results: list[Stage23BatchResult] = []
    for root_code in root_codes:
        stage2_report_path = _stage2_report_path(docs_dir=docs_dir, root_code=root_code)
        stage3_report_path = _stage3_report_path(docs_dir=docs_dir, root_code=root_code)
        start_date = _ROOT_START_DATE_OVERRIDES.get(root_code)
        try:
            frame = build_continuous_contract(
                lakehouse_root=lakehouse_root,
                root_code=root_code,
                start_date=start_date,
            )
            write_result = write_continuous_contract(
                frame=frame,
                lakehouse_root=lakehouse_root,
                root_code=root_code,
            )
            stage2_report = build_stage2_report(
                frame=frame,
                lakehouse_root=lakehouse_root,
                root_code=root_code,
                output_path=write_result.path,
                start_date=start_date,
            )
            _write_text(stage2_report_path, render_stage2_report(stage2_report, frame))
            card = build_quality_card(
                lakehouse_root=lakehouse_root, root_code=root_code
            )
            caveats = _stage2_caveats(start_date=start_date)
            _write_text(stage3_report_path, render_quality_card(card, caveats=caveats))
            results.append(
                _success_result(
                    root_code=root_code,
                    stage2_report_path=stage2_report_path,
                    stage3_report_path=stage3_report_path,
                    continuous_path=write_result.path,
                    card=card,
                    extra_reasons=caveats,
                )
            )
        except ValueError as exc:
            reason = f"Stage 2 failed: {exc}"
            _write_text(
                stage3_report_path,
                _render_blocked_quality_card(root_code=root_code, reason=reason),
            )
            results.append(
                Stage23BatchResult(
                    root_code=root_code,
                    stage2_status="fail",
                    stage3_decision="reject",
                    stage2_report_path=None,
                    stage3_report_path=stage3_report_path,
                    continuous_path=None,
                    row_count=None,
                    start_date=None,
                    end_date=None,
                    roll_count=None,
                    annualized_return=None,
                    annualized_volatility=None,
                    max_drawdown=None,
                    decision_reasons=[reason],
                )
            )
    return results


def render_stage23_summary(
    *,
    generated_at: datetime,
    lakehouse_root: Path,
    results: list[Stage23BatchResult],
) -> str:
    """Render the Stage 3 all-root decision summary."""

    lines: list[str] = []
    lines.append("# TradePilot 商品期货阶段 3：非黄金候选质量汇总")
    lines.append("")
    lines.append(f"Generated at: `{generated_at.isoformat()}`")
    lines.append(f"Lakehouse root: `{lakehouse_root}`")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(
        "本汇总覆盖 Stage 4 第一轮非黄金候选：`AL.SHF / CU.SHF / RB.SHF / "
        "I.DCE / M.DCE / P.DCE / SC.INE / TA.ZCE`。它只汇总 Stage 2 连续"
        "合约构建和 Stage 3 单品种质量筛选结果，不构建商品篮子。"
    )
    lines.append("")
    lines.append("## Candidate Decisions")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Root",
                "Stage 2",
                "Stage 3",
                "Rows",
                "Window",
                "Rolls",
                "Ann return",
                "Ann volatility",
                "Max drawdown",
                "Reasons",
            ],
            rows=[_summary_row(result) for result in results],
        )
    )
    lines.append("")
    lines.append("## Stage 4 Readiness")
    lines.append("")
    if all(result.stage2_status == "pass" for result in results):
        lines.append(
            "结论：`ready_for_stage4_rule_freeze`。所有非黄金候选均已有 Stage 2 "
            "连续合约产物和 Stage 3 质量卡。"
        )
    else:
        failed = ", ".join(
            result.root_code for result in results if result.stage2_status != "pass"
        )
        lines.append(
            "结论：`not_ready_for_stage4`。以下候选未通过 Stage 2 连续合约构建："
            f"`{failed}`。Stage 4 可先做剔除这些失败候选后的规则草案，但不能把"
            "失败候选纳入正式篮子。"
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _success_result(
    *,
    root_code: str,
    stage2_report_path: Path,
    stage3_report_path: Path,
    continuous_path: Path,
    card: FuturesQualityCard,
    extra_reasons: list[str] | None = None,
) -> Stage23BatchResult:
    """Return a successful batch result from one quality card."""

    return Stage23BatchResult(
        root_code=root_code,
        stage2_status="pass",
        stage3_decision=card.decision.value,
        stage2_report_path=stage2_report_path,
        stage3_report_path=stage3_report_path,
        continuous_path=continuous_path,
        row_count=card.row_count,
        start_date=card.start_date,
        end_date=card.end_date,
        roll_count=card.roll_count,
        annualized_return=card.annualized_return,
        annualized_volatility=card.annualized_volatility,
        max_drawdown=card.max_drawdown,
        decision_reasons=[*(extra_reasons or []), *card.decision_reasons],
    )


def _render_blocked_quality_card(*, root_code: str, reason: str) -> str:
    """Render a Stage 3 blocked card when Stage 2 did not produce a series."""

    lines = [
        f"# TradePilot 商品期货阶段 3：{root_code} 单品种质量卡",
        "",
        f"Generated at: `{datetime.now(tz=UTC).isoformat()}`",
        "",
        "## Scope",
        "",
        "本报告记录 Stage 3 前置门槛失败；由于 Stage 2 连续合约未生成，"
        "不计算收益、回撤、相关性或整数手提示。",
        "",
        "## Stage 3 Decision",
        "",
        "结论：`reject`。",
        f"- {reason}",
        "",
    ]
    return "\n".join(lines)


def _summary_row(result: Stage23BatchResult) -> list[str]:
    """Return one markdown table row for a batch result."""

    window = "-"
    if result.start_date is not None and result.end_date is not None:
        window = f"{result.start_date} .. {result.end_date}"
    return [
        result.root_code,
        result.stage2_status,
        result.stage3_decision,
        "-" if result.row_count is None else str(result.row_count),
        window,
        "-" if result.roll_count is None else str(result.roll_count),
        _percent_text(result.annualized_return),
        _percent_text(result.annualized_volatility),
        _percent_text(result.max_drawdown),
        "<br>".join(result.decision_reasons),
    ]


def _stage2_report_path(*, docs_dir: Path, root_code: str) -> Path:
    """Return the Stage 2 report path for one root."""

    if root_code == "M.DCE":
        return docs_dir / "commodity-futures-stage-2-m-continuous-contract-report.md"
    return docs_dir / f"commodity-futures-stage-2-{_root_slug(root_code)}-report.md"


def _stage3_report_path(*, docs_dir: Path, root_code: str) -> Path:
    """Return the Stage 3 quality-card path for one root."""

    if root_code == "M.DCE":
        return docs_dir / "commodity-futures-stage-3-m-quality-card.md"
    return (
        docs_dir / f"commodity-futures-stage-3-{_root_slug(root_code)}-quality-card.md"
    )


def _parse_root_codes(root_codes: str) -> list[str]:
    """Parse a comma-separated root-code list."""

    roots = [root.strip().upper() for root in root_codes.split(",") if root.strip()]
    if not roots:
        raise click.BadParameter("root-codes must contain at least one root")
    return roots


def _stage2_caveats(*, start_date: date | None) -> list[str]:
    """Return explicit Stage 2 caveats for the summary table."""

    if start_date is None:
        return []
    return [
        f"Stage 2 sample starts at {start_date.isoformat()} after excluded unauditable rolls"
    ]


def _root_slug(root_code: str) -> str:
    """Return a stable lowercase file slug for one root code."""

    return root_code.lower().replace(".", "-")


def _write_text(path: Path, text: str) -> None:
    """Write a UTF-8 text artifact, creating its parent directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _percent_text(value: float | None) -> str:
    """Format optional decimal returns as percentages."""

    if value is None:
        return "-"
    return f"{value:.4%}"


def _markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Render a small markdown table."""

    if not rows:
        return ["_No rows_"]
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        output.append("| " + " | ".join(row) + " |")
    return output


if __name__ == "__main__":
    main()
