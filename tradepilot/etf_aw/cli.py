"""CLI entrypoints for ETF all-weather frozen artifact workflows."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import json

import click
import duckdb
import pandas as pd

from tradepilot import db as tradepilot_db
from tradepilot.config import DB_PATH, LAKEHOUSE_ROOT
from tradepilot.etl import update_etf_aw_data
from tradepilot.etl.models import RunStatus, StorageZone
from tradepilot.etl.service import (
    ETLService,
    _validate_baseline_weight_frame,
    _validate_risk_budget_frame,
    _validate_target_weight_frame,
)

_RISK_BUDGET_PROFILE = "derived.etf_aw_risk_budget.build"
_TARGET_WEIGHT_PROFILE = "derived.etf_aw_target_weight.build"
_BASELINE_WEIGHT_PROFILE = "derived.etf_aw_baseline_weight.build"
_BACKTEST_KERNEL_PROFILE = "derived.etf_aw_backtest_kernel.build"
_MONTHLY_EXPLAINABILITY_PROFILE = "derived.etf_aw_monthly_explainability.build"


@click.group()
def main() -> None:
    """Run ETF all-weather artifact build, health, and report commands."""


@main.command("sync-data")
@click.option("--start-date", type=str, default=None, help="Optional start date.")
@click.option("--end-date", type=str, default=None, help="Optional end date.")
@click.option("--repair-days", type=int, default=45, show_default=True)
@click.option("--dry-run", is_flag=True)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def sync_data(
    start_date: str | None,
    end_date: str | None,
    repair_days: int,
    dry_run: bool,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Run the existing ETF all-weather data update pipeline."""

    args = [
        "--repair-days",
        str(repair_days),
        "--db-path",
        str(db_path),
        "--lakehouse-root",
        str(lakehouse_root),
    ]
    if start_date is not None:
        args.extend(["--start", start_date])
    if end_date is not None:
        args.extend(["--end", end_date])
    if dry_run:
        args.append("--dry-run")
    update_etf_aw_data.main.main(args=args, standalone_mode=True)


@main.command("build-risk-budget")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def build_risk_budget(
    start_date: str, end_date: str, db_path: Path, lakehouse_root: Path
) -> None:
    """Build the frozen ETF all-weather risk budget artifact."""

    _run_bootstrap_command(
        profile_name=_RISK_BUDGET_PROFILE,
        start=_parse_date(start_date, "start-date"),
        end=_parse_date(end_date, "end-date"),
        db_path=db_path,
        lakehouse_root=lakehouse_root,
    )


@main.command("build-target-weight")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def build_target_weight(
    start_date: str, end_date: str, db_path: Path, lakehouse_root: Path
) -> None:
    """Build the frozen ETF all-weather target weight artifact."""

    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    with _service(db_path, lakehouse_root) as service:
        budget = service._read_partitioned_dataset(
            "derived.etf_aw_risk_budget", start, end, StorageZone.DERIVED
        )
        findings = _health_findings(
            _validate_risk_budget_frame(budget),
            _status_warnings(budget, "budget_status"),
        )
        if _has_fail(findings):
            _print_findings("risk-budget", findings)
            raise click.ClickException("risk budget health check failed")
        result = service.run_bootstrap(
            _TARGET_WEIGHT_PROFILE,
            start=start,
            end=end,
        )
    _print_bootstrap_result(result)


@main.command("build-baseline-weight")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--baseline",
    type=click.Choice(["static-inverse-vol"]),
    default="static-inverse-vol",
    show_default=True,
)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def build_baseline_weight(
    start_date: str,
    end_date: str,
    baseline: str,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Build the frozen ETF all-weather static baseline weight artifact."""

    if baseline != "static-inverse-vol":
        raise click.ClickException("only static-inverse-vol baseline is supported")
    _run_bootstrap_command(
        profile_name=_BASELINE_WEIGHT_PROFILE,
        start=_parse_date(start_date, "start-date"),
        end=_parse_date(end_date, "end-date"),
        db_path=db_path,
        lakehouse_root=lakehouse_root,
    )


@main.group("health-check")
def health_check() -> None:
    """Run artifact health checks."""


@health_check.command("risk-budget")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def health_check_risk_budget(
    start_date: str, end_date: str, db_path: Path, lakehouse_root: Path
) -> None:
    """Check the risk budget artifact contract."""

    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    with _lakehouse_service(lakehouse_root) as service:
        frame = service._read_partitioned_dataset(
            "derived.etf_aw_risk_budget", start, end, StorageZone.DERIVED
        )
    findings = _health_findings(
        _validate_risk_budget_frame(frame),
        _status_warnings(frame, "budget_status"),
    )
    _print_findings("risk-budget", findings)
    if _has_fail(findings):
        raise click.ClickException("risk budget health check failed")


@health_check.command("target-weight")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def health_check_target_weight(
    start_date: str, end_date: str, db_path: Path, lakehouse_root: Path
) -> None:
    """Check the target weight artifact contract."""

    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    with _lakehouse_service(lakehouse_root) as service:
        frame = service._read_partitioned_dataset(
            "derived.etf_aw_target_weight", start, end, StorageZone.DERIVED
        )
    findings = _health_findings(
        _validate_target_weight_frame(frame),
        _status_warnings(frame, "target_weight_status"),
    )
    _print_findings("target-weight", findings)
    if _has_fail(findings):
        raise click.ClickException("target weight health check failed")


@health_check.command("baseline-weight")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def health_check_baseline_weight(
    start_date: str, end_date: str, db_path: Path, lakehouse_root: Path
) -> None:
    """Check the baseline weight artifact contract."""

    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    with _lakehouse_service(lakehouse_root) as service:
        frame = service._read_partitioned_dataset(
            "derived.etf_aw_baseline_weight", start, end, StorageZone.DERIVED
        )
    findings = _health_findings(_validate_baseline_weight_frame(frame), [])
    _print_findings("baseline-weight", findings)
    if _has_fail(findings):
        raise click.ClickException("baseline weight health check failed")


@main.command("backtest-kernel")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--strategy",
    "strategy_source",
    type=click.Choice(["target-weight", "baseline"]),
    default="target-weight",
    show_default=True,
)
@click.option(
    "--baseline",
    type=click.Choice(["static-inverse-vol"]),
    default="static-inverse-vol",
    show_default=True,
)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def backtest_kernel(
    start_date: str,
    end_date: str,
    strategy_source: str,
    baseline: str,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Build the backtest kernel from frozen target or baseline weights."""

    if baseline != "static-inverse-vol":
        raise click.ClickException("only static-inverse-vol baseline is supported")
    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    if strategy_source == "target-weight":
        _run_bootstrap_command(
            profile_name=_BACKTEST_KERNEL_PROFILE,
            start=start,
            end=end,
            db_path=db_path,
            lakehouse_root=lakehouse_root,
        )
        return
    with _service(db_path, lakehouse_root) as service:
        result = service._build_etf_aw_backtest_kernel(
            start,
            end,
            weight_source_type="baseline",
            baseline_name="static_inverse_vol",
            baseline_version="static_inverse_vol_v1",
        )
    _print_bootstrap_result(result)


@main.command("build-monthly-explainability")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def build_monthly_explainability(
    start_date: str, end_date: str, db_path: Path, lakehouse_root: Path
) -> None:
    """Build the monthly explainability table from frozen artifacts."""

    _run_bootstrap_command(
        profile_name=_MONTHLY_EXPLAINABILITY_PROFILE,
        start=_parse_date(start_date, "start-date"),
        end=_parse_date(end_date, "end-date"),
        db_path=db_path,
        lakehouse_root=lakehouse_root,
    )


@main.command("backtest-report")
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    show_default=True,
)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def backtest_report(
    start_date: str,
    end_date: str,
    output_format: str,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Print a Phase 0 report from the backtest kernel artifact."""

    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    with _lakehouse_service(lakehouse_root) as service:
        frame = service._read_partitioned_dataset(
            "derived.etf_aw_backtest_kernel", start, end, StorageZone.DERIVED
        )
    if frame.empty:
        raise click.ClickException("backtest kernel artifact is missing")
    report = _backtest_report(frame)
    if output_format == "json":
        click.echo(json.dumps(report, sort_keys=True, ensure_ascii=False))
    else:
        _print_markdown_report(report)


class _service:
    """Context manager for ETLService with a local DuckDB connection."""

    def __init__(self, db_path: Path, lakehouse_root: Path) -> None:
        self.db_path = db_path
        self.lakehouse_root = lakehouse_root
        self.conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> ETLService:
        self.conn = duckdb.connect(str(self.db_path))
        tradepilot_db.initialize_schema(self.conn)
        return ETLService(conn=self.conn, lakehouse_root=self.lakehouse_root)

    def __exit__(self, *args: object) -> None:
        if self.conn is not None:
            self.conn.close()


class _lakehouse_service:
    """Context manager for read-only lakehouse checks that do not need project DB."""

    def __init__(self, lakehouse_root: Path) -> None:
        self.lakehouse_root = lakehouse_root
        self.conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> ETLService:
        self.conn = duckdb.connect(":memory:")
        tradepilot_db.initialize_schema(self.conn)
        return ETLService(conn=self.conn, lakehouse_root=self.lakehouse_root)

    def __exit__(self, *args: object) -> None:
        if self.conn is not None:
            self.conn.close()


def _run_bootstrap_command(
    *,
    profile_name: str,
    start: date,
    end: date,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    with _service(db_path, lakehouse_root) as service:
        result = service.run_bootstrap(profile_name, start=start, end=end)
    _print_bootstrap_result(result)


def _print_bootstrap_result(result: dict) -> None:
    click.echo(
        f"profile={result.get('profile_name')} status={result.get('status')} "
        f"records_written={result.get('records_written', 0)}"
    )
    if result.get("validation"):
        for name, passed in result["validation"].items():
            status = "PASS" if passed else "FAIL"
            click.echo(f"{status} {name}")
    if result.get("status") != RunStatus.SUCCESS.value:
        raise click.ClickException(str(result.get("error_message") or "build failed"))


def _health_findings(
    validation: dict[str, bool], warnings: list[tuple[str, str]]
) -> list[tuple[str, str, str]]:
    findings = [
        ("FAIL", name, "validation check failed")
        for name, passed in validation.items()
        if not passed
    ]
    findings.extend(("WARN", name, detail) for name, detail in warnings)
    if not findings:
        findings.append(("PASS", "artifact_contract", "all checks passed"))
    return findings


def _status_warnings(frame: pd.DataFrame, status_column: str) -> list[tuple[str, str]]:
    if frame.empty or status_column not in frame.columns:
        return []
    counts = frame[status_column].astype(str).value_counts().to_dict()
    warnings = []
    for status, count in sorted(counts.items()):
        if status != "complete":
            warnings.append((f"{status_column}.{status}", f"rows={count}"))
    return warnings


def _print_findings(name: str, findings: list[tuple[str, str, str]]) -> None:
    click.echo(f"artifact={name}")
    for severity, check_name, detail in findings:
        click.echo(f"{severity} {check_name} - {detail}")


def _has_fail(findings: list[tuple[str, str, str]]) -> bool:
    return any(severity == "FAIL" for severity, _, _ in findings)


def _backtest_report(frame: pd.DataFrame) -> dict:
    grouped = []
    group_columns = ["strategy_name", "strategy_version"]
    if "weight_source_type" in frame.columns:
        group_columns.append("weight_source_type")
    if "source_weight_dataset" in frame.columns:
        group_columns.append("source_weight_dataset")
    for key, group in frame.groupby(group_columns, dropna=False, sort=True):
        if not isinstance(key, tuple):
            key = (key,)
        values = dict(zip(group_columns, key, strict=True))
        grouped.append(_single_backtest_report(group, values))
    grouped = sorted(
        grouped,
        key=lambda item: (
            item.get("weight_source_type") != "target_weight",
            str(item["strategy_name"]),
            str(item["strategy_version"]),
        ),
    )
    report = {"strategies": grouped, "comparison": _baseline_comparison(grouped)}
    target = next(
        (item for item in grouped if item.get("weight_source_type") == "target_weight"),
        None,
    )
    if target is not None:
        report.update(target)
    return report


def _single_backtest_report(frame: pd.DataFrame, values: dict[str, object]) -> dict:
    metrics = (
        frame[frame["observation_type"].astype(str) == "metric"]
        .set_index("metric_name")["metric_value"]
        .where(pd.notna, None)
        .to_dict()
    )
    daily_nav = frame[frame["observation_type"].astype(str) == "daily_nav"].copy()
    turnover = frame[frame["observation_type"].astype(str) == "turnover"].copy()
    diagnostics = frame[frame["observation_type"].astype(str) == "diagnostic"].copy()
    latest_nav = None
    if not daily_nav.empty:
        daily_nav = daily_nav.sort_values("observation_date")
        latest_nav = float(daily_nav.iloc[-1]["net_value"])
    turnover_values = turnover["metric_value"].dropna().astype(float)
    nav_range = _observation_range(daily_nav)
    turnover_range = _observation_range(turnover)
    return {
        "strategy_name": str(values.get("strategy_name")),
        "strategy_version": str(values.get("strategy_version")),
        "weight_source_type": str(values.get("weight_source_type", "target_weight")),
        "source_weight_dataset": str(
            values.get("source_weight_dataset", "derived.etf_aw_target_weight")
        ),
        "daily_nav_rows": int(len(daily_nav)),
        "latest_net_value": latest_nav,
        "metrics": metrics,
        "turnover_rows": int(len(turnover)),
        "average_turnover": (
            float(turnover_values.mean()) if not turnover_values.empty else None
        ),
        "daily_nav_start": nav_range[0],
        "daily_nav_end": nav_range[1],
        "turnover_start": turnover_range[0],
        "turnover_end": turnover_range[1],
        "diagnostics": [
            json.loads(value)
            for value in diagnostics["quality_notes_json"].tolist()
            if isinstance(value, str)
        ],
    }


def _baseline_comparison(strategies: list[dict]) -> dict | None:
    target = next(
        (
            item
            for item in strategies
            if item.get("weight_source_type") == "target_weight"
        ),
        None,
    )
    baseline = next(
        (item for item in strategies if item.get("weight_source_type") == "baseline"),
        None,
    )
    if target is None or baseline is None:
        return None
    common_turnover_start, common_turnover_end = _common_range(
        target.get("turnover_start"),
        target.get("turnover_end"),
        baseline.get("turnover_start"),
        baseline.get("turnover_end"),
    )
    return {
        "target_strategy_name": target["strategy_name"],
        "target_strategy_version": target["strategy_version"],
        "baseline_strategy_name": baseline["strategy_name"],
        "baseline_strategy_version": baseline["strategy_version"],
        "common_turnover_start": common_turnover_start,
        "common_turnover_end": common_turnover_end,
        "total_return_diff": _metric_diff(target, baseline, "total_return"),
        "max_drawdown_diff": _metric_diff(target, baseline, "max_drawdown"),
        "annualized_volatility_diff": _metric_diff(
            target, baseline, "annualized_volatility"
        ),
        "average_turnover_diff": _value_diff(
            target.get("average_turnover"), baseline.get("average_turnover")
        ),
        "target_diagnostics": target.get("diagnostics", []),
        "baseline_diagnostics": baseline.get("diagnostics", []),
    }


def _observation_range(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    if frame.empty:
        return None, None
    values = pd.to_datetime(frame["observation_date"], errors="coerce").dropna()
    if values.empty:
        return None, None
    return values.min().date().isoformat(), values.max().date().isoformat()


def _common_range(
    left_start: object,
    left_end: object,
    right_start: object,
    right_end: object,
) -> tuple[str | None, str | None]:
    values = [left_start, left_end, right_start, right_end]
    if any(value is None for value in values):
        return None, None
    start = max(str(left_start), str(right_start))
    end = min(str(left_end), str(right_end))
    if start > end:
        return None, None
    return start, end


def _metric_diff(target: dict, baseline: dict, metric_name: str) -> float | None:
    return _value_diff(
        target.get("metrics", {}).get(metric_name),
        baseline.get("metrics", {}).get(metric_name),
    )


def _value_diff(target: object, baseline: object) -> float | None:
    if target is None or baseline is None:
        return None
    return float(target) - float(baseline)


def _print_markdown_report(report: dict) -> None:
    click.echo(f"# ETF All-Weather Backtest Report")
    click.echo("")
    for strategy in report["strategies"]:
        click.echo("")
        click.echo(f"## {strategy['strategy_name']} / {strategy['strategy_version']}")
        click.echo(f"- weight_source_type: {strategy['weight_source_type']}")
        click.echo(f"- source_weight_dataset: {strategy['source_weight_dataset']}")
        click.echo(f"- daily_nav_rows: {strategy['daily_nav_rows']}")
        click.echo(f"- latest_net_value: {strategy['latest_net_value']}")
        click.echo(f"- turnover_rows: {strategy['turnover_rows']}")
        click.echo(f"- average_turnover: {strategy['average_turnover']}")
        click.echo("")
        click.echo("### Metrics")
        for name, value in sorted(strategy["metrics"].items()):
            click.echo(f"- {name}: {value}")
        if strategy["diagnostics"]:
            click.echo("")
            click.echo("### Diagnostics")
            for item in strategy["diagnostics"]:
                click.echo(f"- {json.dumps(item, sort_keys=True, ensure_ascii=False)}")
    if report.get("comparison") is not None:
        click.echo("")
        click.echo("## Comparison")
        for name, value in sorted(report["comparison"].items()):
            click.echo(f"- {name}: {value}")


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            "date must use YYYY-MM-DD format", param_hint=label
        ) from exc


if __name__ == "__main__":
    main()
