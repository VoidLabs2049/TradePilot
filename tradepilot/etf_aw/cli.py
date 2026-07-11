"""CLI entrypoints for ETF all-weather frozen artifact workflows."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import json
import math

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
_ROBUSTNESS_REPORT_VERSION = "etf_aw_backtest_robustness_report_v1"
_ROBUSTNESS_COST_MODEL_NAME = "half_l1_turnover_sensitivity"
_ROBUSTNESS_COST_MODEL_VERSION = "half_l1_turnover_sensitivity_v1"
_ROBUSTNESS_COST_SCENARIOS = (
    ("gross", 0),
    ("cost_5bps", 5),
    ("cost_10bps", 10),
    ("cost_20bps", 20),
)
_ROBUSTNESS_TURNOVER_BASIS = (
    "previous-target half-L1 monthly turnover; daily NAV uses target-weight "
    "daily rebalancing semantics and does not model month-end drift turnover"
)


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


@main.command("backtest-robustness-report")
@click.option("--strategy-name", required=True, type=str)
@click.option("--strategy-version", required=True, type=str)
@click.option("--baseline-name", required=True, type=str)
@click.option("--baseline-version", required=True, type=str)
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
    "--output",
    "output_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
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
def backtest_robustness_report(
    strategy_name: str,
    strategy_version: str,
    baseline_name: str,
    baseline_version: str,
    start_date: str,
    end_date: str,
    output_format: str,
    output_path: Path,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Write the Stage M robustness report from frozen backtest artifacts."""

    del db_path
    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    with _lakehouse_service(lakehouse_root) as service:
        inputs = {
            "kernel": service._read_partitioned_dataset(
                "derived.etf_aw_backtest_kernel", start, end, StorageZone.DERIVED
            ),
            "target_weight": service._read_partitioned_dataset(
                "derived.etf_aw_target_weight", start, end, StorageZone.DERIVED
            ),
            "baseline_weight": service._read_partitioned_dataset(
                "derived.etf_aw_baseline_weight", start, end, StorageZone.DERIVED
            ),
            "risk_budget": service._read_partitioned_dataset(
                "derived.etf_aw_risk_budget", start, end, StorageZone.DERIVED
            ),
            "strategy_context": service._read_partitioned_dataset(
                "derived.etf_aw_strategy_context", start, end, StorageZone.DERIVED
            ),
            "sleeve_daily": service._read_partitioned_dataset(
                "derived.etf_aw_sleeve_daily", start, end, StorageZone.DERIVED
            ),
        }
    report = _backtest_robustness_report(
        inputs=inputs,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        baseline_name=baseline_name,
        baseline_version=baseline_version,
        start=start,
        end=end,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "json":
        output_path.write_text(
            json.dumps(report, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    else:
        output_path.write_text(
            _robustness_markdown(report),
            encoding="utf-8",
        )
    if report["report_status"] == "blocked":
        raise click.ClickException("; ".join(report["coverage"]["blocking_reasons"]))


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
    if grouped:
        report.update(grouped[0])
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
        (
            item
            for item in strategies
            if item.get("strategy_name") == "static_inverse_vol"
            and item.get("strategy_version") == "static_inverse_vol_v1"
        ),
        None,
    )
    if target is None or baseline is None:
        return None
    return {
        "target_strategy_name": target["strategy_name"],
        "target_strategy_version": target["strategy_version"],
        "baseline_strategy_name": baseline["strategy_name"],
        "baseline_strategy_version": baseline["strategy_version"],
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


def _backtest_robustness_report(
    *,
    inputs: dict[str, pd.DataFrame],
    strategy_name: str,
    strategy_version: str,
    baseline_name: str,
    baseline_version: str,
    start: date,
    end: date,
) -> dict:
    kernel = inputs["kernel"]
    target_identity = _resolve_kernel_identity(
        kernel,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        weight_source_type="target_weight",
    )
    baseline_identity = _resolve_kernel_identity(
        kernel,
        strategy_name=baseline_name,
        strategy_version=baseline_version,
        weight_source_type="baseline",
    )
    blocking_reasons = []
    if target_identity["status"] != "matched":
        blocking_reasons.append(target_identity["message"])
    if baseline_identity["status"] != "matched":
        blocking_reasons.append(baseline_identity["message"])

    identities = {
        "strategy": target_identity,
        "baseline": baseline_identity,
    }
    target_frame = _filter_identity_frame(kernel, target_identity)
    baseline_frame = _filter_identity_frame(kernel, baseline_identity)
    coverage = _robustness_coverage(
        inputs=inputs,
        target_frame=target_frame,
        baseline_frame=baseline_frame,
        target_identity=target_identity,
        baseline_identity=baseline_identity,
        start=start,
        end=end,
    )
    blocking_reasons.extend(coverage["blocking_reasons"])
    cost_blocking = _cost_blocking_reasons(target_frame, baseline_frame, coverage)
    blocking_reasons.extend(cost_blocking)
    blocking_reasons = sorted(set(blocking_reasons))
    coverage["blocking_reasons"] = blocking_reasons
    report_status = "blocked" if blocking_reasons else "complete"
    strategies = [
        _robustness_strategy_report(
            label="strategy",
            frame=target_frame,
            identity=target_identity,
            coverage=coverage,
            blocked=report_status == "blocked",
        ),
        _robustness_strategy_report(
            label="baseline",
            frame=baseline_frame,
            identity=baseline_identity,
            coverage=coverage,
            blocked=report_status == "blocked",
        ),
    ]
    comparisons = (
        []
        if report_status == "blocked"
        else _robustness_comparisons(strategies[0], strategies[1])
    )
    return {
        "report_version": _ROBUSTNESS_REPORT_VERSION,
        "report_status": report_status,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cost_model_name": _ROBUSTNESS_COST_MODEL_NAME,
        "cost_model_version": _ROBUSTNESS_COST_MODEL_VERSION,
        "risk_free_rate": 0.0,
        "input_identities": identities,
        "requested_range": {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        },
        "comparable_range": {
            "start_date": coverage["comparable_start_date"],
            "end_date": coverage["comparable_end_date"],
        },
        "coverage": coverage,
        "strategies": strategies,
        "comparisons": comparisons,
        "diagnostics": {
            "warnings": coverage["warnings"],
            "blocking_reasons": blocking_reasons,
        },
    }


def _resolve_kernel_identity(
    frame: pd.DataFrame,
    *,
    strategy_name: str,
    strategy_version: str,
    weight_source_type: str,
) -> dict:
    columns = [
        "calendar_name",
        "strategy_name",
        "strategy_version",
        "weight_source_type",
        "source_weight_dataset",
    ]
    if frame.empty or not set(columns).issubset(frame.columns):
        return {
            "status": "missing",
            "message": f"{weight_source_type} kernel identity is missing",
            "matches": [],
        }
    subset = frame[
        frame["strategy_name"].astype(str).eq(strategy_name)
        & frame["strategy_version"].astype(str).eq(strategy_version)
        & frame["weight_source_type"].astype(str).eq(weight_source_type)
    ]
    identities = (
        subset[columns].drop_duplicates().sort_values(columns).to_dict(orient="records")
    )
    if len(identities) != 1:
        return {
            "status": "ambiguous" if identities else "missing",
            "message": (
                f"{weight_source_type} identity matched {len(identities)} groups"
            ),
            "matches": identities,
        }
    identity = {key: str(value) for key, value in identities[0].items()}
    identity["status"] = "matched"
    identity["message"] = ""
    identity["matches"] = identities
    identity["max_ingested_at"] = _max_iso(subset, "ingested_at")
    return identity


def _filter_identity_frame(frame: pd.DataFrame, identity: dict) -> pd.DataFrame:
    if frame.empty or identity.get("status") != "matched":
        return pd.DataFrame()
    result = frame.copy()
    for column in [
        "calendar_name",
        "strategy_name",
        "strategy_version",
        "weight_source_type",
        "source_weight_dataset",
    ]:
        result = result[result[column].astype(str).eq(str(identity[column]))]
    return result.copy()


def _robustness_coverage(
    *,
    inputs: dict[str, pd.DataFrame],
    target_frame: pd.DataFrame,
    baseline_frame: pd.DataFrame,
    target_identity: dict,
    baseline_identity: dict,
    start: date,
    end: date,
) -> dict:
    warnings = ["initial_formation_cost_unobservable", _ROBUSTNESS_TURNOVER_BASIS]
    blocking = []
    target_daily_dates = _observation_dates(target_frame, "daily_nav")
    baseline_daily_dates = _observation_dates(baseline_frame, "daily_nav")
    target_weight_dates = _complete_weight_dates(
        inputs["target_weight"],
        identity=target_identity,
        name_column="strategy_name",
        version_column="strategy_version",
    )
    baseline_weight_dates = _complete_weight_dates(
        inputs["baseline_weight"],
        identity=baseline_identity,
        name_column="baseline_name",
        version_column="baseline_version",
    )
    comparable_start, comparable_end = _comparable_range(
        [
            min(target_daily_dates) if target_daily_dates else None,
            min(baseline_daily_dates) if baseline_daily_dates else None,
            min(target_weight_dates) if target_weight_dates else None,
            min(baseline_weight_dates) if baseline_weight_dates else None,
            start,
        ],
        [
            max(target_daily_dates) if target_daily_dates else None,
            max(baseline_daily_dates) if baseline_daily_dates else None,
            end,
        ],
    )
    if comparable_start is None or comparable_end is None:
        blocking.append("strategy and baseline have no overlapping comparable range")
        comparable_target_dates: set[date] = set()
        comparable_baseline_dates: set[date] = set()
    else:
        comparable_target_dates = {
            value
            for value in target_daily_dates
            if comparable_start <= value <= comparable_end
        }
        comparable_baseline_dates = {
            value
            for value in baseline_daily_dates
            if comparable_start <= value <= comparable_end
        }
        if comparable_target_dates != comparable_baseline_dates:
            blocking.append("strategy and baseline daily trade dates differ")
        if start < comparable_start or comparable_end < end:
            warnings.append("requested range extends outside comparable range")
        target_missing_weights = _missing_weight_periods(
            comparable_target_dates, target_weight_dates
        )
        baseline_missing_weights = _missing_weight_periods(
            comparable_baseline_dates, baseline_weight_dates
        )
        if target_missing_weights:
            blocking.append("target weight is incomplete inside comparable range")
        if baseline_missing_weights:
            blocking.append("baseline weight is incomplete inside comparable range")
    diagnostics = _diagnostics(target_frame) + _diagnostics(baseline_frame)
    if diagnostics:
        blocking.append("backtest kernel contains blocking diagnostic rows")
    strategy_status_counts = _value_counts(
        inputs["target_weight"], "target_weight_status"
    )
    risk_budget_status_counts = _value_counts(inputs["risk_budget"], "budget_status")
    regime_label_counts = _value_counts(
        inputs["strategy_context"], "market_regime_label"
    )
    if any(key != "complete" for key in strategy_status_counts):
        warnings.append("target weight contains non-complete status")
    if any(key != "complete" for key in risk_budget_status_counts):
        warnings.append("risk budget contains non-complete status")
    sleeve_return_gaps = _sleeve_return_gaps(
        inputs["sleeve_daily"], comparable_start, comparable_end
    )
    if sleeve_return_gaps:
        warnings.append("sleeve return dates contain gaps")
    common_dates = comparable_target_dates & comparable_baseline_dates
    turnover_dates = _observation_dates(target_frame, "turnover") & _observation_dates(
        baseline_frame, "turnover"
    )
    if comparable_start is not None and comparable_end is not None:
        turnover_dates = {
            value
            for value in turnover_dates
            if comparable_start <= value <= comparable_end
        }
    return {
        "requested_start_date": start.isoformat(),
        "requested_end_date": end.isoformat(),
        "comparable_start_date": (
            comparable_start.isoformat() if comparable_start is not None else None
        ),
        "comparable_end_date": (
            comparable_end.isoformat() if comparable_end is not None else None
        ),
        "daily_observation_count": len(common_dates),
        "rebalance_period_count": len(turnover_dates),
        "strategy_status_counts": strategy_status_counts,
        "risk_budget_status_counts": risk_budget_status_counts,
        "regime_label_counts": regime_label_counts,
        "missing_trade_dates": {
            "strategy": sorted(
                value.isoformat()
                for value in comparable_baseline_dates - comparable_target_dates
            ),
            "baseline": sorted(
                value.isoformat()
                for value in comparable_target_dates - comparable_baseline_dates
            ),
        },
        "sleeve_return_gaps": sleeve_return_gaps,
        "blocking_reasons": blocking,
        "warnings": sorted(set(warnings)),
        "diagnostics": diagnostics,
    }


def _robustness_strategy_report(
    *,
    label: str,
    frame: pd.DataFrame,
    identity: dict,
    coverage: dict,
    blocked: bool,
) -> dict:
    scenarios = []
    for scenario_name, bps in _ROBUSTNESS_COST_SCENARIOS:
        scenarios.append(
            _robustness_scenario(
                frame,
                coverage=coverage,
                scenario_name=scenario_name,
                cost_bps=bps,
                blocked=blocked,
            )
        )
    return {
        "label": label,
        "calendar_name": identity.get("calendar_name"),
        "strategy_name": identity.get("strategy_name"),
        "strategy_version": identity.get("strategy_version"),
        "weight_source_type": identity.get("weight_source_type"),
        "source_weight_dataset": identity.get("source_weight_dataset"),
        "scenarios": scenarios,
    }


def _robustness_scenario(
    frame: pd.DataFrame,
    *,
    coverage: dict,
    scenario_name: str,
    cost_bps: int,
    blocked: bool,
) -> dict:
    gross_metrics = _kernel_metrics(frame)
    base = {
        "cost_scenario": scenario_name,
        "cost_bps_per_executed_notional": cost_bps,
        "gross_total_return": gross_metrics.get("total_return"),
        "gross_annualized_volatility": gross_metrics.get("annualized_volatility"),
        "gross_sharpe_ratio": gross_metrics.get("sharpe_ratio"),
        "gross_max_drawdown": gross_metrics.get("max_drawdown"),
        "net_total_return": None,
        "cost_drag": None,
        "net_annualized_return": None,
        "net_annualized_volatility": None,
        "net_sharpe_ratio": None,
        "net_max_drawdown": None,
        "average_turnover": None,
        "estimated_cost_fraction_sum": None,
        "initial_formation_cost_status": "unobservable",
        "turnover_basis": _ROBUSTNESS_TURNOVER_BASIS,
        "metric_basis": (
            "gross" if cost_bps == 0 else "net_excludes_initial_formation_cost"
        ),
        "diagnostics": [
            "risk_free_rate=0",
            "initial formation cost is unobservable",
            _ROBUSTNESS_TURNOVER_BASIS,
        ],
    }
    if blocked:
        return base
    rows = _daily_nav(frame, coverage)
    turnover = _turnover_by_date(frame, coverage)
    returns = [float(row["portfolio_return"]) for _, row in rows.iterrows()]
    costs = _cost_fractions(rows, turnover, cost_bps)
    if cost_bps == 0:
        net_returns = returns
        estimated_cost_sum = 0.0
    else:
        net_returns = [
            ((1.0 - (cost or 0.0)) * (1.0 + gross_return)) - 1.0
            for gross_return, cost in zip(returns, costs, strict=True)
        ]
        estimated_cost_sum = sum(cost for cost in costs if cost is not None)
    nav = _nav_from_returns(net_returns)
    metrics = _metrics_from_nav_and_returns(nav, net_returns)
    average_turnover = sum(turnover.values()) / len(turnover) if turnover else None
    base.update(
        {
            "net_total_return": metrics["total_return"],
            "cost_drag": (
                None
                if gross_metrics.get("total_return") is None
                else float(gross_metrics["total_return"]) - metrics["total_return"]
            ),
            "net_annualized_return": metrics["annualized_return"],
            "net_annualized_volatility": metrics["annualized_volatility"],
            "net_sharpe_ratio": metrics["sharpe_ratio"],
            "net_max_drawdown": metrics["max_drawdown"],
            "average_turnover": average_turnover,
            "estimated_cost_fraction_sum": estimated_cost_sum,
        }
    )
    return base


def _robustness_comparisons(strategy: dict, baseline: dict) -> list[dict]:
    results = []
    baseline_by_name = {item["cost_scenario"]: item for item in baseline["scenarios"]}
    for scenario in strategy["scenarios"]:
        other = baseline_by_name[scenario["cost_scenario"]]
        results.append(
            {
                "cost_scenario": scenario["cost_scenario"],
                "metric_basis": scenario["metric_basis"],
                "gross_total_return_diff": _value_diff(
                    scenario["gross_total_return"], other["gross_total_return"]
                ),
                "gross_annualized_volatility_diff": _value_diff(
                    scenario["gross_annualized_volatility"],
                    other["gross_annualized_volatility"],
                ),
                "gross_sharpe_ratio_diff": _value_diff(
                    scenario["gross_sharpe_ratio"], other["gross_sharpe_ratio"]
                ),
                "gross_max_drawdown_diff": _value_diff(
                    scenario["gross_max_drawdown"], other["gross_max_drawdown"]
                ),
                "net_total_return_diff": _value_diff(
                    scenario["net_total_return"], other["net_total_return"]
                ),
                "net_annualized_volatility_diff": _value_diff(
                    scenario["net_annualized_volatility"],
                    other["net_annualized_volatility"],
                ),
                "net_sharpe_ratio_diff": _value_diff(
                    scenario["net_sharpe_ratio"], other["net_sharpe_ratio"]
                ),
                "net_max_drawdown_diff": _value_diff(
                    scenario["net_max_drawdown"], other["net_max_drawdown"]
                ),
                "estimated_cost_fraction_sum_diff": _value_diff(
                    scenario["estimated_cost_fraction_sum"],
                    other["estimated_cost_fraction_sum"],
                ),
            }
        )
    return results


def _daily_nav(frame: pd.DataFrame, coverage: dict) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    start = date.fromisoformat(coverage["comparable_start_date"])
    end = date.fromisoformat(coverage["comparable_end_date"])
    rows = frame[frame["observation_type"].astype(str).eq("daily_nav")].copy()
    rows["observation_date"] = pd.to_datetime(
        rows["observation_date"], errors="coerce"
    ).dt.date
    rows = rows[
        rows["observation_date"].notna()
        & (rows["observation_date"] >= start)
        & (rows["observation_date"] <= end)
    ].copy()
    rows = rows.sort_values("observation_date")
    if "portfolio_return" not in rows.columns:
        rows["portfolio_return"] = (
            rows["net_value"]
            .astype(float)
            .pct_change()
            .fillna(rows["net_value"].astype(float) - 1.0)
        )
    return rows


def _kernel_metrics(frame: pd.DataFrame) -> dict[str, float | None]:
    if frame.empty:
        return {}
    metrics = frame[frame["observation_type"].astype(str).eq("metric")]
    return {
        str(row["metric_name"]): _finite_float(row["metric_value"])
        for _, row in metrics.iterrows()
    }


def _turnover_by_date(frame: pd.DataFrame, coverage: dict) -> dict[date, float]:
    if frame.empty or coverage["comparable_start_date"] is None:
        return {}
    start = date.fromisoformat(coverage["comparable_start_date"])
    end = date.fromisoformat(coverage["comparable_end_date"])
    rows = frame[frame["observation_type"].astype(str).eq("turnover")].copy()
    rows["observation_date"] = pd.to_datetime(
        rows["observation_date"], errors="coerce"
    ).dt.date
    rows = rows[
        rows["observation_date"].notna()
        & (rows["observation_date"] >= start)
        & (rows["observation_date"] <= end)
    ]
    return {
        row["observation_date"]: float(row["metric_value"])
        for _, row in rows.iterrows()
        if _finite_float(row["metric_value"]) is not None
    }


def _cost_fractions(
    daily_rows: pd.DataFrame, turnover: dict[date, float], cost_bps: int
) -> list[float | None]:
    if cost_bps == 0:
        return [0.0 for _ in range(len(daily_rows))]
    first_turnover_date = min(turnover) if turnover else None
    rate = cost_bps / 10000.0
    costs = []
    for _, row in daily_rows.iterrows():
        current_date = row["observation_date"]
        if current_date == first_turnover_date:
            costs.append(None)
        else:
            costs.append(2.0 * turnover.get(current_date, 0.0) * rate)
    return costs


def _metrics_from_nav_and_returns(nav: list[float], returns: list[float]) -> dict:
    if not nav:
        return {
            "total_return": None,
            "annualized_return": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
        }
    total_return = nav[-1] - 1.0
    annualized_return = nav[-1] ** (252 / len(nav)) - 1.0
    volatility = float(pd.Series(returns).std(ddof=1)) * math.sqrt(252)
    sharpe = (
        None
        if volatility == 0.0 or math.isnan(volatility)
        else annualized_return / volatility
    )
    running_max = []
    current_max = 0.0
    for value in nav:
        current_max = max(current_max, value)
        running_max.append(current_max)
    drawdowns = [
        (value / high) - 1.0 for value, high in zip(nav, running_max, strict=True)
    ]
    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": None if math.isnan(volatility) else volatility,
        "sharpe_ratio": sharpe,
        "max_drawdown": min(drawdowns),
    }


def _nav_from_returns(returns: list[float]) -> list[float]:
    nav = []
    current = 1.0
    for value in returns:
        current *= 1.0 + value
        nav.append(current)
    return nav


def _cost_blocking_reasons(
    target_frame: pd.DataFrame, baseline_frame: pd.DataFrame, coverage: dict
) -> list[str]:
    if coverage["comparable_start_date"] is None:
        return []
    reasons = []
    for label, frame in [("strategy", target_frame), ("baseline", baseline_frame)]:
        turnover = _turnover_by_date(frame, coverage)
        for value in turnover.values():
            if value < 0.0 or not math.isfinite(value):
                reasons.append(f"{label} turnover contains invalid value")
            if 2.0 * value * (20 / 10000.0) >= 1.0:
                reasons.append(f"{label} turnover cost fraction is too large")
    return reasons


def _complete_weight_dates(
    frame: pd.DataFrame,
    *,
    identity: dict,
    name_column: str,
    version_column: str,
) -> set[date]:
    if frame.empty or identity.get("status") != "matched":
        return set()
    required = {
        "calendar_name",
        "rebalance_date",
        name_column,
        version_column,
        "sleeve_code",
    }
    if not required.issubset(frame.columns):
        return set()
    rows = frame[
        frame["calendar_name"].astype(str).eq(str(identity["calendar_name"]))
        & frame[name_column].astype(str).eq(str(identity["strategy_name"]))
        & frame[version_column].astype(str).eq(str(identity["strategy_version"]))
    ].copy()
    rows["rebalance_date"] = pd.to_datetime(
        rows["rebalance_date"], errors="coerce"
    ).dt.date
    result = set()
    for rebalance_date, group in rows.groupby("rebalance_date"):
        if len(set(group["sleeve_code"].astype(str))) == 5:
            result.add(rebalance_date)
    return result


def _missing_weight_periods(
    daily_dates: set[date], weight_dates: set[date]
) -> list[str]:
    if not daily_dates:
        return []
    missing = []
    for current_date in sorted(daily_dates):
        eligible = [value for value in weight_dates if value <= current_date]
        if not eligible:
            missing.append(current_date.isoformat())
    return missing


def _observation_dates(frame: pd.DataFrame, observation_type: str) -> set[date]:
    if frame.empty or "observation_date" not in frame.columns:
        return set()
    rows = frame[frame["observation_type"].astype(str).eq(observation_type)].copy()
    return set(
        pd.to_datetime(rows["observation_date"], errors="coerce").dt.date.dropna()
    )


def _comparable_range(
    starts: list[date | None], ends: list[date | None]
) -> tuple[date | None, date | None]:
    if any(value is None for value in starts + ends):
        return None, None
    start = max(value for value in starts if value is not None)
    end = min(value for value in ends if value is not None)
    if start > end:
        return None, None
    return start, end


def _diagnostics(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []
    rows = frame[frame["observation_type"].astype(str).eq("diagnostic")]
    result = []
    for value in rows["quality_notes_json"].tolist():
        if isinstance(value, str):
            result.append(json.loads(value))
    return result


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {
        str(key): int(value)
        for key, value in frame[column].astype(str).value_counts().sort_index().items()
    }


def _sleeve_return_gaps(
    frame: pd.DataFrame, start: date | None, end: date | None
) -> list[dict]:
    if frame.empty or start is None or end is None:
        return []
    if not {"sleeve_code", "trade_date"}.issubset(frame.columns):
        return []
    rows = frame.copy()
    rows["trade_date"] = pd.to_datetime(rows["trade_date"], errors="coerce").dt.date
    rows = rows[
        rows["trade_date"].notna()
        & (rows["trade_date"] >= start)
        & (rows["trade_date"] <= end)
    ].copy()
    if rows.empty:
        return []
    all_dates = set(rows["trade_date"])
    gaps = []
    for sleeve_code, group in rows.groupby("sleeve_code"):
        sleeve_dates = set(group["trade_date"])
        missing = sorted(all_dates - sleeve_dates)
        if missing:
            gaps.append(
                {
                    "sleeve_code": str(sleeve_code),
                    "gap_start_date": missing[0].isoformat(),
                    "gap_end_date": missing[-1].isoformat(),
                    "missing_trade_date_count": len(missing),
                }
            )
    return gaps


def _max_iso(frame: pd.DataFrame, column: str) -> str | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_datetime(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return values.max().isoformat()


def _finite_float(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _robustness_markdown(report: dict) -> str:
    lines = [
        "# ETF All-Weather Backtest Robustness Report",
        "",
        f"- report_status: {report['report_status']}",
        f"- report_version: {report['report_version']}",
        f"- generated_at: {report['generated_at']}",
        f"- comparable_range: {json.dumps(report['comparable_range'], sort_keys=True)}",
        "",
    ]
    if report["report_status"] == "blocked":
        lines.extend(["## Blocking Reasons", ""])
        for reason in report["coverage"]["blocking_reasons"]:
            lines.append(f"- {reason}")
        lines.append("")
    lines.extend(["## Coverage", ""])
    for key, value in report["coverage"].items():
        lines.append(
            f"- {key}: {json.dumps(value, sort_keys=True, ensure_ascii=False)}"
        )
    lines.append("")
    lines.append("## Strategies")
    for strategy in report["strategies"]:
        lines.append("")
        lines.append(f"### {strategy['label']}")
        lines.append(f"- strategy_name: {strategy.get('strategy_name')}")
        lines.append(f"- strategy_version: {strategy.get('strategy_version')}")
        lines.append(f"- weight_source_type: {strategy.get('weight_source_type')}")
        for scenario in strategy["scenarios"]:
            lines.append("")
            lines.append(f"#### {scenario['cost_scenario']}")
            for key, value in scenario.items():
                if key != "cost_scenario":
                    lines.append(f"- {key}: {json.dumps(value, ensure_ascii=False)}")
    if report["comparisons"]:
        lines.extend(["", "## Comparisons"])
        for comparison in report["comparisons"]:
            lines.append("")
            lines.append(f"### {comparison['cost_scenario']}")
            for key, value in comparison.items():
                if key != "cost_scenario":
                    lines.append(f"- {key}: {json.dumps(value, ensure_ascii=False)}")
    return "\n".join(lines) + "\n"


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            "date must use YYYY-MM-DD format", param_hint=label
        ) from exc


if __name__ == "__main__":
    main()
