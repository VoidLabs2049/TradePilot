"""CLI entrypoints for ETF all-weather frozen artifact workflows."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path
import calendar
import json
import math

import click
import duckdb
import pandas as pd

from tradepilot import db as tradepilot_db
from tradepilot.config import DB_PATH, LAKEHOUSE_ROOT
from tradepilot.etf_aw.rebalance_plan import (
    AccountPosition,
    AccountSnapshot,
    REBALANCE_PLAN_DATASET,
    build_rebalance_plan as build_rebalance_plan_frame,
    load_account_snapshot,
    load_price_snapshot,
    plan_to_json_payload,
    plan_to_markdown,
)
from tradepilot.etf_aw.shadow_run import (
    BaselineObservationInput,
    ClosePriceItem,
    PAPER_DECISION_DATASET,
    PAPER_FILL_DATASET,
    PriceSnapshotInput,
    SHADOW_ACCOUNT_SEED_DATASET,
    SHADOW_OBSERVATION_DATASET,
    ShadowRunError,
    append_dataset,
    build_decision_row,
    build_fill_row,
    build_performance_report,
    build_post_mortem,
    build_shadow_observation_rows,
    build_shadow_seed_rows,
    load_baseline_observation_input,
    load_decision_input,
    load_fill_input,
    load_price_snapshot_input,
    performance_report_html,
    post_mortem_markdown,
    read_shadow_dataset,
)
from tradepilot.etl import update_etf_aw_data
from tradepilot.etl.etf_aw_universe import (
    ETF_AW_SLEEVE_CODE_BY_ROLE,
    ETF_AW_SLEEVE_CODES,
    ETF_AW_SLEEVE_ROLE_ORDER,
)
from tradepilot.etl.models import RunStatus, StorageZone
from tradepilot.etl.service import (
    ETLService,
    _backtest_metric_values,
    _validate_baseline_weight_frame,
    _validate_risk_budget_frame,
    _validate_target_weight_frame,
)
from tradepilot.etl.storage import write_dataset_parquet

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
_ROBUSTNESS_HISTORY_START = date(1900, 1, 1)


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


@main.command("build-rebalance-plan")
@click.option(
    "--account-snapshot",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    required=True,
)
@click.option(
    "--price-snapshot",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    required=True,
)
@click.option("--plan-date", required=True, type=str)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def build_rebalance_plan(
    account_snapshot: Path,
    price_snapshot: Path,
    plan_date: str,
    output_dir: Path,
    lakehouse_root: Path,
) -> None:
    """Build a Stage N simulated rebalance plan draft."""

    parsed_plan_date = _parse_date(plan_date, "plan-date")
    with _lakehouse_service(lakehouse_root) as service:
        target_weight = service._read_partitioned_dataset(
            "derived.etf_aw_target_weight",
            date(1900, 1, 1),
            parsed_plan_date,
            StorageZone.DERIVED,
        )
        existing = service._read_partitioned_dataset(
            REBALANCE_PLAN_DATASET,
            date(parsed_plan_date.year, parsed_plan_date.month, 1),
            date(
                parsed_plan_date.year,
                parsed_plan_date.month,
                calendar.monthrange(parsed_plan_date.year, parsed_plan_date.month)[1],
            ),
            StorageZone.DERIVED,
        )
    try:
        account = load_account_snapshot(account_snapshot)
    except Exception as exc:
        _raise_rebalance_plan_error(["invalid_account_snapshot"], {"error": str(exc)})
    try:
        prices = load_price_snapshot(price_snapshot)
    except Exception as exc:
        _raise_rebalance_plan_error(["missing_or_invalid_price"], {"error": str(exc)})

    frame, summary, diagnostics = build_rebalance_plan_frame(
        target_weight=target_weight,
        account=account,
        prices=prices,
        plan_date=parsed_plan_date,
    )
    if _has_duplicate_rebalance_plan(existing, summary["plan_id"]):
        diagnostics.blocking_reasons.append("duplicate_active_plan")
        summary["blocking_reasons"] = diagnostics.blocking_reasons
    if diagnostics.blocked:
        _raise_rebalance_plan_error(
            diagnostics.blocking_reasons,
            {
                "summary": summary,
                "warnings": diagnostics.warnings,
                "line_diagnostics": diagnostics.line_diagnostics,
            },
        )

    try:
        artifact_frame = _append_rebalance_plan_rows(existing, frame)
    except ValueError as exc:
        _raise_rebalance_plan_error(
            ["rebalance_plan_schema_mismatch"],
            {"error": str(exc)},
        )
    write_result = write_dataset_parquet(
        artifact_frame,
        REBALANCE_PLAN_DATASET,
        StorageZone.DERIVED,
        [("year", parsed_plan_date.year), ("month", f"{parsed_plan_date.month:02d}")],
        lakehouse_root=lakehouse_root,
    )
    payload = plan_to_json_payload(
        frame=frame,
        summary=summary,
        account_snapshot_path=account_snapshot,
        price_snapshot_path=price_snapshot,
        target_weight_artifact="derived.etf_aw_target_weight",
        diagnostics=diagnostics,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{summary['plan_id']}.json"
    markdown_path = output_dir / f"{summary['plan_id']}.md"
    json_path.write_text(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(plan_to_markdown(payload), encoding="utf-8")

    click.echo(f"plan_id={summary['plan_id']} status=DRAFT rows={len(frame)}")
    click.echo(
        "estimated_buy_notional={:.2f} estimated_sell_proceeds={:.2f} "
        "cash_after_plan={:.2f}".format(
            summary["estimated_buy_notional"],
            summary["estimated_sell_proceeds"],
            summary["cash_after_plan"],
        )
    )
    for side, count in frame["order_side"].value_counts().sort_index().items():
        quantity = int(frame.loc[frame["order_side"] == side, "order_quantity"].sum())
        click.echo(f"{side} rows={count} quantity={quantity}")
    click.echo(f"warnings={','.join(diagnostics.warnings)}")
    click.echo(f"artifact={write_result.relative_path}")
    click.echo(f"json={json_path}")
    click.echo(f"markdown={markdown_path}")


@main.command("initialize-shadow-account")
@click.option("--plan-id", required=True, type=str)
@click.option(
    "--account-snapshot",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    required=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def initialize_shadow_account(
    plan_id: str,
    account_snapshot: Path,
    lakehouse_root: Path,
) -> None:
    """Freeze the Stage O shadow account seed for one account."""

    try:
        plans = read_shadow_dataset(lakehouse_root, REBALANCE_PLAN_DATASET)
        plan = _stage_o_plan(plans, plan_id)
        account = load_account_snapshot(account_snapshot)
        existing_seed = read_shadow_dataset(lakehouse_root, SHADOW_ACCOUNT_SEED_DATASET)
        if (
            not existing_seed.empty
            and existing_seed["account_id"].astype(str).eq(account.account_id).any()
        ):
            raise ShadowRunError(["missing_or_duplicate_seed"])
        seed_date = _stage_o_plan_date(plan)
        frame = build_shadow_seed_rows(
            plan=plan,
            account=account,
            account_snapshot_path=account_snapshot,
            seed_date=seed_date,
        )
        artifact = append_dataset(
            lakehouse_root=lakehouse_root,
            dataset_name=SHADOW_ACCOUNT_SEED_DATASET,
            frame=frame,
            partition_parts=[("account_id", account.account_id)],
        )
    except ShadowRunError as exc:
        _raise_shadow_run_error(exc)
    click.echo(f"account_id={account.account_id} seed_date={seed_date.isoformat()}")
    click.echo(f"artifact={artifact}")


@main.command("update-local-shadow")
@click.option("--account-id", default="etf-aw-paper", show_default=True)
@click.option("--initial-asset", type=float, default=1_000_000.0, show_default=True)
@click.option("--seed-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def update_local_shadow(
    account_id: str,
    initial_asset: float,
    seed_date: str | None,
    end_date: str | None,
    lakehouse_root: Path,
) -> None:
    """Update a research-only shadow account from local lakehouse artifacts."""

    if initial_asset <= 0 or not math.isfinite(initial_asset):
        raise click.ClickException("initial-asset must be finite and positive")
    try:
        result = update_local_shadow_artifacts(
            account_id=account_id,
            initial_asset=initial_asset,
            seed_date=(
                None if seed_date is None else _parse_date(seed_date, "seed-date")
            ),
            end_date=None if end_date is None else _parse_date(end_date, "end-date"),
            lakehouse_root=lakehouse_root,
        )
    except ShadowRunError as exc:
        _raise_shadow_run_error(exc)
    click.echo(
        "account_id={} seed_date={} seed_created={} observations_written={}".format(
            result["account_id"],
            result["seed_date"],
            str(result["seed_created"]).lower(),
            result["observations_written"],
        )
    )
    if result["seed_artifact"]:
        click.echo(f"seed_artifact={result['seed_artifact']}")


@main.command("record-paper-decision")
@click.option(
    "--decision",
    "decision_path",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    required=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def record_paper_decision(decision_path: Path, lakehouse_root: Path) -> None:
    """Append one manual paper decision for a Stage N plan."""

    try:
        decision = load_decision_input(decision_path)
        plans = read_shadow_dataset(lakehouse_root, REBALANCE_PLAN_DATASET)
        plan = _stage_o_plan(plans, decision.plan_id)
        decisions = read_shadow_dataset(lakehouse_root, PAPER_DECISION_DATASET)
        fills = read_shadow_dataset(lakehouse_root, PAPER_FILL_DATASET)
        if (
            not decisions.empty
            and decisions["plan_id"].astype(str).eq(decision.plan_id).any()
        ):
            raise ShadowRunError(["duplicate_decision"])
        if (
            decision.decision.value == "CANCELLED"
            and not fills.empty
            and fills["plan_id"].astype(str).eq(decision.plan_id).any()
        ):
            raise ShadowRunError(["fill_before_confirmation"])
        frame = build_decision_row(plan=plan, decision=decision)
        artifact = append_dataset(
            lakehouse_root=lakehouse_root,
            dataset_name=PAPER_DECISION_DATASET,
            frame=frame,
            partition_parts=[("plan_id", decision.plan_id)],
        )
    except ShadowRunError as exc:
        _raise_shadow_run_error(exc)
    click.echo(f"plan_id={decision.plan_id} decision={decision.decision.value}")
    click.echo(f"artifact={artifact}")


@main.command("record-paper-fill")
@click.option(
    "--fill",
    "fill_path",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    required=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def record_paper_fill(fill_path: Path, lakehouse_root: Path) -> None:
    """Append one manual paper fill for a confirmed Stage N plan."""

    try:
        fill = load_fill_input(fill_path)
        plans = read_shadow_dataset(lakehouse_root, REBALANCE_PLAN_DATASET)
        plan = _stage_o_plan(plans, fill.plan_id)
        decisions = read_shadow_dataset(lakehouse_root, PAPER_DECISION_DATASET)
        fills = read_shadow_dataset(lakehouse_root, PAPER_FILL_DATASET)
        frame = build_fill_row(
            plan=plan,
            decisions=decisions,
            existing_fills=fills,
            fill=fill,
        )
        artifact = append_dataset(
            lakehouse_root=lakehouse_root,
            dataset_name=PAPER_FILL_DATASET,
            frame=frame,
            partition_parts=[("plan_id", fill.plan_id)],
        )
    except ShadowRunError as exc:
        _raise_shadow_run_error(exc)
    click.echo(f"fill_id={fill.fill_id} plan_id={fill.plan_id}")
    click.echo(f"artifact={artifact}")


@main.command("build-shadow-observation")
@click.option("--account-id", required=True, type=str)
@click.option("--observation-date", required=True, type=str)
@click.option(
    "--price-snapshot",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    required=True,
)
@click.option(
    "--baseline-observation",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    default=None,
)
@click.option(
    "--note",
    "note_path",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    default=None,
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def build_shadow_observation(
    account_id: str,
    observation_date: str,
    price_snapshot: Path,
    baseline_observation: Path | None,
    note_path: Path | None,
    output_dir: Path,
    lakehouse_root: Path,
) -> None:
    """Build one Stage O daily shadow account observation."""

    parsed_date = _parse_date(observation_date, "observation-date")
    try:
        price = load_price_snapshot_input(price_snapshot)
        baseline = (
            None
            if baseline_observation is None
            else load_baseline_observation_input(baseline_observation)
        )
        note = (
            "" if note_path is None else note_path.read_text(encoding="utf-8").strip()
        )
        frame, review = build_shadow_observation_rows(
            account_id=account_id,
            observation_date=parsed_date,
            price_snapshot=price,
            baseline=baseline,
            note=note,
            seed=read_shadow_dataset(lakehouse_root, SHADOW_ACCOUNT_SEED_DATASET),
            observations=read_shadow_dataset(
                lakehouse_root, SHADOW_OBSERVATION_DATASET
            ),
            decisions=read_shadow_dataset(lakehouse_root, PAPER_DECISION_DATASET),
            fills=read_shadow_dataset(lakehouse_root, PAPER_FILL_DATASET),
            plans=read_shadow_dataset(lakehouse_root, REBALANCE_PLAN_DATASET),
        )
        artifact = append_dataset(
            lakehouse_root=lakehouse_root,
            dataset_name=SHADOW_OBSERVATION_DATASET,
            frame=frame,
            partition_parts=[
                ("year", parsed_date.year),
                ("month", f"{parsed_date.month:02d}"),
            ],
        )
    except ShadowRunError as exc:
        _raise_shadow_run_error(exc)
    output_dir.mkdir(parents=True, exist_ok=True)
    review_path = output_dir / f"shadow-observation-{account_id}-{parsed_date}.json"
    review_path.write_text(
        json.dumps(review, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    click.echo(
        f"account_id={account_id} observation_date={parsed_date.isoformat()} rows={len(frame)}"
    )
    click.echo(f"total_asset={review['total_asset']:.2f}")
    click.echo(f"artifact={artifact}")
    click.echo(f"review={review_path}")


@main.command("shadow-post-mortem")
@click.option("--plan-id", required=True, type=str)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def shadow_post_mortem(plan_id: str, output_dir: Path, lakehouse_root: Path) -> None:
    """Write a read-only Stage O post-mortem for one plan."""

    try:
        payload = build_post_mortem(
            plan_id,
            read_shadow_dataset(lakehouse_root, REBALANCE_PLAN_DATASET),
            read_shadow_dataset(lakehouse_root, PAPER_DECISION_DATASET),
            read_shadow_dataset(lakehouse_root, PAPER_FILL_DATASET),
            read_shadow_dataset(lakehouse_root, SHADOW_OBSERVATION_DATASET),
        )
    except ShadowRunError as exc:
        _raise_shadow_run_error(exc)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"shadow-post-mortem-{plan_id}.json"
    md_path = output_dir / f"shadow-post-mortem-{plan_id}.md"
    json_path.write_text(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(post_mortem_markdown(payload), encoding="utf-8")
    click.echo(f"plan_id={plan_id} status={payload['derived_status']}")
    click.echo(f"json={json_path}")
    click.echo(f"markdown={md_path}")


@main.command("shadow-performance-report")
@click.option("--account-id", required=True, type=str)
@click.option("--start-date", required=True, type=str)
@click.option("--end-date", required=True, type=str)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
def shadow_performance_report(
    account_id: str,
    start_date: str,
    end_date: str,
    output_dir: Path,
    lakehouse_root: Path,
) -> None:
    """Write read-only shadow-performance-report HTML and JSON files."""

    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    try:
        payload = build_performance_report(
            account_id=account_id,
            start=start,
            end=end,
            seed=read_shadow_dataset(lakehouse_root, SHADOW_ACCOUNT_SEED_DATASET),
            observations=read_shadow_dataset(
                lakehouse_root, SHADOW_OBSERVATION_DATASET
            ),
            fills=read_shadow_dataset(lakehouse_root, PAPER_FILL_DATASET),
            plans=read_shadow_dataset(lakehouse_root, REBALANCE_PLAN_DATASET),
        )
    except ShadowRunError as exc:
        _raise_shadow_run_error(exc)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"shadow-performance-report-{account_id}-{start}-{end}"
    json_path = output_dir / f"{stem}.json"
    html_path = output_dir / f"{stem}.html"
    json_path.write_text(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    html_path.write_text(performance_report_html(payload), encoding="utf-8")
    click.echo(
        f"account_id={account_id} observations={payload['integrity']['observation_count']}"
    )
    click.echo(f"json={json_path}")
    click.echo(f"html={html_path}")


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
    lakehouse_root: Path,
) -> None:
    """Write the Stage M robustness report from frozen backtest artifacts."""

    start = _parse_date(start_date, "start-date")
    end = _parse_date(end_date, "end-date")
    with _lakehouse_service(lakehouse_root) as service:
        inputs = {
            "kernel": service._read_partitioned_dataset(
                "derived.etf_aw_backtest_kernel",
                _ROBUSTNESS_HISTORY_START,
                end,
                StorageZone.DERIVED,
            ),
            "target_weight": service._read_partitioned_dataset(
                "derived.etf_aw_target_weight",
                _ROBUSTNESS_HISTORY_START,
                end,
                StorageZone.DERIVED,
            ),
            "baseline_weight": service._read_partitioned_dataset(
                "derived.etf_aw_baseline_weight",
                _ROBUSTNESS_HISTORY_START,
                end,
                StorageZone.DERIVED,
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


def _has_duplicate_rebalance_plan(existing: pd.DataFrame, plan_id: str) -> bool:
    """Return whether the same Stage N plan id already exists."""

    if existing.empty or "plan_id" not in existing.columns:
        return False
    return bool(existing["plan_id"].astype(str).eq(plan_id).any())


def _append_rebalance_plan_rows(
    existing: pd.DataFrame, frame: pd.DataFrame
) -> pd.DataFrame:
    """Append a new plan to any existing monthly artifact rows."""

    if existing.empty:
        return frame
    if set(existing.columns) != set(frame.columns):
        raise ValueError("schema mismatch: cannot append plan with different columns")
    return pd.concat([existing, frame], ignore_index=True)


def _raise_rebalance_plan_error(
    blocking_reasons: list[str], diagnostics: dict[str, object]
) -> None:
    """Print machine-readable diagnostics and fail the CLI command."""

    payload = {
        "blocking_reasons": blocking_reasons,
        "diagnostics": diagnostics,
    }
    click.echo(json.dumps(payload, sort_keys=True, ensure_ascii=False), err=True)
    raise SystemExit(1)


def _stage_o_plan(plans: pd.DataFrame, plan_id: str) -> pd.DataFrame:
    """Return one complete Stage N plan for Stage O commands."""

    if plans.empty or "plan_id" not in plans.columns:
        raise ShadowRunError(["missing_plan"])
    plan = plans[plans["plan_id"].astype(str).eq(plan_id)].copy()
    if plan.empty:
        raise ShadowRunError(["missing_plan"])
    return plan


def _stage_o_plan_date(plan: pd.DataFrame) -> date:
    """Return the Stage N plan date for seed partitioning and validation."""

    if "plan_date" not in plan.columns:
        raise ShadowRunError(["missing_plan"])
    value = pd.to_datetime(plan.iloc[0]["plan_date"], errors="coerce")
    if pd.isna(value):
        raise ShadowRunError(["missing_plan"])
    return value.date()


def update_local_shadow_artifacts(
    *,
    account_id: str,
    initial_asset: float,
    seed_date: date | None,
    end_date: date | None,
    lakehouse_root: Path,
) -> dict[str, object]:
    """Update a research shadow account from local lakehouse artifacts."""

    target_weight = read_shadow_dataset(lakehouse_root, "derived.etf_aw_target_weight")
    sleeve_daily = read_shadow_dataset(lakehouse_root, "derived.etf_aw_sleeve_daily")
    baseline = read_shadow_dataset(lakehouse_root, "derived.etf_aw_backtest_kernel")
    existing_seed = read_shadow_dataset(lakehouse_root, SHADOW_ACCOUNT_SEED_DATASET)
    seed_created = False
    account_seed = (
        existing_seed[existing_seed["account_id"].astype(str).eq(account_id)].copy()
        if not existing_seed.empty and "account_id" in existing_seed.columns
        else pd.DataFrame()
    )
    if account_seed.empty:
        seed_frame, resolved_seed_date = _local_shadow_seed_frame(
            target_weight=target_weight,
            sleeve_daily=sleeve_daily,
            account_id=account_id,
            initial_asset=initial_asset,
            seed_date=seed_date,
        )
        seed_artifact = append_dataset(
            lakehouse_root=lakehouse_root,
            dataset_name=SHADOW_ACCOUNT_SEED_DATASET,
            frame=seed_frame,
            partition_parts=[("account_id", account_id)],
        )
        seed_created = True
    else:
        resolved_seed_date = pd.Timestamp(account_seed.iloc[0]["seed_date"]).date()
        seed_artifact = ""
    written = _append_local_shadow_observations(
        lakehouse_root=lakehouse_root,
        account_id=account_id,
        seed_date=resolved_seed_date,
        end_date=end_date,
        sleeve_daily=sleeve_daily,
        baseline=baseline,
    )
    return {
        "account_id": account_id,
        "seed_date": resolved_seed_date.isoformat(),
        "seed_created": seed_created,
        "observations_written": written,
        "seed_artifact": seed_artifact,
    }


def _local_shadow_seed_frame(
    *,
    target_weight: pd.DataFrame,
    sleeve_daily: pd.DataFrame,
    account_id: str,
    initial_asset: float,
    seed_date: date | None,
) -> tuple[pd.DataFrame, date]:
    """Build a research-only shadow seed from local target weights and closes."""

    if target_weight.empty:
        raise ShadowRunError(["missing_target_weight"])
    if sleeve_daily.empty:
        raise ShadowRunError(["missing_or_invalid_close_price"])
    weights = target_weight.copy()
    weights["rebalance_date"] = pd.to_datetime(
        weights["rebalance_date"], errors="coerce"
    ).dt.date
    weights = weights[weights["target_weight_status"].astype(str).eq("complete")]
    complete_dates = _complete_weight_dates_for_seed(weights)
    if not complete_dates:
        raise ShadowRunError(["missing_target_weight"])
    resolved_seed_date = seed_date or complete_dates[-1]
    eligible = [value for value in complete_dates if value <= resolved_seed_date]
    if not eligible:
        raise ShadowRunError(["missing_target_weight"])
    source_weight_date = eligible[-1]
    selected_weights = weights[weights["rebalance_date"].eq(source_weight_date)].copy()
    prices = _local_close_prices(sleeve_daily, resolved_seed_date)
    positions = []
    market_value_sum = 0.0
    for role in ETF_AW_SLEEVE_ROLE_ORDER:
        symbol = ETF_AW_SLEEVE_CODE_BY_ROLE[role]
        row = selected_weights[selected_weights["sleeve_code"].astype(str).eq(symbol)]
        if len(row) != 1:
            raise ShadowRunError(["missing_target_weight"])
        price = prices[symbol]
        target_notional = initial_asset * float(row.iloc[0]["target_weight"])
        quantity = int(target_notional // (price * 100)) * 100
        market_value = quantity * price
        market_value_sum += market_value
        positions.append(
            AccountPosition(
                symbol=symbol,
                quantity=quantity,
                available_quantity=quantity,
                market_value=market_value,
            )
        )
    snapshot_at = datetime.combine(
        resolved_seed_date, time(hour=15), tzinfo=timezone.utc
    )
    account = AccountSnapshot(
        account_id=account_id,
        snapshot_at=snapshot_at.isoformat(),
        cash=initial_asset - market_value_sum,
        total_asset=initial_asset,
        positions=positions,
    )
    plan = selected_weights.rename(columns={"sleeve_code": "symbol"}).copy()
    plan["account_id"] = account_id
    plan["plan_id"] = f"local-target-weight:{source_weight_date.isoformat()}"
    return (
        build_shadow_seed_rows(
            plan=plan,
            account=account,
            account_snapshot_path=Path("local-lakehouse"),
            seed_date=resolved_seed_date,
            recorded_at=snapshot_at,
        ),
        resolved_seed_date,
    )


def _append_local_shadow_observations(
    *,
    lakehouse_root: Path,
    account_id: str,
    seed_date: date,
    end_date: date | None,
    sleeve_daily: pd.DataFrame,
    baseline: pd.DataFrame,
) -> int:
    """Append missing research shadow observations from local lakehouse closes."""

    complete_dates = _complete_price_dates(sleeve_daily)
    if not complete_dates:
        raise ShadowRunError(["missing_or_invalid_close_price"])
    resolved_end = end_date or complete_dates[-1]
    observations = read_shadow_dataset(lakehouse_root, SHADOW_OBSERVATION_DATASET)
    start_after = seed_date
    if not observations.empty and "account_id" in observations.columns:
        account_obs = observations[
            observations["account_id"].astype(str).eq(account_id)
        ]
        if not account_obs.empty:
            start_after = max(
                seed_date,
                pd.to_datetime(
                    account_obs["observation_date"], errors="coerce"
                ).dt.date.max(),
            )
    seed = read_shadow_dataset(lakehouse_root, SHADOW_ACCOUNT_SEED_DATASET)
    decisions = read_shadow_dataset(lakehouse_root, PAPER_DECISION_DATASET)
    fills = read_shadow_dataset(lakehouse_root, PAPER_FILL_DATASET)
    plans = read_shadow_dataset(lakehouse_root, REBALANCE_PLAN_DATASET)
    written = 0
    for observation_date in [
        value for value in complete_dates if start_after < value <= resolved_end
    ]:
        frame, _ = build_shadow_observation_rows(
            account_id=account_id,
            observation_date=observation_date,
            price_snapshot=_local_price_snapshot(sleeve_daily, observation_date),
            baseline=_local_baseline_observation(baseline, observation_date),
            note="local lakehouse research shadow observation",
            seed=seed,
            observations=observations,
            decisions=decisions,
            fills=fills,
            plans=plans,
            generated_at=datetime.combine(
                observation_date, time(hour=15), tzinfo=timezone.utc
            ),
        )
        append_dataset(
            lakehouse_root=lakehouse_root,
            dataset_name=SHADOW_OBSERVATION_DATASET,
            frame=frame,
            partition_parts=[
                ("year", observation_date.year),
                ("month", f"{observation_date.month:02d}"),
            ],
        )
        observations = (
            frame
            if observations.empty
            else pd.concat([observations, frame], ignore_index=True)
        )
        written += 1
    return written


def _complete_weight_dates_for_seed(weights: pd.DataFrame) -> list[date]:
    """Return complete target-weight rebalance dates usable as seed sources."""

    return [
        value
        for value in sorted(weights["rebalance_date"].dropna().unique())
        if set(
            weights.loc[weights["rebalance_date"].eq(value), "sleeve_code"].astype(str)
        )
        == set(ETF_AW_SLEEVE_CODES)
    ]


def _complete_price_dates(sleeve_daily: pd.DataFrame) -> list[date]:
    """Return dates with valid close prices for every frozen sleeve."""

    rows = sleeve_daily.copy()
    rows["trade_date"] = pd.to_datetime(rows["trade_date"], errors="coerce").dt.date
    rows = rows[
        rows["trade_date"].notna()
        & rows["sleeve_code"].astype(str).isin(ETF_AW_SLEEVE_CODES)
        & pd.to_numeric(rows["close"], errors="coerce").gt(0)
    ]
    return [
        value
        for value in sorted(rows["trade_date"].dropna().unique())
        if set(rows.loc[rows["trade_date"].eq(value), "sleeve_code"].astype(str))
        == set(ETF_AW_SLEEVE_CODES)
    ]


def _local_close_prices(
    sleeve_daily: pd.DataFrame, trade_date: date
) -> dict[str, float]:
    """Return local close prices for one complete ETF AW trade date."""

    rows = sleeve_daily.copy()
    rows["trade_date"] = pd.to_datetime(rows["trade_date"], errors="coerce").dt.date
    rows = rows[rows["trade_date"].eq(trade_date)]
    prices = {
        str(row["sleeve_code"]): float(row["close"]) for _, row in rows.iterrows()
    }
    missing = [
        symbol
        for symbol in ETF_AW_SLEEVE_CODES
        if symbol not in prices
        or not math.isfinite(prices[symbol])
        or prices[symbol] <= 0
    ]
    if missing:
        raise ShadowRunError(
            ["missing_or_invalid_close_price"], {"missing_symbols": missing}
        )
    return prices


def _local_price_snapshot(
    sleeve_daily: pd.DataFrame, observation_date: date
) -> PriceSnapshotInput:
    """Build a Stage O price snapshot from local sleeve daily closes."""

    prices = _local_close_prices(sleeve_daily, observation_date)
    price_as_of = datetime.combine(observation_date, time(hour=15), tzinfo=timezone.utc)
    return PriceSnapshotInput(
        price_as_of=price_as_of,
        prices=[
            ClosePriceItem(
                symbol=symbol,
                close_price=prices[symbol],
                price_trade_date=observation_date,
            )
            for symbol in ETF_AW_SLEEVE_CODES
        ],
    )


def _local_baseline_observation(
    baseline: pd.DataFrame, observation_date: date
) -> BaselineObservationInput | None:
    """Build an optional baseline observation from the local backtest kernel."""

    if baseline.empty:
        return None
    rows = baseline.copy()
    rows["observation_date"] = pd.to_datetime(
        rows["observation_date"], errors="coerce"
    ).dt.date
    rows = rows[
        rows["observation_date"].eq(observation_date)
        & rows["strategy_name"].astype(str).eq("static_inverse_vol")
        & rows["strategy_version"].astype(str).eq("static_inverse_vol_v1")
        & rows["observation_type"].astype(str).eq("daily_nav")
    ]
    if rows.empty:
        return None
    row = rows.sort_values("ingested_at").iloc[-1]
    return BaselineObservationInput(
        observation_date=observation_date,
        strategy_name="static_inverse_vol",
        strategy_version="static_inverse_vol_v1",
        baseline_daily_return=float(row["portfolio_return"]),
        baseline_net_value=float(row["net_value"]),
        source_artifact="derived.etf_aw_backtest_kernel",
    )


def _raise_shadow_run_error(error: ShadowRunError) -> None:
    """Print machine-readable Stage O diagnostics and fail the CLI command."""

    payload = {
        "blocking_reasons": error.reasons,
        "diagnostics": error.diagnostics,
    }
    click.echo(json.dumps(payload, sort_keys=True, ensure_ascii=False), err=True)
    raise SystemExit(1)


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
    if target_daily_dates and not target_weight_dates:
        blocking.append("target weight is incomplete inside comparable range")
    if baseline_daily_dates and not baseline_weight_dates:
        blocking.append("baseline weight is incomplete inside comparable range")
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
    diagnostics = _diagnostics(
        target_frame, comparable_start, comparable_end
    ) + _diagnostics(baseline_frame, comparable_start, comparable_end)
    if _blocking_diagnostics(diagnostics):
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
    gross_metrics: dict[str, float | None] = {}
    if not blocked:
        rows = _daily_nav(frame, coverage)
        returns = [float(row["portfolio_return"]) for _, row in rows.iterrows()]
        gross_final_nav = float((1.0 + pd.Series(returns, dtype=float)).prod())
        gross_metrics = _backtest_metric_values(returns, [], gross_final_nav)
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
    turnover = _turnover_by_date(frame, coverage)
    costs = _cost_fractions(
        rows,
        turnover,
        cost_bps,
        initial_formation_date=_initial_formation_date(frame),
    )
    if cost_bps == 0:
        net_returns = returns
        estimated_cost_sum = 0.0
    else:
        net_returns = [
            ((1.0 - (cost or 0.0)) * (1.0 + gross_return)) - 1.0
            for gross_return, cost in zip(returns, costs, strict=True)
        ]
        estimated_cost_sum = sum(cost for cost in costs if cost is not None)
    final_nav = float((1.0 + pd.Series(net_returns, dtype=float)).prod())
    metrics = _backtest_metric_values(net_returns, [], final_nav)
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
    fallback_returns = (
        rows["net_value"]
        .astype(float)
        .pct_change()
        .fillna(rows["net_value"].astype(float) - 1.0)
    )
    if "portfolio_return" not in rows.columns:
        rows["portfolio_return"] = fallback_returns
    else:
        rows["portfolio_return"] = pd.to_numeric(
            rows["portfolio_return"], errors="coerce"
        )
        rows["portfolio_return"] = rows["portfolio_return"].where(
            rows["portfolio_return"].apply(
                lambda value: _finite_float(value) is not None
            ),
            fallback_returns,
        )
    return rows


def _kernel_metrics(frame: pd.DataFrame, coverage: dict) -> dict[str, float | None]:
    if frame.empty:
        return {}
    if coverage["comparable_start_date"] is None:
        return {}
    start = date.fromisoformat(coverage["comparable_start_date"])
    end = date.fromisoformat(coverage["comparable_end_date"])
    metrics = frame[frame["observation_type"].astype(str).eq("metric")]
    metrics = metrics.copy()
    metrics["observation_date"] = pd.to_datetime(
        metrics["observation_date"], errors="coerce"
    ).dt.date
    metrics = metrics[
        metrics["observation_date"].notna()
        & (metrics["observation_date"] >= start)
        & (metrics["observation_date"] <= end)
    ].copy()
    metrics = metrics.sort_values(["metric_name", "observation_date"])
    metrics = metrics.drop_duplicates(["metric_name"], keep="last")
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
    daily_rows: pd.DataFrame,
    turnover: dict[date, float],
    cost_bps: int,
    initial_formation_date: date | None,
) -> list[float | None]:
    if cost_bps == 0:
        return [0.0 for _ in range(len(daily_rows))]
    rate = cost_bps / 10000.0
    costs = []
    for _, row in daily_rows.iterrows():
        current_date = row["observation_date"]
        if current_date == initial_formation_date:
            costs.append(None)
        else:
            costs.append(2.0 * turnover.get(current_date, 0.0) * rate)
    return costs


def _initial_formation_date(frame: pd.DataFrame) -> date | None:
    """Return the unfiltered initial formation turnover date for one strategy."""

    if frame.empty:
        return None
    rows = frame[frame["observation_type"].astype(str).eq("turnover")].copy()
    if rows.empty:
        return None
    rows["observation_date"] = pd.to_datetime(
        rows["observation_date"], errors="coerce"
    ).dt.date
    rows["metric_value"] = pd.to_numeric(rows["metric_value"], errors="coerce")
    rows = rows[
        rows["observation_date"].notna()
        & rows["metric_value"].notna()
        & rows["metric_value"].apply(math.isfinite)
    ].copy()
    if rows.empty:
        return None
    zero_turnover = rows[rows["metric_value"].eq(0.0)]
    if zero_turnover.empty:
        return None
    return min(zero_turnover["observation_date"].tolist())


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
        "target_weight",
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
        if _complete_weight_group(group):
            result.add(rebalance_date)
    return result


def _complete_weight_group(group: pd.DataFrame) -> bool:
    if len(set(group["sleeve_code"].astype(str))) != 5:
        return False
    for status_column in ("target_weight_status", "baseline_weight_status"):
        if status_column in group.columns and not all(
            group[status_column].astype(str).eq("complete")
        ):
            return False
    weights = [_finite_float(value) for value in group["target_weight"].tolist()]
    if any(value is None or value < 0.0 for value in weights):
        return False
    return abs(sum(value or 0.0 for value in weights) - 1.0) <= 1e-6


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


def _diagnostics(
    frame: pd.DataFrame, start: date | None = None, end: date | None = None
) -> list[dict]:
    if frame.empty:
        return []
    rows = frame[frame["observation_type"].astype(str).eq("diagnostic")]
    if start is not None and end is not None and not rows.empty:
        rows = rows.copy()
        rows["observation_date"] = pd.to_datetime(
            rows["observation_date"], errors="coerce"
        ).dt.date
        rows = rows[
            rows["observation_date"].notna()
            & (rows["observation_date"] >= start)
            & (rows["observation_date"] <= end)
        ]
    result = []
    for value in rows["quality_notes_json"].tolist():
        if isinstance(value, str):
            result.append(json.loads(value))
    return result


def _blocking_diagnostics(diagnostics: list[dict]) -> list[dict]:
    blocking = []
    for item in diagnostics:
        severity = str(item.get("severity", "")).lower()
        status = str(item.get("status", "")).lower()
        if item.get("blocking") is True or severity in {"error", "fail", "blocking"}:
            blocking.append(item)
        elif status in {"error", "fail", "blocked", "blocking"}:
            blocking.append(item)
    return blocking


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
