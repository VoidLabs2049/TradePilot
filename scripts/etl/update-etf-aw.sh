#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  cat <<'USAGE'
Usage:
  ./scripts/etl/update-etf-aw.sh [options]
  ./scripts/etl/update-etf-aw.sh --scheduled

Examples:
  ./scripts/etl/update-etf-aw.sh
  ./scripts/etl/update-etf-aw.sh --scheduled
  ./scripts/etl/update-etf-aw.sh --dry-run
  ./scripts/etl/update-etf-aw.sh --full-refresh
  ./scripts/etl/update-etf-aw.sh --start 2026-05-01 --end 2026-06-07
  ./scripts/etl/update-etf-aw.sh --repair-days 90
  ./scripts/etl/update-etf-aw.sh --codes 510300.SH,159845.SZ

Daily cron example:
  30 18 * * 1-5 cd /path/to/TradePilot && ./scripts/etl/update-etf-aw.sh --scheduled

This command runs the normal ETF all-weather data pipeline: it downloads the
project-defined source datasets and rebuilds derived tables. It does not use
review-only website sources such as eastmoney, tencent, or sina.
On a fresh clone, or when local lakehouse parquet coverage is missing, it backfills
from the project history starts automatically. Use --full-refresh to force that path.

--scheduled is the systemd/cron wrapper mode: it switches to the repository root,
takes a non-blocking lock, enters nix develop, and appends logs to logs/etf-aw-update.log.
Options are passed to:
  python -m tradepilot.etl.update_etf_aw_data
USAGE
  exit 0
fi

if [ "${1:-}" = "--scheduled" ]; then
  shift
  if [ "$#" -ne 0 ]; then
    echo "--scheduled does not accept extra arguments" >&2
    exit 2
  fi

  SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

  cd "$REPO_ROOT"
  mkdir -p logs

  if command -v flock >/dev/null 2>&1; then
    FLOCK_BIN="$(command -v flock)"
  else
    FLOCK_BIN="/run/current-system/sw/bin/flock"
  fi

  if command -v nix >/dev/null 2>&1; then
    NIX_BIN="$(command -v nix)"
  else
    NIX_BIN="/run/current-system/sw/bin/nix"
  fi

  exec "$FLOCK_BIN" -n /tmp/tradepilot-etf-aw-update.lock \
    "$NIX_BIN" develop --command bash -lc \
    './scripts/etl/update-etf-aw.sh && python -m tradepilot.etf_aw.cli update-local-shadow' \
    >> logs/etf-aw-update.log 2>&1
fi

python -m tradepilot.etl.update_etf_aw_data "$@"
