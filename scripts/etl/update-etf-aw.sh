#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  cat <<'USAGE'
Usage:
  ./scripts/etl/update-etf-aw.sh [options]

Examples:
  ./scripts/etl/update-etf-aw.sh
  ./scripts/etl/update-etf-aw.sh --dry-run
  ./scripts/etl/update-etf-aw.sh --full-refresh
  ./scripts/etl/update-etf-aw.sh --start 2026-05-01 --end 2026-06-07
  ./scripts/etl/update-etf-aw.sh --repair-days 90
  ./scripts/etl/update-etf-aw.sh --codes 510300.SH,159845.SZ

Daily cron example:
  30 18 * * 1-5 cd /path/to/TradePilot && mkdir -p logs && ./scripts/etl/update-etf-aw.sh >> logs/etf-aw-update.log 2>&1

This command downloads ETF all-weather data and rebuilds derived tables.
On a fresh clone, or when local lakehouse parquet coverage is missing, it backfills
from the project history starts automatically. Use --full-refresh to force that path.
Options are passed to:
  python -m tradepilot.etl.update_etf_aw_data
USAGE
  exit 0
fi

python -m tradepilot.etl.update_etf_aw_data "$@"
