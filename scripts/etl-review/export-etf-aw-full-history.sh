#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  cat <<'USAGE'
Usage:
  ./scripts/etl-review/export-etf-aw-full-history.sh [options]

Examples:
  ./scripts/etl-review/export-etf-aw-full-history.sh
  ./scripts/etl-review/export-etf-aw-full-history.sh --sources local,tencent,sina
  ./scripts/etl-review/export-etf-aw-full-history.sh --codes 511010.SH --start 2016-01-01 --end 2026-06-07
  ./scripts/etl-review/export-etf-aw-full-history.sh --codes 511010 --sources local,eastmoney,tencent,sina

The command exports full-history website CSVs for ETF all-weather sleeves and
writes aggregate comparison files under data/source-review/full-history.
Default sources: local,tencent,sina. Add eastmoney/xueqiu/investing with --sources when needed.
USAGE
  exit 0
fi

python -m tools.etl_review.export_etf_aw_full_history "$@"
