#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/etl-review/view-etf-aw.sh ETF_CODE START_DATE END_DATE [options]

Examples:
  ./scripts/etl-review/view-etf-aw.sh 510300.SH 2026-05-01 2026-05-31
  ./scripts/etl-review/view-etf-aw.sh 510300 2026-05-01 2026-05-31
  ./scripts/etl-review/view-etf-aw.sh 510300.SH 2026-04-01 2026-05-31 --dataset snapshot
  ./scripts/etl-review/view-etf-aw.sh 159845.SZ 2026-05-01 2026-05-31 --csv /tmp/159845.csv

Options are passed to:
  python -m tools.etl_review.view_etf_aw
USAGE
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

if [ "$#" -lt 3 ]; then
  usage
  exit 2
fi

python -m tools.etl_review.view_etf_aw "$@"
