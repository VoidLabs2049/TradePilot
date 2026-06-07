#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  cat <<'USAGE'
Usage:
  ./scripts/etl-review/export-etf-aw-sources.sh --view ETF_CODE START END [options]
  ./scripts/etl-review/export-etf-aw-sources.sh ETF_CODE START END [options]
  ./scripts/etl-review/export-etf-aw-sources.sh --full-history [options]

Examples:
  ./scripts/etl-review/export-etf-aw-sources.sh --view 510300 2026-06-01 2026-06-07
  ./scripts/etl-review/export-etf-aw-sources.sh --view 510300 2026-04-01 2026-06-07 --dataset snapshot
  ./scripts/etl-review/export-etf-aw-sources.sh 511010 2026-06-01 2026-06-07
  ./scripts/etl-review/export-etf-aw-sources.sh 510300.SH 2026-05-01 2026-06-07 --sources local,tencent,sina
  ./scripts/etl-review/export-etf-aw-sources.sh 511010 2026-06-01 2026-06-07 --sources local,tencent,sina
  ./scripts/etl-review/export-etf-aw-sources.sh --full-history
  ./scripts/etl-review/export-etf-aw-sources.sh --full-history --codes 511010.SH --start 2016-01-01 --end 2026-06-07
  ./scripts/etl-review/export-etf-aw-sources.sh 511010 2026-06-01 2026-06-07 --sources investing --investing-url https://cn.investing.com/etfs/guotai-sse-deliverable-5-tb-historical-data
  ./scripts/etl-review/export-etf-aw-sources.sh 511010 2026-06-01 2026-06-07 --sources xueqiu --xueqiu-cookie 'xq_a_token=...; u=...'

--view is read-only unless --csv is passed. Comparison CSV files are written under
data/source-review by default. Full-history CSVs are written under
data/source-review/full-history by default.
USAGE
  exit 0
fi

if [ "${1:-}" = "--view" ]; then
  shift
  python -m tools.etl_review.view_etf_aw "$@"
  exit $?
fi

python -m tools.etl_review.export_etf_aw_sources "$@"
