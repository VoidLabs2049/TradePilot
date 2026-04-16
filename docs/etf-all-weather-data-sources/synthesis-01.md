# Synthesis 01 — ETF All-Weather Data Sources

## Covered Milestones
- Milestone 01 — Local Capability And Category Map
- Milestone 02 — External Source Survey

## Stable Findings
- The repo already has enough local capability to support a serious v1 on ETF/index prices and part of the macro layer.
- A clean v1 does not need every possible source; it needs a stable stack for ETF history, domestic macro, rates/liquidity, and a small global overlay.
- The practical v1 stack is: `Tushare + official macro/rates sources + AKShare fallback + FRED overlay + exchange/fund-site validation`.
- Options sentiment, full futures normalization, and robust China credit-spread factors are better delayed to v2.

## Active Tensions
- convenience wrappers versus official-source validation
- breadth of data coverage versus endpoint stability
- whether credit deserves v1 inclusion or later addition

## Ruled Out
- starting from a giant multi-source architecture
- relying solely on undocumented public web endpoints
- treating advanced derivatives sentiment as v1-essential

## What Changed In The Mission Understanding
- the data problem sharpened from “what can we get?” into “what is stable enough, necessary enough, and aligned enough to support an honest monthly allocation engine?”

## Progress Snapshot
- Completed: source survey, field inventory, risk map, reliability plan, Stage 01 real validation, slow-field release-date rules, bond-sleeve selection, cash-sleeve selection, and the frozen v1 canonical field list.
- Stable v1 sleeve set: `510300.SH`, `159845.SZ`, `511010.SH`, `518850.SH`, `159001.SZ`.
- Continuation handoff document: `progress-status.md`.

## Open Work
- v1 schema design is not done yet.
- release/effective-date metadata is specified but not yet encoded into a data model.
- China government yield-curve extraction still needs a paged/windowed historical method.
- Stage 02+ validation items such as AUM/fund_share reconciliation remain open.
- notebook MVP / backtest / explainability table / shadow run are not started in code.

## Recommended Next Stage
- completed in this extension: added a field-level inventory, a data-risk map, a staged data-reliability test plan, a real Stage 01 execution report, explicit release-date rules for v1 slow fields, a bond-sleeve candidate comparison, a cash/short-duration proxy comparison, and a v1 canonical field list; next if desired is to harden the v1 schema around this frozen sleeve set and field boundary

## Promotion Decision
- Promote now:
- Defer: durable memory until the source stack is exercised in code
- Archive only: this project artifact set
