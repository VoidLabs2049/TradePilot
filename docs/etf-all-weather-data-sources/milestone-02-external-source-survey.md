# Milestone 02 — External Source Survey

## Question
> Which external data sources are realistically obtainable for this strategy and how should they be prioritized?

## Why It Matters
Having too many candidate sources is almost as bad as having too few. The strategy needs a practical source stack, not a catalog of names.

## Status
`complete`

## Done Condition
- compare main candidate sources by category
- classify sources into primary, fallback, and delayed
- identify v1-realistic source stack

## Source Boundary
- Primary sources:
- web survey of Tushare, AKShare, Eastmoney ecosystem, SSE/SZSE, CSIndex, NBS, PBOC, Chinamoney, ChinaBond, official futures exchanges, SAFE, FRED
- Secondary sources:
- local ETF/macro implementation notes

## Confirmed Findings
- `Tushare Pro` is the strongest single Python-friendly backbone for ETF/index/macro/rates panel assembly, but is token-gated and should not be treated as the sole truth source.
- `AKShare` is the best free wrapper/fallback for public Chinese financial data, but endpoint stability risk is materially higher.
- `Eastmoney / 天天基金` is practical for ETF/fund metadata discovery and fallback collection, but public endpoints are undocumented and brittle.
- `SSE / SZSE` are the right validation surfaces for listed ETF universe and official notices, but not ideal bulk-history APIs.
- `CSIndex / SSE index publisher pages` are important for benchmark truth and methodology, though wrappers are still better for timeseries ingestion.
- `NBS` and `PBOC` are the official domestic macro anchors for monthly-quarterly regime work.
- `Chinamoney`, `Shibor`, and `ChinaBond` are the practical core for rates, interbank liquidity, and yield-curve context.
- Official futures exchanges (`CFFEX`, `SHFE`, `INE`, and later DCE/CZCE) can support commodity/futures overlays, but full multi-exchange sentiment infrastructure is a later-stage task.
- `FRED` is the cleanest overseas overlay source for US rates, USD liquidity, and global risk context.
- China credit-spread proxies are obtainable in principle via `ChinaBond` / `Chinamoney`, but a robust spread system is better delayed to v2.

## Analysis / Judgments
- The realistic v1 source stack is:
1. Tushare for core panels
2. NBS + PBOC + Chinamoney + ChinaBond for official macro/rates confirmation
3. AKShare as wrapper/fallback
4. FRED for global overlay
5. SSE/SZSE/Eastmoney for ETF metadata validation
- The most dangerous false step would be to start with options sentiment or advanced credit spreads before the core price-macro-rates stack is stable.
- Official sources should be the epistemic anchors for slow data; wrappers are collection conveniences, not reasons to lower validation standards.

## Hypotheses / Unresolved
- Credit spread inputs may prove useful only after the bond sleeve and rates layer are already working.
- Some official sites may be awkward enough that wrapper + periodic validation is the best operational compromise.

## Blockers
- no hard blocker for planning, but long-term endpoint stability remains uncertain

## Next Recommended Step
- convert the source map into a concrete v1 schema: table names, refresh cadence, release-date alignment rules, and preferred source per field

## Promotion Candidates
- Research note:
- Durable memory:
- Skill:
