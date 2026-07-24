# Milestone 01 — Local Capability And Category Map

## Question
> What relevant data-access capability already exists locally in the repo and what categories does the strategy actually need?

## Why It Matters
The availability question should start from reality. Existing wrappers and verified project providers matter more than hypothetical sources.

## Status
`complete`

## Done Condition
- identify local accessible wrappers/providers
- define the strategy's major data categories
- distinguish directly usable from still missing

## Source Boundary
- Primary sources:
- local durable memory on ETF and macro frameworks
- local provider code under `daily-workflow/workflow/providers/`
- Secondary sources:
- none

## Confirmed Findings
- The repo already has active `tushare` and `akshare` integration in the daily workflow stack.
- Local provider code already fetches:
- index daily quotes via Tushare and AKShare fallback
- ETF proxy quotes via Tushare `fund_daily` and AKShare `fund_etf_spot_em`
- trade calendars via Tushare
- style-relative strength via Tushare index history
- The strategy's required data categories can be grouped into:
1. ETF/fund history and metadata
2. index history and benchmark mapping
3. domestic macro monthly data
4. rates / money-market / curve data
5. gold and commodity proxy data
6. futures/options sentiment data
7. credit spread proxies
8. overseas overlay data

## Analysis / Judgments
- Local capability is already enough to support a serious v1 for prices, proxies, and part of the macro layer.
- The main missing piece is not raw market prices but a disciplined source map for slow macro, rates, and credit-adjacent indicators.
- Options sentiment and credit-spread sophistication should be treated as optional until the core allocation engine works.

## Hypotheses / Unresolved
- Some data may exist locally through wrappers but still be too brittle for production reliance.
- ETF AUM/份额 fields may require cross-validation across Tushare, exchange sites, and Eastmoney.

## Blockers
- none for the category-mapping stage

## Next Recommended Step
- survey external sources and compare official source, wrapper, and fallback path by category

## Promotion Candidates
- Research note:
- Durable memory:
- Skill:
