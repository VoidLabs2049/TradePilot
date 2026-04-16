# Mission Charter — ETF All-Weather Data Sources

## Mission Statement
> Deep-research what data is realistically obtainable for a China ETF all-weather strategy, then classify it into v1-essential, validation-only, and better-delayed categories.

## Scope
- In scope:
- China ETF/fund daily history and metadata
- index history
- domestic macro monthly data
- rates, yield curve, and liquidity data
- gold/commodity proxies
- futures/options sentiment availability
- credit spread proxy availability
- overseas macro overlay availability
- Out of scope:
- full coding implementation
- proving signal effectiveness
- selecting final ticker universe

## Success Criteria
- produce a source map by data category
- distinguish official source, practical wrapper, and fallback source
- identify what is realistic for v1 versus better delayed

## Output Plan
- `mission-charter.md`
- `milestone-01-local-capability-and-category-map.md`
- `milestone-02-external-source-survey.md`
- `synthesis-01.md`
- `data-source-map.md`

## Autonomy Boundary
- The kernel may decide data-category grouping and source prioritization without re-confirmation
- Re-confirm only if this should expand into implementation-level schema design

## Known Unknowns
- which vendor endpoints are stable enough for long-term automation
- which exact ETF/fund scale fields are easiest to reconcile across sources
- whether China credit spread proxies are clean enough for v1

## Initial Milestones
1. Check local/project-available data access and define data categories
2. Survey external obtainable sources and classify v1 vs delayed
