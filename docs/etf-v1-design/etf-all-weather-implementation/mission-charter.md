# Mission Charter — ETF All-Weather Implementation

## Mission Statement
> Turn the existing `etf-all-weather-quant-framework.md` research note into a realistic implementation roadmap for a China ETF all-weather strategy, including MVP scope, data requirements, model boundaries, backtest protocol, and deployment path.

## Scope
- In scope:
- Reuse the existing ETF all-weather framework as the conceptual base
- Define a practical China ETF asset pool and the minimum usable signal stack
- Specify the MVP system architecture from data to rebalancing
- Define what should be rules first, what can be statistical, and what should be delayed
- Propose a staged backtest and live-shadow rollout path
- Out of scope:
- Claiming the strategy is already profitable without code-level verification
- Choosing final ETF tickers without a data-availability and liquidity check
- Writing full production code in this mission pass

## Success Criteria
- The strategy can be described as a concrete system rather than a concept note
- There is a clear MVP with limited assets, limited signals, and limited rebalancing logic
- The implementation path is broken into executable milestones
- Major overfitting and China-specific failure modes are made explicit

## Output Plan
- Expected project artifacts:
- `mission-charter.md`
- `milestone-01-system-boundary-and-mvp.md`
- `milestone-02-data-and-research-pipeline.md`
- `milestone-03-rollout-roadmap.md`
- `synthesis-01.md`
- `implementation-blueprint.md`
- Possible research notes:
- none in this pass
- Possible memory/skill promotion targets:
- none unless a reusable implementation workflow emerges

## Autonomy Boundary
- The kernel may decide without re-confirmation:
- milestone ordering
- MVP versus later-stage boundary
- risk-first architecture choices
- Must re-confirm with the user if:
- the mission should expand into code implementation now
- external data/vendor selection becomes a hard requirement

## Known Unknowns
- Which exact ETF universe is available and liquid enough in the intended brokerage environment
- Which macro and market features are easiest to source with real release-date alignment
- Whether CTA-like exposure must be proxied, omitted, or sourced through funds/FOF wrappers

## Initial Milestones
1. Define the system boundary, MVP asset pool, and what the strategy is really optimizing
2. Define the data, feature, validation, and research pipeline needed to test it honestly
3. Define the staged implementation and rollout path from research to shadow to live
