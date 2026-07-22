# Milestone 01 — System Boundary And MVP

## Question
> What is the smallest honest version of the ETF all-weather strategy that can be researched and implemented without pretending to solve the whole Bridgewater problem?

## Why It Matters
The framework note is conceptually rich but too broad. Without an MVP boundary, the project will sprawl into a pseudo-institutional architecture that cannot be verified.

## Status
`complete`

## Done Condition
- State the true objective of the strategy
- Define the initial asset universe
- Define the minimum signal stack
- Define what the optimizer is and is not allowed to do in v1

## Source Boundary
- Primary sources:
- `.the-one/research/markets/etf-all-weather-quant-framework.md`
- Durable memory on ETF and macro-asset identification
- Secondary sources:
- none in this pass

## Confirmed Findings
- The research note's strongest idea is not “predict ETF returns” but “allocate risk across macro-weather-sensitive exposures.”
- The note already contains a natural MVP boundary: rules-based quadrant scoring, static base risk budgets, simplified ERC, monthly rebalancing.
- The China context makes the Bridgewater label dangerous if copied too literally: domestic assets are more policy-driven, credit-sensitive, and regime-shifting than textbook US all-weather templates assume.
- The creator's comparative edge is closer to monthly-quarterly environment identification than high-frequency execution.

## Analysis / Judgments
- The strategy should be defined as a `risk allocation engine`, not a return-prediction engine.
- The real optimization target in v1 should be: avoid catastrophic single-regime overexposure while preserving enough participation in favorable regimes.
- The first asset universe should stay small and legible. Recommended initial buckets:
- equity beta: one large-cap broad ETF and one small-cap broad ETF
- duration defense: one treasury/long-duration bond ETF or equivalent fund proxy
- real-rate / stress hedge: one gold ETF
- liquidity buffer: cash or short-bond proxy
- Commodity and CTA sleeves should be delayed unless there is a clean, liquid, and testable instrument path.
- The first state model should not be online learning or ML. It should be a transparent scorecard:
- growth score
- inflation or credit-pressure score
- market confirmation layer
- technical execution filter
- The first optimizer should be `budgeted simplified ERC`, not full learnable ERC. In v1, the model can tilt risk budgets by regime confidence, but the confidence itself should come from transparent rules.

## Hypotheses / Unresolved
- A domestic variant may work better with `growth x credit` as the main driver and use inflation more as a cross-check than as the sole second axis.
- Relative-strength signals between equity and bond/gold sleeves may be more stable than direct absolute-return forecasts.
- Bond sleeve implementation may require flexibility because ETF depth and duration purity differ across products.

## Blockers
- Exact ticker mapping and instrument quality were not verified in this mission pass.

## Next Recommended Step
- Specify the research pipeline: data alignment, feature construction, regime scoring, validation windows, and what constitutes an honest backtest.

## Promotion Candidates
- Research note:
- Durable memory:
- Skill:
