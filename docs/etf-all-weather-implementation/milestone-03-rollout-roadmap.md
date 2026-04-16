# Milestone 03 — Rollout Roadmap

## Question
> Assuming the strategy concept is sound enough to test, how should it be rolled out from notebook research into something that could eventually run with real money?

## Why It Matters
Without staged rollout, “implementation” collapses into either endless research or premature live deployment.

## Status
`complete`

## Done Condition
- Define the implementation phases
- Define what each phase must prove
- Define what is blocked from graduating early

## Source Boundary
- Primary sources:
- `.the-one/research/markets/etf-all-weather-quant-framework.md`
- Mission milestones 01-02
- Secondary sources:
- none in this pass

## Confirmed Findings
- The research note already suggests a natural sequence: rules first, simple models second, richer models last.
- The biggest practical risk is not optimizer math but false confidence from unstable state classification and weak instrument mapping.
- Monthly or low-frequency operation is aligned with both the macro framework and the creator's working style.

## Analysis / Judgments

### Phase 0 — Research Notebook MVP

Goal:
- prove the full chain can run end to end on historical data

Deliverables:
- asset sleeve return series
- release-date-aligned feature table
- rules-based regime score
- static base risk budgets
- simplified ERC or inverse-vol-plus-budget approximation
- monthly rebalance backtest

Graduate only if:
- the pipeline is reproducible
- regime labels and decisions can be inspected month by month
- results survive basic perturbation tests

### Phase 1 — Modular Research Engine

Goal:
- move from one-off notebook to reusable components

Modules:
- data loaders
- release-date alignment helpers
- feature builders
- regime scorer
- budget generator
- weight optimizer
- backtest evaluator

Graduate only if:
- each module can be rerun independently
- new features or sleeves can be swapped in without rewriting the whole system
- baseline comparisons remain easy

### Phase 2 — Shadow Portfolio

Goal:
- run the strategy forward without capital pressure

Requirements:
- monthly model freeze date
- documented reallocation recommendation
- realized vs expected post-mortem each rebalance

Graduate only if:
- several forward rebalance cycles complete cleanly
- no repeated data-timing or execution-assumption errors appear

### Phase 3 — Small-Capital Live Pilot

Goal:
- test operational reality, not maximize returns

Constraints:
- small capital only
- strict sleeve caps
- manual confirmation allowed
- turnover and slippage tracked explicitly

Graduate only if:
- operational discipline holds
- tracking error to shadow assumptions is explainable
- no evidence emerges that the strategy only “worked” in the backtest environment

### Phase 4 — Optional Enhancements

Only after the above works:
- confidence-weighted budgets
- online expert weighting such as MWU
- additional sleeves such as commodity or overseas exposures
- learnable ERC variants
- richer classification models

### What Should Be Deliberately Delayed

- full machine-learning regime classifier
- CTA proxy sleeve if instrument quality is poor
- highly reactive rebalancing frequency
- large asset universe before the 5-sleeve core is understandable

## Hypotheses / Unresolved
- A shadow phase may reveal that simple risk-budget throttling explains most of the value, making sophisticated optimization unnecessary.
- Domestic ETF proxies may force pragmatic compromises in duration and commodity representation.

## Blockers
- None for architectural planning; actual coding and data access are outside this mission pass.

## Next Recommended Step
- Consolidate these milestones into one implementation blueprint with recommended v1 specification, verification checklist, and explicit anti-rationalization rules.

## Promotion Candidates
- Research note:
- Durable memory:
- Skill:
