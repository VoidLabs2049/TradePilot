# Milestone 02 — Data And Research Pipeline

## Question
> What data and validation pipeline is required so that the strategy can be researched without quietly leaking future information or overfitting to China's unstable regimes?

## Why It Matters
Most allocation systems fail not because the high-level idea is wrong, but because data alignment, release timing, and validation discipline are weak.

## Status
`complete`

## Done Condition
- Define the data layers
- Define the feature families
- Define the backtest protocol and failure checks
- Clarify what should be treated as baseline, score, or optimizer input

## Source Boundary
- Primary sources:
- `.the-one/research/markets/etf-all-weather-quant-framework.md`
- Durable memory on macro indicators and ETF framework
- Secondary sources:
- none in this pass

## Confirmed Findings
- The existing framework already separates three useful feature layers:
- macro slow variables
- market medium variables
- technical fast variables
- The note explicitly warns that macro data must be aligned by real publication date.
- The note already places linear regression in the correct role: baseline / score layer / probability-aid, not final engine.
- The note's own MVP recommends monthly rebalance and a small feature pool, which is appropriate for monthly-quarterly macro identification.

## Analysis / Judgments
- The pipeline should be built in six layers:
1. instrument layer
2. feature layer
3. state-scoring layer
4. risk-budget layer
5. weighting / rebalance layer
6. validation layer
- Feature families for v1 should stay narrow.

### V1 Data Layers

1. Instrument prices
- equity ETFs
- bond ETF or bond-fund proxy
- gold ETF
- cash / short-bond proxy

2. Macro slow variables
- PMI level and momentum
- PPI direction or momentum
- M1-M2 or credit pulse proxy, treated carefully because historical stability is weaker
- credit spread proxy if available

3. Market medium variables
- large-cap vs small-cap relative strength
- bond trend
- gold trend
- market breadth or style proxy if simple and reliable

4. Technical fast variables
- MA cross / distance
- ATR or realized volatility for position scaling
- optional ADX as trend-strength filter

### What The Model Should Output

- regime score or regime probabilities
- confidence level
- target risk budgets by sleeve

It should not directly output full portfolio weights in v1.

### Honest Validation Protocol

1. Use monthly decision frequency first
2. Align all macro features to actual release dates
3. Prefer walk-forward or expanding-window validation
4. Penalize turnover and cost from day one
5. Run parameter perturbation checks
6. Compare against trivial baselines:
- static equal weight
- static risk parity or inverse-vol approximation
- simple 60/40-like domestic baseline
- rule-only regime budget without optimizer complexity

### Core Research Questions For Each Backtest

- Did the strategy reduce drawdown in the intended stress regimes?
- Did it simply repackage one lucky period?
- Is the improvement still present after cost and lag assumptions?
- Are results robust to small changes in MA window, confidence thresholds, or rebalance timing?

### Strong Negative Rules

- No full learnable ERC in v1
- No XGBoost / deep model before rule-based baseline is understood
- No claim of quadrants as clean states; mixed states are normal and confidence should throttle tilt size
- No using one backtest period as proof of portability

## Hypotheses / Unresolved
- Credit-spread proxies may add more signal than CPI/PPI alone in the China context.
- A two-axis domestic system may ultimately look more like `growth x credit` plus a global overlay than textbook `growth x inflation` only.
- Market confirmation may matter more than slow macro turning points for avoiding early or false tilts.

## Blockers
- The exact accessible data vendors and release-calendar implementation details remain open.

## Next Recommended Step
- Convert the research pipeline into a staged build roadmap: notebook prototype, modular backtest engine, shadow portfolio, and live constraints.

## Promotion Candidates
- Research note:
- Durable memory:
- Skill:
