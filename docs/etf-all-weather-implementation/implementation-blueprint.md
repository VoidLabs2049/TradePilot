# ETF All-Weather Implementation Blueprint

## Core Question

If we want to build a real ETF all-weather system from the existing research note, what should we actually implement first?

## Short Answer

Implement a `small, transparent, monthly risk-allocation engine` first.

Do not start from:
- Bridgewater mythology
- full machine learning
- full learnable ERC
- a giant asset universe

Start from:
- a small sleeve set
- release-date-aligned features
- transparent regime scoring
- static base budgets
- simple risk balancing
- strict validation discipline

---

## Upstream Dependency

This implementation project is **downstream** of:

- `/.the-one/research/projects/etf-all-weather-data-sources`

The correct relationship is:

- `etf-all-weather-data-sources`
  - defines the frozen `v1` sleeve set
  - defines the frozen field boundary
  - defines source priority and fallback paths
  - defines release/effective-date discipline
  - defines the ETF return convention
  - defines the monthly decision clock
- `etf-all-weather-implementation`
  - turns those frozen research conclusions into schema, pipeline, notebook MVP, backtest, and rollout sequence

This means the implementation project should treat the data-sources project as an **input contract**, not as optional background reading.

### Primary upstream handoff file

Before implementation work proceeds, read:

- `/.the-one/research/projects/etf-all-weather-data-sources/developer-handoff-summary.md`

### Boundary rule

This project should **not** casually reopen questions already frozen upstream, including:

- the canonical 5-sleeve `v1` definition
- the `v1` field boundary
- the adjustment-aware ETF return convention
- the post-20th monthly rebalance rule
- the defer decision on deep learning / options sentiment / full credit system

Only reopen those if implementation reveals a concrete blocker rather than a mere preference.

---

## What The Strategy Is Actually Trying To Solve

This strategy is not trying to answer:

- which ETF will go up the most next month?

It is trying to answer:

- how should risk be distributed across a few ETF sleeves so the portfolio is not crippled when macro weather changes?

That means the objective function is closer to:

- survive regime shifts
- reduce single-regime fragility
- participate when aligned
- de-risk when confidence is weak

This is why the right mental model is `allocation engine`, not `prediction engine`.

---

## Recommended V1 Specification

### Sleeves

Use `4-5` sleeves only:

1. large-cap equity ETF
2. small-cap equity ETF
3. bond / duration defense sleeve
4. gold sleeve
5. cash or short-duration sleeve

Delay:
- commodity sleeve if representation is messy
- CTA sleeve if instrument path is unclear
- overseas sleeve until domestic core works

### Feature Stack

#### Layer 1 — Macro Slow Variables

- PMI level / momentum
- PPI direction or momentum
- M1-M2 / credit pulse proxy
- optional credit spread proxy

#### Layer 2 — Market Confirmation

- large-cap vs small-cap relative strength
- bond trend
- gold trend
- optional style/breadth confirmation

#### Layer 3 — Technical Execution Filters

- MA distance or cross
- ATR / realized vol for scaling
- optional ADX for trend quality

### State Engine

V1 should be rules-based.

Outputs:
- regime score
- confidence score
- target sleeve risk budgets

Suggested interpretation:
- low confidence -> stay close to neutral base budgets
- high confidence -> allow moderate tilt, not all-in regime bets

### Weight Engine

V1 choices, in preferred order:

1. budgeted inverse-vol approximation
2. simplified ERC
3. only later: learnable ERC

The optimizer's job is to translate budgets into risk-balanced sleeves, not to manufacture alpha by itself.

### Rebalance Logic

- monthly fixed schedule first
- optional threshold overlay later

Reason:
- aligns with macro release cadence
- easier to validate
- lower turnover

---

## Verification Checklist

Before trusting any backtest, verify all of these:

1. macro data aligned to actual publication dates
2. features only use information available at decision time
3. ETF sleeves are liquid and realistically tradable
4. turnover and cost included
5. benchmark comparisons included
6. parameter perturbation does not destroy the result
7. month-by-month decisions are explainable

If these are not satisfied, the result is analysis theater.

---

## Benchmarks The Strategy Must Beat Or At Least Justify Against

The system should be compared with:

1. static equal weight
2. static inverse-vol or risk-parity-like baseline
3. simple stock-bond-gold fixed mix
4. rule-score tilt without ERC complexity

If complex architecture does not materially improve one of:
- drawdown behavior
- regret reduction across regimes
- robustness after cost

then the complexity is probably not earned.

---

## Main China-Specific Risks

1. policy-driven regime shifts can outrun textbook macro quadrants
2. domestic inflation signals are less clean than in classic Western all-weather narratives
3. M1/M2 and credit variables have changing definition and institutional meaning
4. ETF proxies may not perfectly express the intended sleeve exposure
5. a copied US-style asset map may create false comfort

This is why the domestic adaptation should lean toward `growth x credit` with market confirmation, rather than worshiping the pure original label.

---

## Practical Build Sequence

### Step 1 — Notebook MVP

Build one notebook that can:
- load sleeves
- load and align macro features
- score regime
- generate budgets
- compute weights
- run monthly backtest

### Step 2 — Explainability Table

For every rebalance date, output:
- feature snapshot
- regime score
- confidence
- target budgets
- resulting weights

If this table is not interpretable, the model is too opaque for this stage.

### Step 3 — Baseline Comparison Pack

Produce one report comparing v1 against the simple baselines.

### Step 4 — Shadow Run

Run monthly paper allocations with frozen decisions and post-mortem review.

### Step 5 — Tiny Live Pilot

Only after shadow behavior is clean.

---

## Anti-Rationalization Rules

1. Do not call it all-weather just because there are multiple sleeves.
2. Do not call it robust just because drawdown was lower in one backtest.
3. Do not introduce ML because the rule model is emotionally unsatisfying.
4. Do not treat optimizer output as evidence if regime scores themselves are weak.
5. Do not let Bridgewater branding hide local market mismatch.

---

## Final Judgment

The best way to land this strategy is not to imitate an institution.

It is to build a modest but honest allocation engine whose decisions can be explained, checked, stress-tested, and gradually promoted from research to shadow to live.
