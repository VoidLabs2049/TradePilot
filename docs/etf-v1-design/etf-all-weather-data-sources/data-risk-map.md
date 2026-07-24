# ETF All-Weather Data Risk Map

## Purpose

This document answers a harder question than “what data can we get?”

It asks:

> Which fields are most likely to create false confidence, through what mechanism, and how should the strategy defend itself?

The goal is not perfect safety.
The goal is to prevent the most common forms of self-deception before the first notebook already bakes them in.

---

## The Three Main Risk Classes

For this strategy, most data failures belong to one of three classes:

### 1. Timing Risk

The value itself may be correct, but it is used earlier than it was actually knowable.

Typical forms:
- using month-end macro values before release date
- aligning monthly macro to daily price bars without lag
- using same-day close to both decide and execute unrealistically

### 2. Semantic Drift Risk

The field name remains the same, but what it means changes over time.

Typical forms:
- M1 definition changes
- ETF sleeve changes benchmark or character
- a proxy asset stops representing the intended risk exposure cleanly

### 3. Structural Interpretation Risk

The data is real and timely, but the strategy asks it to answer a question it cannot reliably answer.

Typical forms:
- treating a noisy macro field as a precise state label
- treating a bond ETF as pure duration exposure when credit and liquidity are mixed in
- treating one credit proxy as if it captured full financial stress

---

## High-Risk Fields And Why They Mislead

## 1. `official_pmi` / `official_pmi_mom`

### Risk Type
- Timing risk
- Structural interpretation risk

### False-Signal Mechanism
- The market often moves before the official release.
- The index level itself is not enough; direction, subcomponents, and context matter.
- A single PMI print can be over-read as a clean regime switch.

### Why This Is Dangerous
- It creates the illusion of a crisp macro state where reality is transitional.
- It encourages overconfident tilt changes.

### Defense Rule
- Use real release dates only.
- Treat PMI as one component of a score, not a direct quadrant label.
- Prefer `direction + confirmation` over one-print threshold logic.

---

## 2. `cpi_yoy` / `core_cpi_yoy`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- Headline CPI in China can be heavily distorted by food and especially pork cycles.
- The strategy may mistake temporary food-driven inflation for broad macro heating.

### Why This Is Dangerous
- It can incorrectly tilt away from bonds or into commodity-like exposures.

### Defense Rule
- Prefer core CPI when available.
- Treat headline CPI as a noisy context field, not a high-authority driver.
- Cross-check with PPI, rates, and market pricing before changing risk budgets materially.

---

## 3. `ppi_yoy` / `ppi_mom`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- PPI is highly informative, but mostly for industrial pricing pressure and upstream profit logic.
- It can be mistaken for a broad inflation regime signal when it may mainly reflect sectoral supply-demand conditions.

### Why This Is Dangerous
- It can overweight commodity/reflation interpretations in periods where final-demand recovery is weak.

### Defense Rule
- Use PPI as a strong industrial-cycle field, not as standalone economy-wide inflation truth.
- Read it together with PMI, credit, and bond-market response.

---

## 4. `m1_yoy` / `m2_yoy` / `m1_m2_spread`

### Risk Type
- Semantic drift risk
- Structural interpretation risk

### False-Signal Mechanism
- M1 definition changed in 2025.
- Historical relationships that once looked predictive may not survive across definition regimes.
- M1-M2 spread is psychologically attractive because it looks simple and directional.

### Why This Is Dangerous
- It creates false precision from a field whose institutional meaning has changed.
- It invites narrative overuse: “money is getting active, therefore risk assets must work.”

### Defense Rule
- Explicitly tag pre-2025 and post-2025 regimes.
- Avoid long-history direct comparability without adjustment.
- Treat M1/M2 as supporting evidence, not sole driver.

---

## 5. `tsf_yoy` / `credit_impulse_proxy`

### Risk Type
- Timing risk
- Structural interpretation risk

### False-Signal Mechanism
- Credit data is high-value but noisy in monthly detail.
- Total volume and structure can tell different stories.
- A short burst in financing can be policy noise rather than durable transmission.

### Why This Is Dangerous
- It can create premature “policy works” conclusions.

### Defense Rule
- Prefer smoother yoy or impulse-style construction over raw monthly jumps.
- When possible, separate total amount from structure.
- Require confirmation from market layer before strong risk tilt.

---

## 6. `cn_gov_10y_yield` / `cn_yield_curve_slope_10y_1y`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- Curve moves can reflect growth expectations, policy signaling, liquidity, or positioning.
- The same yield change can mean different things in different policy contexts.

### Why This Is Dangerous
- It can make the bond sleeve look more “macro-clean” than it really is.

### Defense Rule
- Do not interpret yield moves in isolation.
- Pair with liquidity fields and actual bond-sleeve price action.
- Use curve fields as explanatory anchors, not as single-trigger switches.

---

## 7. `bond_close`

### Risk Type
- Semantic drift risk
- Structural interpretation risk

### False-Signal Mechanism
- The chosen bond ETF may mix duration, credit, liquidity, or policy-bank exposure in ways that drift from the intended sleeve role.

### Why This Is Dangerous
- The strategy thinks it owns “duration defense” when it may partially own another risk package.

### Defense Rule
- Document the exact product exposure.
- Validate benchmark, duration character, and liquidity.
- Treat bond sleeve as an instrument-specific exposure, not an abstract idealized bond factor.

---

## 8. `gold_close`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- Gold can respond to real rates, USD, geopolitics, positioning, and domestic flows simultaneously.
- It is tempting to narrate every gold move as a single macro message.

### Why This Is Dangerous
- The strategy may overfit a tidy gold story onto a mixed driver set.

### Defense Rule
- Use gold primarily as a hedge sleeve and market-confirmation field.
- Avoid assuming a one-to-one mapping between gold and one macro quadrant.

---

## 9. `hs300_vs_zz1000_20d`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- Relative strength is informative, but it may reflect liquidity, policy preference, retail risk appetite, valuation rotation, or short squeeze dynamics.

### Why This Is Dangerous
- It can be mistaken for a pure macro growth signal when it is partly a market micro-regime signal.

### Defense Rule
- Treat it as market confirmation, not macro truth.
- Use it to confirm or throttle allocation, not to define the state alone.

---

## 10. `fund_share` / `aum` / `fund_scale`

### Risk Type
- Timing risk
- Semantic drift risk

### False-Signal Mechanism
- Different sites report scale and shares with different lags and definitions.
- AUM can change because of market movement, subscriptions, or both.

### Why This Is Dangerous
- It can create fake confidence about liquidity and capacity.

### Defense Rule
- Use these as validation fields, not alpha fields.
- Cross-check across at least two sources before making hard exclusions.

---

## 11. `market_breadth_proxy`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- Breadth is seductive because it compresses many stocks into one number.
- But breadth definitions vary, stability varies, and it may capture short-term mood more than allocation-relevant structure.

### Why This Is Dangerous
- It can smuggle high-frequency noise into a monthly system.

### Defense Rule
- Keep breadth optional in v1.
- Only include it if the source definition is stable and the use case is narrow.

---

## 12. `adx_14` / `rsi_14`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- These indicators often feel precise because they have formulas and thresholds.
- In practice they easily become decorative complexity in a low-frequency allocation system.

### Why This Is Dangerous
- They create technical certainty without necessarily improving regime allocation.

### Defense Rule
- Demote them to optional execution filters.
- `ATR / realized vol` should have higher priority than `ADX / RSI` in v1.

---

## 13. `us10y_yield` / `dxy_proxy` / `oil_proxy`

### Risk Type
- Structural interpretation risk

### False-Signal Mechanism
- Global overlay fields are real and often useful, but they can overpower domestic signals if treated as direct domestic state drivers.

### Why This Is Dangerous
- The system may become a pseudo-global macro model without the infrastructure to support that ambition.

### Defense Rule
- Keep these as overlay or veto/context fields.
- Do not let them dominate domestic regime classification in v1.

---

## The Most Dangerous Illusions

These are the top failure illusions to watch.

### Illusion 1 — “Official Data Means Safe Data”

False.
Official data can still be used too early, interpreted too literally, or mapped to the wrong exposure.

### Illusion 2 — “More Fields Means More Robustness”

False.
More fields often mean more degrees of freedom, more narrative flexibility, and more hidden overfitting.

### Illusion 3 — “A Formula Creates Objectivity”

False.
Indicators such as RSI, ADX, scorecards, and even ERC can still be fed by unstable or misinterpreted inputs.

### Illusion 4 — “If Several Sources Agree, It Must Be True”

Not necessarily.
Wrappers often inherit the same underlying source and can repeat the same error with different packaging.

---

## Priority Risk Ranking For v1

### Highest Risk

1. macro release-date leakage
2. M1/M2 semantic drift
3. mis-specified bond sleeve exposure
4. over-reading single macro prints as regime shifts

### Medium Risk

1. ETF scale / AUM inconsistency
2. style-relative strength over-interpretation
3. gold narrative overfitting
4. breadth noise entering a monthly system

### Lower But Still Real Risk

1. decorative technical indicators
2. excessive global overlay influence

---

## Practical Defensive Rules

### Rule 1

Every slow field must carry a `release_date` or documented publication lag.

### Rule 2

Every field with changing institutional meaning must carry a `regime_note`.

Example:
- `m1_definition_regime = pre_2025 / post_2025`

### Rule 3

Every sleeve must have an `exposure_note`.

Example:
- “this bond sleeve is a tradable proxy for duration defense, not a pure duration factor.”

### Rule 4

No field should be both:
- poorly understood
- timing-sensitive
- heavily weighted

If all three are true, it is too dangerous for v1.

### Rule 5

Whenever a field feels intuitively powerful, treat that as a warning sign to demand extra validation.

This is the WYSIATI defense: coherent stories are precisely where false confidence grows fastest.

---

## Final Judgment

The data risk problem is not mainly about missing data.

It is about preventing real data from being given fake precision.

For this strategy, the best defense is:

- correct release timing
- explicit semantic-regime annotations
- sparse v1 feature set
- using market confirmation as throttle rather than pretending macro states are clean

That is how the data layer stays honest enough for the first research loop.
