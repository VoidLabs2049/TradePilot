# Synthesis 01 — ETF All-Weather Implementation

## Covered Milestones
- Milestone 01 — System Boundary And MVP
- Milestone 02 — Data And Research Pipeline
- Milestone 03 — Rollout Roadmap

## Stable Findings
- The strategy should be treated as a `risk allocation system`, not as a direct ETF return predictor.
- The smallest honest version is a 4-5 sleeve portfolio with transparent regime scoring, static base budgets, simplified ERC, and monthly rebalance.
- China implementation should not copy textbook Bridgewater `growth x inflation` mechanically; domestic `growth x credit` pressure and policy transmission matter materially.
- The biggest implementation risk is weak data alignment and unstable state inference, not lack of model sophistication.
- The correct rollout path is notebook MVP -> modular research engine -> shadow portfolio -> small-capital live pilot.

## Active Tensions
- Whether inflation deserves equal status with credit in the domestic state model or should be treated as a secondary/global overlay
- Whether bond and CTA sleeves can be represented cleanly enough with available domestic instruments
- How much the optimizer adds beyond a simpler budgeted inverse-vol approach

## Ruled Out
- Starting with full learnable ERC
- Starting with ML-first state classification
- Treating backtest uplift without release-date alignment and cost checks as evidence

## What Changed In The Mission Understanding
- The problem sharpened from “understand all-weather ETF logic” into “design the smallest verifiable allocation engine that can survive Chinese market structure and data reality.”

## Recommended Next Stage
- If desired, the next mission should be implementation-oriented: specify instrument candidates, data sources, folder/module structure, and the first notebook/backtest scaffold.

## Promotion Decision
- Promote now:
- Defer: durable memory and skill promotion until the workflow is tested in code and forward observation
- Archive only: this project artifact set
