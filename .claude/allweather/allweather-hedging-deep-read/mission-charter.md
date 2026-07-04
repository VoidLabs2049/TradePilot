# Mission Charter — AllWeather Hedging Deep Read

## Mission Statement
Deep-read the `all_weather_hedging/` codebase under `/Users/lhh/Desktop/Work/AllWeather/` and produce a module-by-module comparison against The One's existing ETF AllWeather research framework (`markets/etf-all-weather-quant-framework.md`), identifying what is reusable, what needs adaptation, and what structural gaps exist between the two approaches.

## Scope
- In scope:
  - Technical architecture review of all 14 Python files
  - CNN model design, pretraining logic, and inference pipeline
  - Risk parity optimizer and risk engine implementation
  - Deep OTM hedging mechanism and option pricing
  - Real-time streaming pipeline architecture
  - Gap analysis: what The One's framework has but this code lacks, and vice versa
  - Integration assessment: whether this code can serve as a v1 implementation backbone
- Out of scope:
  - Running/evaluating the code on real data
  - Modifying the code
  - Deep-reading other projects in AllWeather/ (RV_Transformer_CTA, 筹码分析)

## Success Criteria
- Every module's design rationale, strengths, and weaknesses are understood
- The structural gap between "our framework" and "their implementation" is explicitly mapped
- A concrete judgment is made: which modules can be directly inherited, which need adaptation, and what is missing entirely

## Output Plan
- `mission-charter.md`
- `milestone-01-module-deep-read.md` — module-by-module analysis
- `milestone-02-gap-comparison.md` — side-by-side comparison with our framework
- `synthesis-01.md` — final judgment and inheritance recommendation

## Autonomy Boundary
- The kernel may decide module reading order and analysis depth without re-confirmation
- Must re-confirm if the mission should expand to other projects in AllWeather/

## Known Unknowns
- The docs/ PDFs are image-based and cannot be read — user may provide context orally
- Whether this code was actually tested/runned successfully is unknown
- The relationship between this project and RV_Transformer_CTA / 筹码分析 is unclear
