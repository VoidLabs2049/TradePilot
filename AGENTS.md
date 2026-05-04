# AGENTS.md

Repository instructions for coding agents working on TradePilot.

## Operating Principles

Apply the Karpathy-style guidelines whenever writing, reviewing, refactoring, or designing code in this repository.

### Think Before Coding

- State assumptions explicitly before implementation when the task is ambiguous.
- Surface tradeoffs instead of silently choosing between plausible interpretations.
- Prefer the simpler approach when it satisfies the request.
- Ask a clarifying question if missing information would make the change risky.

### Simplicity First

- Write the minimum code that solves the requested problem.
- Do not add speculative features, abstractions, flexibility, or configuration.
- Avoid error handling for impossible scenarios.
- If an implementation becomes large for a small task, simplify before continuing.

### Surgical Changes

- Touch only files and lines directly related to the request.
- Do not refactor, reformat, or "improve" adjacent code unless required.
- Match existing style even when a different style would be preferable.
- Remove imports, variables, and functions made unused by your own changes.
- Mention unrelated dead code or issues instead of deleting them.

### Goal-Driven Execution

- Convert work into verifiable success criteria.
- For bug fixes, prefer a failing test or clear reproduction before the fix.
- For validation changes, cover invalid and valid cases where practical.
- For multi-step work, state a short plan with verification for each step.

## Project Overview

TradePilot is an A-share assisted decision dashboard with a separated backend and frontend.

- Backend: Python + FastAPI + DuckDB
- Frontend: React 18 + TypeScript + Vite + Ant Design
- Development environment: Nix Flakes

Current product direction is workflow-first:

- Backend main path: daily workflow data platform, pre/post workflow, context/insight contract, watch config, news sync, and dashboard support.
- Frontend main path: Dashboard as The-One insight-first / TradePilot context-fallback, with Portfolio retained as the position input UI.

## Environment Rules

- Manage the development environment with `flake.nix` only.
- Assume `nix develop` is active.
- Do not use `pip`, `uv`, or `poetry`.
- Run Python programs as modules, for example `python -m package.module`.

## Common Commands

Backend:

```bash
python -c "from tradepilot.main import app; print('OK')"
python -m unittest discover
python -m unittest -v path/to/test_file.py
python -m uvicorn tradepilot.main:app --reload
```

Frontend:

```bash
cd webapp
yarn dev
yarn build
```

## Python Rules

- Use builtin `unittest` for tests.
- Use Pydantic v2 for schemas and domain models.
- Use PyTorch and JAX for ML models.
- Use `loguru` for logging.
- Use `click` for CLI and argument parsing.
- Prefer `pathlib` over `os.path`.
- Use explicit `StrEnum` / `IntEnum` for enums.
- Use absolute imports; do not use relative imports such as `from .x import y`.
- Prefer specific imports, for example `from pydantic import BaseModel`.
- Add type hints to all function parameters and return values.
- Use builtin generics such as `list`, `dict`, and `tuple`.
- Use `MyType | None` instead of `Optional[MyType]`.
- Write docstrings for public modules, functions, classes, methods, and public-facing APIs.
- Do not repeat types in docstring `Args:` sections when signatures already contain type hints.

## Frontend Rules

- Use React 18, TypeScript, Vite, Ant Design, `@ant-design/icons`, `@ant-design/charts`, and `react-router-dom`.
- Keep `/` focused on the Dashboard workflow experience.
- Keep `/portfolio` focused on position CRUD and trade records.
- Do not use legacy pages as the default location for new product work.
- Keep typed API contracts in `webapp/src/services/api.ts` aligned with backend API changes.
- The Vite API proxy maps `/api` to `http://localhost:8000`.

## Module Rules

Read the relevant `.claude/rules/*.md` file before changing files covered by its `paths` frontmatter. These files are the source of detailed module guidance.

| Area | Paths | Rules |
| --- | --- | --- |
| Backend | `tradepilot/**/*.py` | `.claude/rules/tradepilot-backend.md` |
| API routes | `tradepilot/api/**/*.py` | `.claude/rules/api-routes.md` |
| Data provider and ingestion | `tradepilot/data/**/*.py`, `tradepilot/ingestion/**/*.py`, `tradepilot/collector/**/*.py` | `.claude/rules/data-provider.md` |
| Analysis engine | `tradepilot/analysis/**/*.py` | `.claude/rules/analysis-engine.md` |
| Frontend | `webapp/src/**/*.tsx`, `webapp/src/**/*.ts`, `webapp/package.json`, `webapp/vite.config.ts` | `.claude/rules/webapp-frontend.md` |

### Backend Architecture Notes

- `tradepilot/main.py` is the FastAPI entrypoint and mounts summary, portfolio, collector, briefing, workflow, and scheduler routers.
- `tradepilot/db.py` owns DuckDB connection management and table initialization.
- `tradepilot/workflow/models.py` and `tradepilot/workflow/service.py` own context, insight, workflow orchestration, and news mapping.
- `tradepilot/summary/models.py` and `tradepilot/api/summary.py` own richer watch config and backward-compatible normalization.
- `tradepilot/collector/news.py` handles real news collection from Cailian Press and Eastmoney.

### API Notes

- Main frontend API surfaces are `/api/workflow`, `/api/summary`, `/api/scheduler`, and `/api/portfolio`.
- Keep route changes backward compatible unless the user explicitly requests a breaking change.
- Update typed frontend API contracts when backend response shapes change.

### Data Provider And Ingestion Notes

- Use the three-layer data access model: Provider for structured market data, Collector for content, and Ingestion Service for orchestration.
- `get_provider()` selects Mock or AKShare provider by config.
- `DataProvider` defines the structured data interface; new providers must implement the full interface.
- Each sync path should record an ingestion run.

### Market Data Unit Conventions

- ETF daily `volume` / source `vol` follows Tushare `fund_daily` units: hands.
- ETF daily `amount` follows Tushare `fund_daily` units: thousand CNY.
- `derived.etf_aw_sleeve_daily` inherits those units without conversion.
- Future schema, API, dashboard, or strategy design must explicitly label units for market data fields and document any unit conversion at the transformation boundary.

### Analysis Notes

- Analysis modules receive DataFrame inputs and return signal lists or score dictionaries.
- `signal.py` aggregates module outputs into a weighted 0-100 score.
- `risk.py` independently evaluates take-profit and stop-loss conditions.

## Project Structure

```text
tradepilot/          Python backend
  main.py            FastAPI entrypoint
  config.py          Configuration
  db.py              DuckDB connection and table initialization
  data/              Data provider layer
  ingestion/         Market/news/Bilibili sync orchestration
  collector/         News and content collectors
  workflow/          Pre/post workflow and context/insight contract
  api/               REST API routes
  portfolio/         Portfolio management
  scheduler/         Scheduled jobs

webapp/              React frontend
  src/pages/         Daily Workflow and Portfolio pages
  src/services/      API client wrappers and typed contracts
  src/components/    Shared components

docs/                Project documentation
  系统设计.md         Architecture, data requirements, signal logic
  投资策略.md         Investment strategy source text
  worklog.md         Work log
```

## Adding A New Module

- Create `.claude/rules/<module>.md` with `paths:` frontmatter for relevant files.
- Add the module to the rules table in `CLAUDE.md`.
- Include key files, architecture, design patterns, and testing guidance.
- Update this `AGENTS.md` if the new module changes repository-wide agent behavior.

## Success Signal

These instructions are working when diffs stay focused, implementations remain simple, module-specific rules are consulted before edits, and changes are verified with the narrowest useful test or build command.
