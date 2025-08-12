# Repository Guidelines
## Project Structure & Module Organization
- `dbt_wrapper/`: Adapter source and CLI (`typer` app).
- `dbt/`: dbt package scaffolding, macros, and include files.
- `tests/`: Pytest suites — `unit/`, `functional/adapter/`.
- `samples/`, `samples_tests/`: runnable examples and companion tests.
- `scripts/`: helper scripts; `docs/` + `mkdocs.yml` for documentation.
- Root configs: `pyproject.toml`, `.pre-commit-config.yaml`, `tox.ini`, `pytest.ini`, `.flake8`, `Makefile`.
## Build, Test, and Development Commands
- `make dev`: editable install + dev deps; installs pre-commit.
- `make test`: run unit tests via tox and style/type checks.
- `make lint`: run Black/Flake8/Mypy on staged files.
- `tox -e unit`: run unit tests (`tests/unit`).
- `pytest -m unit`: run only unit tests; `-m functional` for adapter tests.
- `dbt_wrapper dev|deploy|build|test <PROJECT>`: workflow-based CLI for Fabric; see docs `user_guide/cli_reference.md`.
## Coding Style & Naming Conventions
- Python: target 3.12; line length 100; use `black` and `ruff`.
- Linting: `flake8` (Black-compatible ignores) and `mypy` (loose, `ignore_missing_imports`).
- Naming: modules `lower_snake_case.py`; functions/vars `snake_case`; classes `CapWords`.
- Pre-commit: `pre-commit install` then `pre-commit run -a` before pushing.
## Testing Guidelines
- Framework: `pytest` with markers `unit`, `functional`, `integration`.
- Locations: add tests under `tests/unit/` or `tests/functional/adapter/`.
- Naming: files `test_*.py` or `*_test.py`; classes `Test*`; functions `test_*`.
- Examples: `pytest -m unit -q`, `pytest --cov dbt_wrapper` for coverage locally.
## Commit & Pull Request Guidelines
- Commits: imperative, concise subject (<= 72 chars), focused diffs.
- Branching: use `feature/<name>`; open PRs into `dev` (not `main`).
- Reference issues with `Fixes #123` or `Refs #123` when applicable.
- PRs: clear description, rationale, tests, and docs updates (when user-facing).
- Include CLI snippets (e.g., `dbt_wrapper dev ...`) or screenshots when behavior changes.
## Security & Configuration Tips
- Secrets: never commit credentials. Use env vars; see `test.env.example`.
- Azure/Fabric: integration tests may require `az login` and `DBT_*` env vars.
- Fabric lakehouse: avoid enabling "Lakehouse schemas (Public Preview)"; use unique table names.
- Local envs: prefer `.venv` and `make dev`; avoid system Python.
- Docs: install extras `pip install -e .[docs]` then `mkdocs serve`.

## Architecture Overview
- clean: remove `target/` and temp artifacts for a fresh run.
- pre-scripts: generate helper notebooks/scripts required before dbt.
- metadata-extract: pull Fabric workspace metadata for notebook generation.
- metadata-download: sync remote metadata to local project cache.
- build: run `dbt build` (compile, run, and test selected models).
- post-scripts: create post-dbt scripts (e.g., validation, utilities).
- upload: publish generated notebooks to Fabric workspace/Lakehouse.
- execute: run the master orchestration notebook in Fabric.
- results: collect execution results and surface status/history locally.
