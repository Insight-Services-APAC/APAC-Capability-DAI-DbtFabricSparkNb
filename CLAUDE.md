# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the dbt-fabricsparknb adapter, an Insight version of the dbt adapter for Microsoft Fabric that works WITHOUT a connection endpoint like Livy or the Datawarehouse SQL endpoint. It's based on the community dbt-fabricspark adapter.

## Development Commands

### Setup and Installation
```bash
# IMPORTANT: Use uv for package management (not pip)
# Install the package in development mode with all dependencies
uv pip install -e . -r requirements.txt

# Install development dependencies from pyproject.toml
uv pip install -e ".[dev]"

# Install documentation dependencies
uv pip install -e ".[docs]"

# Run pre-commit install after setup
pre-commit install
```

### Testing
```bash
# Run pytest tests
pytest

# Run specific test files
pytest tests/unit/
pytest tests/functional/

# Run tests with coverage
pytest --cov=dbt --cov=dbt_wrapper

# Run Makefile tests (legacy)
make test  # Runs unit tests with py38 and code checks
make unit  # Runs unit tests with py38
```

### Code Quality and Linting
```bash
# Run ruff for linting
ruff check .
ruff format .

# Run pre-commit checks
pre-commit run --all-files

# Legacy Makefile commands
make lint    # Runs flake8 and mypy against staged changes
make flake8  # Runs flake8 only
make mypy    # Runs mypy only
make black   # Runs black formatter
```

### Building and Distribution
```bash
# Build distribution
python setup.py sdist bdist_wheel

# Build script in scripts/
./scripts/build-dist.sh
```

### Documentation
```bash
# Serve documentation locally
mkdocs serve

# Build documentation
mkdocs build

# Deploy documentation with mike
mike deploy --push --update-aliases 0.1 latest
```

### dbt_wrapper CLI Commands
The project includes a custom CLI tool `dbt_wrapper` with the following key commands:

```bash
# Main command for running dbt projects
dbt_wrapper run-all <dbt_project_dir> <output_dir> [options]

# Build and compare metadata between environments
dbt_wrapper buildcomparemetadata <dbt_project_dir> <source_env> <target_env> [dbt_profiles_dir]

# Compare environments
dbt_wrapper compare <dbt_project_dir> <source_env> <target_env> [dbt_profiles_dir]

# Generate catalog
dbt_wrapper generatecatalog <dbt_project_dir> [dbt_profiles_dir]
```

## Architecture

### Core Components

1. **dbt Adapter Implementation** (`dbt/adapters/fabricsparknb/`)
   - `impl.py`: Main adapter implementation extending SQLAdapter
   - `connections.py`: Connection management for Fabric Spark
   - `fabric_spark_credentials.py`: Credential handling
   - `livysession.py`: Livy session management
   - `notebook.py`: Notebook generation and execution
   - `catalog.py`, `manifest.py`, `mock.py`: Supporting utilities

2. **dbt_wrapper Package** (`dbt_wrapper/`)
   - `main.py`: Typer CLI application entry point
   - `wrapper.py`: Core command implementations
   - `stage_executor.py`: Stage-based execution orchestration
   - `generate_files.py`: File generation utilities
   - `fabric_api.py`: Microsoft Fabric API interactions
   - `fabric_sql.py`: SQL operations for Fabric
   - `catalog.py`: Catalog management
   - `log_levels.py`, `hashcheck_levels.py`: Configuration enums

3. **Macros and Templates** (`dbt/include/fabricsparknb/macros/`)
   - SQL macros for Spark operations
   - Jinja2 templates for code generation

### Key Design Patterns

- **Notebook-Based Execution**: Unlike traditional dbt adapters, this generates and executes notebooks in Fabric
- **Stage Executor Pattern**: Operations are organized into stages that can be selectively executed
- **Mock Testing Support**: Includes mock implementations for testing without Fabric connection
- **Metadata Extraction**: Automated extraction and comparison of lakehouse metadata

### Dependencies

- Core: dbt-fabricspark==1.7.0rc1, msfabricpysdkcore
- Azure: azure-identity, azure-core, azure-storage-file-datalake
- Database: pyodbc, sqlparse, sqlparams
- CLI: typer
- Testing: pytest, pytest-asyncio, pytest-cov, pytest-mock

### Environment Configuration

- Supports devcontainer development with Bitnami Spark
- Profiles stored in `~/.dbt/profiles.yml` by default
- Supports multiple Fabric environments (source/target) for comparisons

## Important Notes

- This adapter does NOT require Livy or SQL endpoints - it works directly with Fabric notebooks
- The project includes comprehensive test coverage under `tests/` with both unit and functional tests
- Documentation is hosted on GitHub Pages
- Main branch is `main`, development happens on `dev` branch
- GitHub Actions workflows handle testing and deployment

## Debugging and Testing Tips
- **IMPORTANT**: Always activate the virtual environment first: `source .venv/bin/activate`
- When running the `dbt_wrapper` command in a development environment (non pip installed) you need to run it from the root directory of the project an use the command `python -m dbt_wrapper.main` instead of just `dbt_wrapper`.
- Use `python -m dbt_wrapper.main --help` to see available commands and options.
