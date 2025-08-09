---
title: Complete CLI Command Reference
excerpt: Comprehensive reference for all dbt_wrapper CLI commands and options
sidebar_label: CLI Commands
slug: /reference/cli_commands
weight: 1
---

# Complete CLI Command Reference

This is the comprehensive reference for all `dbt_wrapper` CLI commands, generated from the actual CLI help text.

## Main Command

### `dbt_wrapper`

**Usage:**
```bash
dbt_wrapper [OPTIONS] COMMAND [ARGS]...
```

**Global Options:**
- `--install-completion` - Install completion for the current shell
- `--show-completion` - Show completion for the current shell, to copy it or customize the installation
- `--help` - Show this message and exit

## Workflow Commands

These are the primary commands for running dbt transformation workflows.

### `dbt_wrapper dev`

🚀 **Development workflow** - Quick iteration for local development

**Usage:**
```bash
dbt_wrapper dev [OPTIONS] [DBT_PROJECT_DIR]
```

**Description:**
Runs: clean → pre-scripts → metadata-extract → build → post-scripts

Perfect for rapid development and testing cycles.

**Arguments:**
- `[DBT_PROJECT_DIR]` - The path to the dbt_project directory [default: .]

**Options:**
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--skip TEXT` - Comma-separated list of stages to skip [default: None]
- `--only TEXT` - Comma-separated list of stages to run exclusively [default: None]
- `--select TEXT` - dbt resource selection syntax [default: ""]
- `--exclude TEXT` - dbt resource exclude syntax [default: ""]
- `--help` - Show this message and exit

**Examples:**
```bash
# Basic development workflow
dbt_wrapper dev my_project

# Skip cleaning for faster iteration
dbt_wrapper dev my_project --skip clean

# Focus on staging models
dbt_wrapper dev my_project --select models/staging
```

### `dbt_wrapper deploy`

🚢 **Deploy workflow** - Full deployment pipeline

**Usage:**
```bash
dbt_wrapper deploy [OPTIONS] [DBT_PROJECT_DIR]
```

**Description:**
Runs: clean → pre-scripts → metadata-extract → metadata-download → build → post-scripts → upload → execute → results

Complete pipeline with Fabric deployment and execution.

**Arguments:**
- `[DBT_PROJECT_DIR]` - The path to the dbt_project directory [default: .]

**Options:**
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--skip TEXT` - Comma-separated list of stages to skip [default: None]
- `--only TEXT` - Comma-separated list of stages to run exclusively [default: None]
- `--select TEXT` - dbt resource selection syntax [default: ""]
- `--exclude TEXT` - dbt resource exclude syntax [default: ""]
- `--help` - Show this message and exit

**Examples:**
```bash
# Full deployment
dbt_wrapper deploy my_project

# Deploy without execution
dbt_wrapper deploy my_project --skip execute,results
```

### `dbt_wrapper build`

🔨 **Build workflow** - Minimal build only

**Usage:**
```bash
dbt_wrapper build [OPTIONS] [DBT_PROJECT_DIR]
```

**Description:**
Runs: metadata-download → build

Just builds the dbt project with minimal overhead.

**Arguments:**
- `[DBT_PROJECT_DIR]` - The path to the dbt_project directory [default: .]

**Options:**
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--select TEXT` - dbt resource selection syntax [default: ""]
- `--exclude TEXT` - dbt resource exclude syntax [default: ""]
- `--help` - Show this message and exit

**Examples:**
```bash
# Quick build
dbt_wrapper build my_project

# Build specific models
dbt_wrapper build my_project --select models/staging
```

### `dbt_wrapper test`

🧪 **Test workflow** - Validation-focused pipeline

**Usage:**
```bash
dbt_wrapper test [OPTIONS] [DBT_PROJECT_DIR]
```

**Description:**
Runs: clean → metadata-extract → metadata-download → build → validation

Ensures quality without deployment.

**Arguments:**
- `[DBT_PROJECT_DIR]` - The path to the dbt_project directory [default: .]

**Options:**
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--skip TEXT` - Comma-separated list of stages to skip [default: None]
- `--select TEXT` - dbt resource selection syntax [default: ""]
- `--exclude TEXT` - dbt resource exclude syntax [default: ""]
- `--help` - Show this message and exit

**Examples:**
```bash
# Run full test workflow
dbt_wrapper test my_project

# Test without cleaning
dbt_wrapper test my_project --skip clean
```

### `dbt_wrapper run`

🎯 **Custom workflow** - Run workflows from configuration file or interactively

**Usage:**
```bash
dbt_wrapper run [OPTIONS]
```

**Description:**
Run a workflow from configuration file or interactively.

Examples:
- `dbt_wrapper run --workflow ci`
- `dbt_wrapper run --interactive`
- `dbt_wrapper run -w production -c ./my-config.yml`

**Options:**
- `--workflow TEXT` - Workflow name from configuration file [default: None]
- `--config TEXT` - Path to configuration file [default: None]
- `--project-dir TEXT` - The path to the dbt_project directory [default: .]
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--interactive` - Run in interactive mode [default: False]
- `--help` - Show this message and exit

**Examples:**
```bash
# Run configured workflow
dbt_wrapper run --workflow ci

# Interactive mode
dbt_wrapper run --interactive

# Custom config file
dbt_wrapper run --workflow prod --config ./prod-config.yml
```

## Stage Management Commands

### `dbt_wrapper stage`

**Usage:**
```bash
dbt_wrapper stage [OPTIONS] COMMAND [ARGS]...
```

**Description:**
Manage and run individual pipeline stages

#### `dbt_wrapper stage list`

List all available pipeline stages

**Usage:**
```bash
dbt_wrapper stage list [OPTIONS]
```

**Options:**
- `--help` - Show this message and exit

**Example Output:**
```
Available Stages
┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Stage             ┃ Description                  ┃ Used In               ┃
┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━┩
│ clean             │ Clean target directory       │ dev, deploy, test, ci │
│ pre-scripts       │ Generate pre-dbt scripts     │ dev, deploy, test     │
│ metadata-extract  │ Extract metadata from Fabric │ dev, deploy, test, ci │
│ metadata-download │ Download metadata locally    │ deploy, build, test   │
│ build             │ Build dbt project            │ all workflows         │
│ post-scripts      │ Generate post-dbt scripts    │ dev, deploy, test     │
│ upload            │ Upload notebooks to Fabric   │ deploy                │
│ execute           │ Execute master notebook      │ deploy                │
│ validate          │ Run validation checks        │ test, ci              │
│ results           │ Retrieve execution results   │ deploy                │
└───────────────────┴──────────────────────────────┴───────────────────────┘
```

#### `dbt_wrapper stage describe`

Get detailed information about a specific stage

**Usage:**
```bash
dbt_wrapper stage describe [OPTIONS] STAGE_NAME
```

**Arguments:**
- `STAGE_NAME` - Name of the stage to describe [required]

**Options:**
- `--help` - Show this message and exit

**Examples:**
```bash
dbt_wrapper stage describe build
dbt_wrapper stage describe metadata-extract
```

#### `dbt_wrapper stage run`

Run specific pipeline stages

**Usage:**
```bash
dbt_wrapper stage run [OPTIONS] STAGES...
```

**Arguments:**
- `STAGES...` - Stage names to run [required]

**Options:**
- `--project-dir TEXT` - The path to the dbt_project directory [default: .]
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--help` - Show this message and exit

**Examples:**
```bash
# Run multiple stages
dbt_wrapper stage run clean metadata-extract build

# Run single stage
dbt_wrapper stage run build
```

## Configuration Management Commands

### `dbt_wrapper config`

**Usage:**
```bash
dbt_wrapper config [OPTIONS] COMMAND [ARGS]...
```

**Description:**
Manage pipeline configuration files

#### `dbt_wrapper config init`

Create an example configuration file

**Usage:**
```bash
dbt_wrapper config init [OPTIONS]
```

**Options:**
- `--path TEXT` - Path where to create the configuration file [default: None]
- `--help` - Show this message and exit

**Examples:**
```bash
# Create in current directory
dbt_wrapper config init

# Create in specific location
dbt_wrapper config init --path ./config/.dbt-wrapper.yml
```

#### `dbt_wrapper config list`

List workflows in configuration file

**Usage:**
```bash
dbt_wrapper config list [OPTIONS]
```

**Options:**
- `--config TEXT` - Path to configuration file [default: None]
- `--help` - Show this message and exit

**Examples:**
```bash
# List workflows from default config
dbt_wrapper config list

# List from specific config file
dbt_wrapper config list --config ./my-config.yml
```

#### `dbt_wrapper config validate`

Validate a configuration file

**Usage:**
```bash
dbt_wrapper config validate [OPTIONS] [PATH]
```

**Arguments:**
- `[PATH]` - Path to configuration file [default: None]

**Options:**
- `--help` - Show this message and exit

**Examples:**
```bash
# Validate default config
dbt_wrapper config validate

# Validate specific file
dbt_wrapper config validate ./my-config.yml
```

## Environment Management Commands

### `dbt_wrapper env`

**Usage:**
```bash
dbt_wrapper env [OPTIONS] COMMAND [ARGS]...
```

**Description:**
Manage environments and configurations

#### `dbt_wrapper env list`

List available environments from profiles.yml

**Usage:**
```bash
dbt_wrapper env list [OPTIONS]
```

**Options:**
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--help` - Show this message and exit

#### `dbt_wrapper env validate`

Validate environment configuration

**Usage:**
```bash
dbt_wrapper env validate [OPTIONS] ENV_NAME
```

**Arguments:**
- `ENV_NAME` - Environment name to validate [required]

**Options:**
- `--profiles-dir TEXT` - The path to the dbt_profiles directory [default: None]
- `--help` - Show this message and exit

**Examples:**
```bash
dbt_wrapper env validate development
dbt_wrapper env validate production
```

## Status and Monitoring Commands

### `dbt_wrapper status`

Show status of last pipeline run

**Usage:**
```bash
dbt_wrapper status [OPTIONS]
```

**Options:**
- `--project-dir TEXT` - The path to the dbt_project directory [default: .]
- `--watch` - Watch for status updates [default: False]
- `--help` - Show this message and exit

**Examples:**
```bash
# Show current status
dbt_wrapper status

# Watch for updates
dbt_wrapper status --watch
```

### `dbt_wrapper history`

Show pipeline run history

**Usage:**
```bash
dbt_wrapper history [OPTIONS]
```

**Options:**
- `--project-dir TEXT` - The path to the dbt_project directory [default: .]
- `--limit INTEGER` - Number of entries to show [default: 10]
- `--help` - Show this message and exit

**Examples:**
```bash
# Show last 10 runs
dbt_wrapper history

# Show last 20 runs
dbt_wrapper history --limit 20
```

## Legacy Commands

These commands are maintained for backwards compatibility.

### `dbt_wrapper docs`

Generate documentation for the dbt project

**Usage:**
```bash
dbt_wrapper docs [OPTIONS]
```

**Options:**
- `--help` - Show this message and exit

### `dbt_wrapper run-all`

**DEPRECATED** - Use workflow commands instead

This command will run all elements of the project. For more granular control you can use the options provided to suppress certain stages or use a different command.

**Usage:**
```bash
dbt_wrapper run-all [OPTIONS] DBT_PROJECT_DIR [DBT_PROFILES_DIR]
```

**Arguments:**
- `DBT_PROJECT_DIR` - The path to the dbt_project directory [required]
- `[DBT_PROFILES_DIR]` - The path to the dbt_profile directory [default: None]

**Options:**
- `--clean-target-dir / --no-clean-target-dir` - Clear out the target folder before dbt project build [default: clean-target-dir]
- `--generate-pre-dbt-scripts / --no-generate-pre-dbt-scripts` - Generate pre dbt scripts before dbt project build [default: generate-pre-dbt-scripts]
- `--generate-post-dbt-scripts / --no-generate-post-dbt-scripts` - Generate post dbt scripts before dbt project build [default: generate-post-dbt-scripts]
- `--auto-execute-metadata-extract / --no-auto-execute-metadata-extract` - Automatically refresh metadata before dbt project build [default: auto-execute-metadata-extract]
- `--download-metadata / --no-download-metadata` - Automatically download metadata before dbt project build [default: download-metadata]
- `--build-dbt-project / --no-build-dbt-project` - Suppress build the dbt project [default: build-dbt-project]
- `--pre-install / --no-pre-install` - Run the dbt adapter using source code and not the installed package [default: no-pre-install]
- `--upload-notebooks-via-api / --no-upload-notebooks-via-api` - Upload notebooks directly via the powerbi api [default: upload-notebooks-via-api]
- `--auto-run-master-notebook / --no-auto-run-master-notebook` - Automatically execute transformation pipeline by executing the master orchestration notebook [default: auto-run-master-notebook]
- `--log-level TEXT` - Set the log level (DEBUG, INFO, WARNING, ERROR) [default: WARNING]
- `--hashcheck-level TEXT` - Set the hash check level (BYPASS, WARNING, ERROR) [default: BYPASS]
- `--notebook-timeout INTEGER` - Change the default notebook execution timeout setting [default: 1800]
- `--select TEXT` - dbt resource selection syntax [default: ""]
- `--exclude TEXT` - dbt resource exclude syntax [default: ""]
- `--lakehouse-config TEXT` - Set the default lakehouse in code or metadata (CODE or METADATA) [default: METADATA]
- `--help` - Show this message and exit

**Migration Note:**
!!! warning "Deprecated"
    This command is deprecated. Use the new workflow commands instead:
    - `dbt_wrapper dev` for development
    - `dbt_wrapper deploy` for full deployment  
    - `dbt_wrapper build` for build-only
    - `dbt_wrapper test` for validation

### `dbt_wrapper buildcomparemetadata`

Execute 'build metadata' notebooks in two specified environments

**Usage:**
```bash
dbt_wrapper buildcomparemetadata [OPTIONS] DBT_PROJECT_DIR SOURCE TARGET [DBT_PROFILES_DIR]
```

**Arguments:**
- `DBT_PROJECT_DIR` - The path to the dbt_project directory [required]
- `SOURCE` - Source environment name from profile.yml [required]
- `TARGET` - Target environment name from profile.yml [required]
- `[DBT_PROFILES_DIR]` - The path to the dbt_profile directory [default: None]

**Options:**
- `--help` - Show this message and exit

### `dbt_wrapper compare`

Compare two environments' Lakehouses

**Usage:**
```bash
dbt_wrapper compare [OPTIONS] DBT_PROJECT_DIR SOURCE TARGET [DBT_PROFILES_DIR]
```

**Arguments:**
- `DBT_PROJECT_DIR` - The path to the dbt_project directory [required]
- `SOURCE` - Source environment name from profile.yml [required]
- `TARGET` - Target environment name from profile.yml [required]
- `[DBT_PROFILES_DIR]` - The path to the dbt_profile directory [default: None]

**Options:**
- `--help` - Show this message and exit

### `dbt_wrapper download-metadata`

Run just the metadata download

**Usage:**
```bash
dbt_wrapper download-metadata [OPTIONS] DBT_PROJECT_DIR [DBT_PROFILES_DIR]
```

**Arguments:**
- `DBT_PROJECT_DIR` - The path to the dbt_project directory [required]
- `[DBT_PROFILES_DIR]` - The path to the dbt_profile directory [default: None]

**Options:**
- `--log-level TEXT` - Set the log level (DEBUG, INFO, WARNING, ERROR) [default: WARNING]
- `--help` - Show this message and exit

### `dbt_wrapper get-execution-results`

Run just the extract of the last execution results

**Usage:**
```bash
dbt_wrapper get-execution-results [OPTIONS] DBT_PROJECT_DIR [DBT_PROFILES_DIR]
```

**Arguments:**
- `DBT_PROJECT_DIR` - The path to the dbt_project directory [required]
- `[DBT_PROFILES_DIR]` - The path to the dbt_profile directory [default: None]

**Options:**
- `--log-level TEXT` - Set the log level (DEBUG, INFO, WARNING, ERROR) [default: WARNING]
- `--help` - Show this message and exit

## Common Patterns and Examples

### Development Iteration
```bash
# Start fresh
dbt_wrapper dev my_project

# Quick iteration (skip clean)
dbt_wrapper dev my_project --skip clean

# Focus on specific models
dbt_wrapper dev my_project --select models/staging
```

### CI/CD Integration
```bash
# Validation workflow
dbt_wrapper test my_project

# Deployment workflow
dbt_wrapper deploy my_project

# Custom CI workflow
dbt_wrapper run --workflow ci
```

### Stage-by-Stage Debugging
```bash
# List all stages
dbt_wrapper stage list

# Understand a stage
dbt_wrapper stage describe build

# Run specific stages
dbt_wrapper stage run clean metadata-extract build
```

### Configuration Management
```bash
# Initialize config
dbt_wrapper config init

# Validate config
dbt_wrapper config validate

# List workflows
dbt_wrapper config list

# Run custom workflow
dbt_wrapper run --workflow my-custom-workflow
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Connection error |
| 4 | Validation error |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DBT_PROFILES_DIR` | Default profiles directory | `~/.dbt` |
| `DBT_PROJECT_DIR` | Default project directory | `.` |
| `DBT_FABRIC_WORKSPACE_ID` | Default workspace ID | (none) |
| `DBT_FABRIC_LAKEHOUSE_ID` | Default lakehouse ID | (none) |

## Getting More Help

- `dbt_wrapper --help` - Main help
- `dbt_wrapper <command> --help` - Command-specific help
- `dbt_wrapper stage --help` - Stage management help
- `dbt_wrapper config --help` - Configuration help
- Visit the [CLI Reference Guide](../user_guide/cli_reference.md) for usage examples
- Check the [Migration Guide](../user_guide/migration_guide.md) for migrating from old commands