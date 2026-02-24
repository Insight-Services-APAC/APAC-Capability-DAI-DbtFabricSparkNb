---
title: CLI Reference
excerpt: Complete reference for the dbt_wrapper command-line interface
sidebar_label: CLI Reference
slug: /user_guide/cli_reference
weight: 2
---

# CLI Reference

The `dbt_wrapper` command-line interface provides intuitive, workflow-based commands for building and deploying your dbt projects to Microsoft Fabric.

## Quick Reference

### Workflow Commands
| Command | Purpose | Use Case |
|---------|---------|----------|
| `dbt_wrapper dev` | Development workflow | Local development and testing |
| `dbt_wrapper deploy` | Full deployment | Complete Fabric deployment |
| `dbt_wrapper build` | Build only | Minimal build without deployment |
| `dbt_wrapper test` | Test workflow | Validation without deployment |
| `dbt_wrapper run` | Custom workflow | Run configured workflows |

### Management Commands
| Command | Purpose |
|---------|---------|
| `dbt_wrapper stage` | Manage pipeline stages |
| `dbt_wrapper config` | Manage configuration files |
| `dbt_wrapper env` | Manage environments |
| `dbt_wrapper status` | View pipeline status |

## Workflow Commands

### `dbt_wrapper dev`
🚀 **Development workflow** - Perfect for rapid development and testing cycles.

**Usage:**
```bash
dbt_wrapper dev [PROJECT_DIR] [OPTIONS]
```

**What it runs:**
```
clean → pre-scripts → metadata-extract → build → post-scripts
```

**Options:**
- `--profiles-dir PATH` - Path to dbt profiles directory
- `--target, -t TEXT` - The dbt target to use (overrides DBT_TARGET env var and profiles.yml default)
- `--skip STAGES` - Comma-separated list of stages to skip
- `--only STAGES` - Run only these stages
- `--select SELECTOR` - dbt resource selection syntax
- `--exclude SELECTOR` - dbt resource exclude syntax

**Examples:**
```bash
# Basic development workflow
dbt_wrapper dev my_project

# Skip cleaning for faster iteration
dbt_wrapper dev my_project --skip clean

# Run only build and post-scripts
dbt_wrapper dev my_project --only build,post-scripts

# Select specific models
dbt_wrapper dev my_project --select tag:daily
```

### `dbt_wrapper deploy`
🚢 **Deploy workflow** - Complete deployment pipeline with Fabric upload and execution.

**Usage:**
```bash
dbt_wrapper deploy [PROJECT_DIR] [OPTIONS]
```

**What it runs:**
```
clean → pre-scripts → metadata-extract → metadata-download → build → post-scripts → upload → execute → results
```

**Options:**
- `--profiles-dir PATH` - Path to dbt profiles directory
- `--target, -t TEXT` - The dbt target to use (overrides DBT_TARGET env var and profiles.yml default)
- `--skip STAGES` - Comma-separated list of stages to skip
- `--only STAGES` - Run only these stages
- `--select SELECTOR` - dbt resource selection syntax
- `--exclude SELECTOR` - dbt resource exclude syntax
- `--retry-batch BATCH_ID` - Retry failed notebooks from a specific batch_id (mutually exclusive with --select)

**Examples:**
```bash
# Full deployment
dbt_wrapper deploy my_project

# Deploy without execution
dbt_wrapper deploy my_project --skip execute,results

# Deploy with extended timeout
dbt_wrapper deploy my_project --notebook-timeout 3600

# Retry failed notebooks from a previous batch
dbt_wrapper deploy my_project --retry-batch abc123-def456-789
```

### `dbt_wrapper build`
🔨 **Build workflow** - Minimal build with just the essentials.

**Usage:**
```bash
dbt_wrapper build [PROJECT_DIR] [OPTIONS]
```

**What it runs:**
```
metadata-download → build
```

**Options:**
- `--profiles-dir PATH` - Path to dbt profiles directory
- `--target, -t TEXT` - The dbt target to use (overrides DBT_TARGET env var and profiles.yml default)
- `--select SELECTOR` - dbt resource selection syntax
- `--exclude SELECTOR` - dbt resource exclude syntax

**Examples:**
```bash
# Quick build
dbt_wrapper build my_project

# Build specific models
dbt_wrapper build my_project --select models/staging
```

### `dbt_wrapper test`
🧪 **Test workflow** - Validation-focused pipeline without deployment.

**Usage:**
```bash
dbt_wrapper test [PROJECT_DIR] [OPTIONS]
```

**What it runs:**
```
clean → metadata-extract → metadata-download → build → validation
```

**Options:**
- `--profiles-dir PATH` - Path to dbt profiles directory
- `--target, -t TEXT` - The dbt target to use (overrides DBT_TARGET env var and profiles.yml default)
- `--skip STAGES` - Comma-separated list of stages to skip
- `--select SELECTOR` - dbt resource selection syntax
- `--exclude SELECTOR` - dbt resource exclude syntax

**Examples:**
```bash
# Run tests
dbt_wrapper test my_project

# Test without cleaning
dbt_wrapper test my_project --skip clean
```

### `dbt_wrapper run`
🎯 **Custom workflow** - Run workflows from configuration files or interactively.

**Usage:**
```bash
dbt_wrapper run [OPTIONS]
```

**Options:**
- `--workflow NAME` - Workflow name from configuration file
- `--config PATH` - Path to configuration file
- `--interactive` - Run in interactive mode
- `--project-dir PATH` - dbt project directory
- `--profiles-dir PATH` - dbt profiles directory
- `--target, -t TEXT` - The dbt target to use (overrides DBT_TARGET env var and profiles.yml default)

**Examples:**
```bash
# Run configured workflow
dbt_wrapper run --workflow ci

# Interactive mode
dbt_wrapper run --interactive

# Use custom config file
dbt_wrapper run --workflow production --config ./my-config.yml
```

## Stage Management Commands

### `dbt_wrapper stage list`
List all available pipeline stages.

**Usage:**
```bash
dbt_wrapper stage list
```

**Output:**
```
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

### `dbt_wrapper stage describe`
Get detailed information about a specific stage.

**Usage:**
```bash
dbt_wrapper stage describe <STAGE_NAME>
```

**Example:**
```bash
dbt_wrapper stage describe build
```

**Output:**
```
Build DBT Project
Runs dbt build command to compile and test models

Purpose: Core transformation logic execution
When to skip: Never - this is the core functionality
```

### `dbt_wrapper stage run`
Run specific pipeline stages.

**Usage:**
```bash
dbt_wrapper stage run <STAGE1> <STAGE2> ... [OPTIONS]
```

**Options:**
- `--project-dir PATH` - dbt project directory
- `--profiles-dir PATH` - dbt profiles directory

**Examples:**
```bash
# Run specific stages
dbt_wrapper stage run clean metadata-extract build

# Run single stage
dbt_wrapper stage run build
```

## Configuration Management Commands

### `dbt_wrapper config init`
Create an example configuration file.

**Usage:**
```bash
dbt_wrapper config init [--path PATH]
```

**Examples:**
```bash
# Create in current directory
dbt_wrapper config init

# Create in specific location
dbt_wrapper config init --path ./config/.dbt-wrapper.yml
```

### `dbt_wrapper config list`
List workflows in configuration file.

**Usage:**
```bash
dbt_wrapper config list [--config PATH]
```

**Output:**
```
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Workflow         ┃ Description                 ┃ Stages                      ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ dev              │ Quick iteration workflow    │ clean → pre-scripts →       │
│                  │ for local development       │ metadata-extract ... (+2)   │
│ deploy           │ Complete deployment         │ clean → pre-scripts →       │
│                  │ pipeline with upload and    │ metadata-extract ... (+6)   │
│                  │ execution                   │                             │
└──────────────────┴─────────────────────────────┴─────────────────────────────┘
```

### `dbt_wrapper config validate`
Validate a configuration file.

**Usage:**
```bash
dbt_wrapper config validate [PATH]
```

**Examples:**
```bash
# Validate default config
dbt_wrapper config validate

# Validate specific file
dbt_wrapper config validate ./my-config.yml
```

## Common Options

### Global Options
Available for most workflow commands:

| Option | Description | Default |
|--------|-------------|---------|
| `--profiles-dir PATH` | dbt profiles directory | `~/.dbt/` |
| `--target, -t TEXT` | dbt target to use | profiles.yml default |
| `--select SELECTOR` | dbt resource selection | (none) |
| `--exclude SELECTOR` | dbt resource exclusion | (none) |
| `--notebook-timeout SECONDS` | Notebook execution timeout | 1800 |
| `--lakehouse-config MODE` | Lakehouse config mode (`CODE` or `METADATA`) | `METADATA` |
| `--skip STAGES` | Skip comma-separated stages | (none) |
| `--only STAGES` | Run only comma-separated stages | (none) |

### Selection Syntax
Use standard dbt selection syntax:

```bash
# Tag-based selection
--select tag:daily
--exclude tag:hourly

# Model-based selection
--select models/staging
--select models/marts/finance

# Combined selection
--select models/staging --exclude tag:external
```

## Environment Commands

### `dbt_wrapper env list`
List available environments from profiles.yml.

**Usage:**
```bash
dbt_wrapper env list [--profiles-dir PATH]
```

### `dbt_wrapper env validate`
Validate environment configuration.

**Usage:**
```bash
dbt_wrapper env validate <ENV_NAME> [--profiles-dir PATH]
```

## Status Commands

### `dbt_wrapper status`
Show status of last pipeline run.

**Usage:**
```bash
dbt_wrapper status [--project-dir PATH] [--watch]
```

### `dbt_wrapper history`
Show pipeline run history.

**Usage:**
```bash
dbt_wrapper history [--project-dir PATH] [--limit N]
```

## Legacy Commands

For backwards compatibility, the old `run-all` command is still supported:

```bash
dbt_wrapper run-all my_project  # Still works, but deprecated
```

!!! warning "Migration Recommended"
    We recommend migrating to the new workflow commands. They are more intuitive and provide better control over the pipeline execution.

## Getting Help

- `dbt_wrapper --help` - Show main help
- `dbt_wrapper <command> --help` - Show command-specific help
- `dbt_wrapper stage --help` - Show stage command help
- `dbt_wrapper config --help` - Show config command help

## Examples by Use Case

### Daily Development
```bash
# Start development session
dbt_wrapper dev my_project

# Quick iteration (skip cleaning)
dbt_wrapper dev my_project --skip clean

# Focus on specific models
dbt_wrapper dev my_project --select models/staging
```

### CI/CD Pipeline
```bash
# Run CI workflow
dbt_wrapper run --workflow ci

# Or use test workflow
dbt_wrapper test my_project
```

### Production Deployment
```bash
# Full deployment
dbt_wrapper deploy my_project

# Use configured production workflow
dbt_wrapper run --workflow production
```

### Troubleshooting
```bash
# Run specific stages for debugging
dbt_wrapper stage run metadata-extract build

# Check configuration
dbt_wrapper config validate

# View stage details
dbt_wrapper stage describe upload
```