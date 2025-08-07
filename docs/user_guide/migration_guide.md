---
title: Migration Guide
excerpt: Guide to migrating from old run-all commands to new workflow-based CLI
sidebar_label: Migration Guide
slug: /user_guide/migration_guide
weight: 5
---

# Migration Guide

This guide helps you migrate from the old `run-all` command to the new workflow-based CLI interface. The new interface is more intuitive and provides better control over your pipeline execution.

## Why Migrate?

The new workflow-based commands offer several advantages:

- ✅ **Intuitive** - Commands match your intent (`dev`, `deploy`, `build`, `test`)
- ✅ **Simpler** - No need to remember complex flag combinations
- ✅ **Flexible** - Easy stage control with `--skip` and `--only`
- ✅ **Configurable** - YAML-based workflow definitions
- ✅ **Interactive** - Guided workflow selection
- ✅ **Better Help** - Clear documentation for each command

## Command Migration Map

### Basic Usage

| Old Command | New Command | Notes |
|-------------|-------------|-------|
| `dbt_wrapper run-all my_project` | `dbt_wrapper deploy my_project` | Full deployment equivalent |
| `dbt_wrapper run-all --help` | `dbt_wrapper --help` | Main help |

### Common Flag Patterns

| Old Pattern | New Pattern | Explanation |
|-------------|-------------|-------------|
| `--no-upload-notebooks-via-api` | `--skip upload` | Skip upload stage |
| `--no-auto-run-master-notebook` | `--skip execute` | Skip execution stage |
| `--no-clean-target-dir` | `--skip clean` | Skip cleaning stage |
| `--no-generate-pre-dbt-scripts` | `--skip pre-scripts` | Skip pre-script generation |
| `--no-generate-post-dbt-scripts` | `--skip post-scripts` | Skip post-script generation |
| `--no-download-metadata` | `--skip metadata-download` | Skip metadata download |

### Development Workflows

| Old Workflow | New Workflow | Benefits |
|--------------|--------------|----------|
| Local development | `dbt_wrapper dev` | Purpose-built for development |
| Quick build only | `dbt_wrapper build` | Minimal overhead |
| Testing/validation | `dbt_wrapper test` | Validation-focused |
| Full deployment | `dbt_wrapper deploy` | Complete pipeline |

## Step-by-Step Migration

### Step 1: Identify Your Current Usage

First, identify how you currently use `run-all`:

```bash
# What you currently run
dbt_wrapper run-all my_project --no-upload-notebooks-via-api --no-auto-run-master-notebook
```

### Step 2: Map to New Commands

Find the equivalent new command:

```bash
# New equivalent
dbt_wrapper dev my_project  # or
dbt_wrapper deploy my_project --skip upload,execute
```

### Step 3: Test the New Command

Run the new command to ensure it works as expected:

```bash
# Test the new workflow
dbt_wrapper dev my_project --help  # See available options
dbt_wrapper dev my_project         # Run it
```

### Step 4: Update Scripts and Documentation

Update any scripts, CI/CD pipelines, and documentation to use the new commands.

## Common Migration Examples

### Example 1: Development Workflow

**Before:**
```bash
dbt_wrapper run-all my_project \
  --no-upload-notebooks-via-api \
  --no-auto-run-master-notebook \
  --log-level INFO
```

**After:**
```bash
dbt_wrapper dev my_project  # Built-in defaults for development
```

### Example 2: CI/CD Pipeline

**Before:**
```bash
dbt_wrapper run-all my_project \
  --no-upload-notebooks-via-api \
  --no-auto-run-master-notebook \
  --log-level ERROR \
  --notebook-timeout 900
```

**After:**
```bash
# Option 1: Direct command
dbt_wrapper test my_project

# Option 2: Configuration-based
dbt_wrapper run --workflow ci
```

With `.dbt-wrapper.yml`:
```yaml
workflows:
  ci:
    description: CI/CD pipeline
    stages: [clean, metadata-extract, build, validate]
    options:
      log_level: ERROR
      notebook_timeout: 900
```

### Example 3: Production Deployment

**Before:**
```bash
dbt_wrapper run-all my_project \
  --log-level WARNING \
  --notebook-timeout 3600 \
  --hashcheck-level ERROR
```

**After:**
```bash
dbt_wrapper deploy my_project --notebook-timeout 3600
```

### Example 4: Selective Stage Execution

**Before:**
```bash
dbt_wrapper run-all my_project \
  --no-clean-target-dir \
  --no-generate-pre-dbt-scripts \
  --no-upload-notebooks-via-api \
  --no-auto-run-master-notebook
```

**After:**
```bash
# Option 1: Skip specific stages
dbt_wrapper deploy my_project --skip clean,pre-scripts,upload,execute

# Option 2: Run only specific stages
dbt_wrapper stage run metadata-extract build post-scripts

# Option 3: Use build workflow
dbt_wrapper build my_project  # Just metadata-download + build
```

### Example 5: Custom Timeout and Config

**Before:**
```bash
dbt_wrapper run-all my_project \
  --notebook-timeout 2100 \
  --lakehouse-config CODE \
  --select tag:daily
```

**After:**
```bash
dbt_wrapper deploy my_project \
  --notebook-timeout 2100 \
  --lakehouse-config CODE \
  --select tag:daily
```

## Advanced Migration Patterns

### Complex Flag Combinations

**Before:**
```bash
dbt_wrapper run-all my_project \
  --no-clean-target-dir \
  --no-generate-pre-dbt-scripts \
  --no-auto-execute-metadata-extract \
  --no-upload-notebooks-via-api \
  --no-auto-run-master-notebook \
  --log-level DEBUG \
  --select models/staging
```

**After (Option 1 - Direct):**
```bash
dbt_wrapper build my_project \
  --log-level DEBUG \
  --select models/staging
```

**After (Option 2 - Configuration):**
Create `.dbt-wrapper.yml`:
```yaml
workflows:
  staging-only:
    description: Build staging models only
    stages: [metadata-download, build]
    options:
      log_level: DEBUG
```

Then run:
```bash
dbt_wrapper run --workflow staging-only --select models/staging
```

### Development Environment Variations

**Before:**
```bash
# Development
python test_pre_install.py run-all my_project --pre-install

# Different variations for different needs
dbt_wrapper run-all my_project --no-upload-notebooks-via-api
dbt_wrapper run-all my_project --no-clean-target-dir --no-upload-notebooks-via-api
```

**After:**
```bash
# Development environment (non-pip installed)
python -m dbt_wrapper.main dev my_project

# Different workflows for different needs
dbt_wrapper dev my_project          # Full development workflow
dbt_wrapper dev my_project --skip clean  # Skip cleaning for speed
dbt_wrapper build my_project        # Minimal build only
```

## Migration Strategies

### Strategy 1: Gradual Migration

1. Start using new commands alongside old ones
2. Update one workflow at a time
3. Keep old commands until team is comfortable
4. Migrate CI/CD pipelines last

### Strategy 2: Configuration-First Migration

1. Create `.dbt-wrapper.yml` with current workflows
2. Test workflows using `dbt_wrapper run --workflow`
3. Replace direct commands with configuration-based ones
4. Gradually simplify to built-in workflows

### Strategy 3: Team Migration

1. Document team's current `run-all` usage patterns
2. Create equivalent workflows in shared config
3. Train team on new commands
4. Update shared scripts and CI/CD

## Troubleshooting Migration

### Common Issues and Solutions

#### Issue: "Command not found"
```bash
# Old
dbt_wrapper run-all --no-upload-notebooks-via-api

# Error: No such option: --no-upload-notebooks-via-api
```

**Solution:**
```bash
# New equivalent
dbt_wrapper dev my_project
# or
dbt_wrapper deploy my_project --skip upload
```

#### Issue: "Different behavior than expected"
**Solution:** Compare stage execution:
```bash
# See what stages run in each workflow
dbt_wrapper stage list

# Compare with your old command
dbt_wrapper deploy --help  # See what stages it runs
```

#### Issue: "Missing configuration"
**Solution:** Create configuration file:
```bash
# Create default config
dbt_wrapper config init

# Validate your setup
dbt_wrapper config validate
```

#### Issue: "Development environment differences"
**Solution:** Use correct development command:
```bash
# Development environment (non-pip installed)
python -m dbt_wrapper.main dev my_project

# Not the old:
python test_pre_install.py run-all my_project --pre-install
```

### Validation Steps

After migration, validate that your new commands produce the same results:

1. **Compare outputs:** Run old and new commands and compare generated files
2. **Check stages:** Ensure the same stages are executed
3. **Verify notebooks:** Confirm generated notebooks are equivalent
4. **Test execution:** Validate Fabric execution produces same results

### Getting Help During Migration

- `dbt_wrapper --help` - Overview of all commands
- `dbt_wrapper <command> --help` - Specific command help
- `dbt_wrapper stage list` - See all available stages
- `dbt_wrapper stage describe <stage>` - Understand what each stage does
- `dbt_wrapper config init` - Create example configuration

## Migration Checklist

### Pre-Migration
- [ ] Document current `run-all` usage patterns
- [ ] Identify different workflows your team uses
- [ ] Test new commands in development environment
- [ ] Create `.dbt-wrapper.yml` if using configuration approach

### During Migration
- [ ] Update local development workflows
- [ ] Migrate team scripts and documentation
- [ ] Update CI/CD pipelines
- [ ] Train team members on new commands

### Post-Migration
- [ ] Remove old `run-all` commands from scripts
- [ ] Update documentation
- [ ] Validate that all workflows produce expected results
- [ ] Collect team feedback and refine workflows

## Benefits After Migration

Once migrated, you'll enjoy:

1. **Clearer Intent** - Commands clearly express what you want to do
2. **Easier Onboarding** - New team members understand commands intuitively  
3. **Better Control** - Fine-grained control over pipeline stages
4. **Configuration as Code** - Version-controlled workflow definitions
5. **Interactive Help** - Better help system and command discovery
6. **Future-Proof** - New features will be added to the new interface

## Legacy Support

The old `run-all` command will continue to work for backwards compatibility:

```bash
dbt_wrapper run-all my_project  # Still works, but deprecated
```

However, we recommend migrating to the new interface to take advantage of improved usability and new features.

## Need Help?

If you encounter issues during migration:

1. Check this migration guide for common patterns
2. Use `dbt_wrapper <command> --help` for command-specific help
3. Create an issue in the project repository
4. Consult the [CLI Reference](cli_reference.md) for complete command documentation