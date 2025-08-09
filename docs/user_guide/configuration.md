---
title: Configuration Guide
excerpt: Complete guide to configuring dbt_wrapper workflows and environments
sidebar_label: Configuration
slug: /user_guide/configuration
weight: 4
---

# Configuration Guide

The `dbt_wrapper` CLI supports powerful configuration capabilities through YAML files, allowing you to define custom workflows, manage environments, and standardize your team's development practices.

## Configuration Files Overview

### Two Configuration Systems

1. **profiles.yml** (dbt standard) - Handles **connection details**
   - Workspace IDs, Lakehouse IDs
   - Authentication settings
   - Connection parameters

2. **.dbt-wrapper.yml** (our extension) - Handles **workflow behavior**
   - Pipeline stages and order
   - Execution options and timeouts
   - Environment-specific settings

These work together: profiles.yml provides the "where" (which Fabric workspace/lakehouse), and .dbt-wrapper.yml provides the "how" (which stages to run and how to run them).

## Creating Configuration Files

### Initialize a Configuration File

Create a starter configuration file:

```bash
dbt_wrapper config init
```

This creates `.dbt-wrapper.yml` in your current directory with example workflows.

### Custom Location

```bash
dbt_wrapper config init --path ./config/my-workflows.yml
```

## Configuration File Structure

### Basic Structure

```yaml
# .dbt-wrapper.yml
version: '1.0'

# Default options applied to all workflows
defaults:
  log_level: WARNING
  notebook_timeout: 1800
  lakehouse_config: METADATA

# Environment-specific configurations
environments:
  development:
    workspace_id: ${DBT_FABRIC_WORKSPACE_ID_DEV}
    lakehouse_id: ${DBT_FABRIC_LAKEHOUSE_ID_DEV}
  
  production:
    workspace_id: ${DBT_FABRIC_WORKSPACE_ID_PROD}
    lakehouse_id: ${DBT_FABRIC_LAKEHOUSE_ID_PROD}

# Workflow definitions
workflows:
  my-dev:
    description: My custom development workflow
    stages:
      - clean
      - metadata-extract
      - build
    options:
      log_level: INFO
      upload_notebooks: false
```

## Workflow Configuration

### Available Stages

| Stage | Description | When to Use |
|-------|-------------|-------------|
| `clean` | Clean target directory | Start fresh, avoid cached issues |
| `pre-scripts` | Generate pre-dbt scripts | Need metadata extraction setup |
| `metadata-extract` | Extract metadata from Fabric | Schema discovery, fresh metadata |
| `metadata-download` | Download metadata locally | Need current schema info for dbt |
| `build` | Build dbt project | Core transformation - always needed |
| `post-scripts` | Generate post-dbt scripts | Prepare for Fabric execution |
| `upload` | Upload notebooks to Fabric | Deploy to Fabric workspace |
| `execute` | Execute master notebook | Run transformations in Fabric |
| `validate` | Run validation checks | Quality assurance |
| `results` | Retrieve execution results | Get execution logs and metrics |

### Example Workflows

#### Development Workflow
```yaml
workflows:
  dev:
    description: Fast development iteration
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - build
      - post-scripts
    options:
      log_level: INFO
      hashcheck_level: WARNING
      upload_notebooks: false
      auto_run_master: false
```

#### CI/CD Workflow
```yaml
workflows:
  ci:
    description: Continuous integration pipeline
    stages:
      - clean
      - metadata-extract
      - build
      - validate
    options:
      log_level: INFO
      hashcheck_level: ERROR
      notebook_timeout: 900
      upload_notebooks: false
      auto_run_master: false
```

#### Production Deployment
```yaml
workflows:
  production:
    description: Full production deployment
    environment: production
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - metadata-download
      - build
      - post-scripts
      - upload
      - execute
      - results
    options:
      log_level: ERROR
      hashcheck_level: ERROR
      notebook_timeout: 3600
      upload_notebooks: true
      auto_run_master: true
```

#### Quick Test
```yaml
workflows:
  quick-test:
    description: Quick build without full setup
    stages:
      - metadata-download
      - build
    options:
      log_level: DEBUG
      hashcheck_level: BYPASS
```

## Configuration Options

### Default Options
Applied to all workflows unless overridden:

```yaml
defaults:
  log_level: WARNING           # DEBUG, INFO, WARNING, ERROR
  hashcheck_level: BYPASS      # BYPASS, WARNING, ERROR
  notebook_timeout: 1800       # Seconds (max: 7 days)
  lakehouse_config: METADATA   # METADATA or CODE
```

### Workflow-Specific Options

| Option | Values | Description |
|--------|--------|-------------|
| `log_level` | DEBUG, INFO, WARNING, ERROR | Controls logging verbosity |
| `hashcheck_level` | BYPASS, WARNING, ERROR | Controls hash validation |
| `notebook_timeout` | Integer (seconds) | Notebook execution timeout |
| `lakehouse_config` | METADATA, CODE | How lakehouse is set in notebooks |
| `upload_notebooks` | true/false | Whether to upload to Fabric |
| `auto_run_master` | true/false | Whether to execute in Fabric |

### Lakehouse Configuration Modes

#### METADATA Mode (Default)
Lakehouse is set in notebook metadata only:
```yaml
options:
  lakehouse_config: METADATA
```

#### CODE Mode
Adds `%%configure` cell to notebooks:
```yaml
options:
  lakehouse_config: CODE
```

## Environment Configuration

### Basic Environment Setup
```yaml
environments:
  development:
    workspace_id: "dev-workspace-guid"
    lakehouse_id: "dev-lakehouse-guid"
    extra_validation: false
    
  staging:
    workspace_id: "staging-workspace-guid"
    lakehouse_id: "staging-lakehouse-guid"
    extra_validation: true
    
  production:
    workspace_id: "prod-workspace-guid"
    lakehouse_id: "prod-lakehouse-guid"
    extra_validation: true
    alert_on_failure: true
```

### Using Environment Variables
```yaml
environments:
  development:
    workspace_id: ${DBT_FABRIC_WORKSPACE_ID_DEV}
    lakehouse_id: ${DBT_FABRIC_LAKEHOUSE_ID_DEV}
  
  production:
    workspace_id: ${DBT_FABRIC_WORKSPACE_ID_PROD}
    lakehouse_id: ${DBT_FABRIC_LAKEHOUSE_ID_PROD}
```

### Environment-Specific Workflows
```yaml
workflows:
  deploy-prod:
    description: Production deployment
    environment: production  # References environments section
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - build
      - post-scripts
      - upload
      - execute
    options:
      log_level: ERROR
      hashcheck_level: ERROR
```

## Integration with profiles.yml

### profiles.yml Structure
Your dbt profiles.yml handles connection details:

```yaml
# ~/.dbt/profiles.yml
my-fabric-project:
  outputs:
    dev:
      type: fabricsparknb
      workspaceid: 'dev-workspace-guid'
      lakehouseid: 'dev-lakehouse-guid'
      lakehouse: datalake_dev
      # ... other connection settings
    
    production:
      type: fabricsparknb
      workspaceid: 'prod-workspace-guid'
      lakehouseid: 'prod-lakehouse-guid'
      lakehouse: datalake_prod
      # ... other connection settings
  
  target: dev  # Default environment
```

### How They Work Together

1. **profiles.yml** provides connection details for each environment
2. **.dbt-wrapper.yml** provides additional workflow settings
3. When you specify `environment: production` in a workflow, it:
   - Uses the `production` target from profiles.yml for connections
   - Applies any additional settings from the `production` environment in .dbt-wrapper.yml

## Using Configuration Files

### List Available Workflows
```bash
dbt_wrapper config list
```

### Run Configured Workflow
```bash
# Run default workflow
dbt_wrapper run --workflow dev

# Use custom config file
dbt_wrapper run --workflow ci --config ./my-config.yml

# Specify project directory
dbt_wrapper run --workflow production --project-dir ./my-project
```

### Validate Configuration
```bash
# Validate default config
dbt_wrapper config validate

# Validate specific file
dbt_wrapper config validate ./my-config.yml
```

## Advanced Configuration Patterns

### Multi-Environment Deployment
```yaml
workflows:
  deploy-dev:
    environment: development
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute]
    options:
      log_level: INFO
      
  deploy-staging:
    environment: staging
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute]
    options:
      log_level: WARNING
      
  deploy-prod:
    environment: production
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute]
    options:
      log_level: ERROR
      notebook_timeout: 3600
```

### Specialized Workflows
```yaml
workflows:
  # Metadata refresh only
  metadata-sync:
    description: Refresh metadata from Fabric
    stages:
      - metadata-extract
      - metadata-download
    options:
      log_level: INFO
      
  # Upload and execute only (after local build)
  deploy-only:
    description: Deploy pre-built notebooks
    stages:
      - upload
      - execute
      - results
    options:
      upload_notebooks: true
      auto_run_master: true
      
  # Build with specific models
  build-staging:
    description: Build staging models only
    stages:
      - metadata-download
      - build
    options:
      log_level: DEBUG
```

### Team Configuration Example
```yaml
version: '1.0'

defaults:
  log_level: WARNING
  notebook_timeout: 1800
  lakehouse_config: METADATA

environments:
  development:
    workspace_id: ${TEAM_DEV_WORKSPACE}
    lakehouse_id: ${TEAM_DEV_LAKEHOUSE}
    
  shared-staging:
    workspace_id: ${TEAM_STAGING_WORKSPACE}
    lakehouse_id: ${TEAM_STAGING_LAKEHOUSE}
    
  production:
    workspace_id: ${TEAM_PROD_WORKSPACE}
    lakehouse_id: ${TEAM_PROD_LAKEHOUSE}

workflows:
  # Individual developer workflow
  dev:
    description: Individual developer workflow
    environment: development
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts]
    options:
      log_level: INFO
      upload_notebooks: false
      
  # Team integration testing
  integration:
    description: Team integration testing
    environment: shared-staging
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute]
    options:
      log_level: INFO
      hashcheck_level: WARNING
      
  # Production deployment
  release:
    description: Production release
    environment: production
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute, results]
    options:
      log_level: ERROR
      hashcheck_level: ERROR
      notebook_timeout: 7200
```

## Best Practices

### 1. Version Control Configuration
- Commit `.dbt-wrapper.yml` to your repository
- Use environment variables for sensitive values
- Document custom workflows in your README

### 2. Environment Management
- Use clear, consistent environment naming
- Separate development, staging, and production environments
- Use environment variables for workspace/lakehouse IDs

### 3. Workflow Design
- Create workflows for specific use cases
- Use descriptive names and descriptions
- Start with simple workflows and add complexity as needed

### 4. Team Coordination
- Standardize workflows across your team
- Use shared configuration files
- Document any custom workflows or options

### 5. Security
- Never commit workspace IDs or sensitive data directly
- Use environment variables or external secret management
- Validate configurations before deployment

## Troubleshooting

### Common Issues

#### Configuration Not Found
```bash
# Check if config file exists
ls -la .dbt-wrapper.yml

# Create default config
dbt_wrapper config init
```

#### Invalid Workflow
```bash
# List available workflows
dbt_wrapper config list

# Validate configuration
dbt_wrapper config validate
```

#### Environment Variables Not Set
```bash
# Check environment variables
echo $DBT_FABRIC_WORKSPACE_ID_DEV

# Set environment variables (example)
export DBT_FABRIC_WORKSPACE_ID_DEV="your-dev-workspace-id"
```

### Validation Errors
Common validation errors and fixes:

| Error | Fix |
|-------|-----|
| "Invalid stage name" | Check stage names against available stages |
| "Workflow has no stages" | Add at least one stage to the workflow |
| "Invalid log_level" | Use DEBUG, INFO, WARNING, or ERROR |
| "Undefined environment" | Add environment to environments section |

### Getting Help
- `dbt_wrapper config --help` - Configuration command help
- `dbt_wrapper config validate` - Validate your configuration
- `dbt_wrapper stage list` - See available stages
- Review this guide for configuration patterns