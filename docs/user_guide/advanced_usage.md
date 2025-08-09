---
title: Advanced Usage
excerpt: Advanced patterns and techniques for dbt_wrapper power users
sidebar_label: Advanced Usage
slug: /user_guide/advanced_usage
weight: 6
---

# Advanced Usage

This guide covers advanced patterns and techniques for power users of the dbt_wrapper CLI.

## Advanced Workflow Patterns

### Multi-Environment Deployment Strategy

Create workflows for different deployment stages:

```yaml
# .dbt-wrapper.yml
workflows:
  # Development - fast iteration
  dev:
    description: Local development workflow
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts]
    options:
      log_level: INFO
      upload_notebooks: false
      
  # Integration testing
  integration:
    description: Integration testing in shared environment
    environment: staging
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute]
    options:
      log_level: WARNING
      hashcheck_level: WARNING
      
  # Production deployment
  production:
    description: Production deployment with full validation
    environment: production
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute, results]
    options:
      log_level: ERROR
      hashcheck_level: ERROR
      notebook_timeout: 7200
```

Usage:
```bash
# Development
dbt_wrapper run --workflow dev

# Deploy to staging
dbt_wrapper run --workflow integration

# Deploy to production
dbt_wrapper run --workflow production
```

### Incremental Development Patterns

#### Hot Reloading Pattern
For rapid development with minimal overhead:

```bash
# Initial setup (run once)
dbt_wrapper stage run clean pre-scripts metadata-extract

# Hot reload loop (run repeatedly)
dbt_wrapper stage run build post-scripts
```

#### Model-Specific Development
Work on specific model groups:

```bash
# Staging models only
dbt_wrapper dev --select models/staging

# Specific tag
dbt_wrapper dev --select tag:daily

# Downstream from specific model
dbt_wrapper dev --select +my_model+
```

#### Metadata Management
Separate metadata updates from builds:

```yaml
workflows:
  metadata-sync:
    description: Sync metadata from Fabric
    stages: [metadata-extract, metadata-download]
    options:
      log_level: INFO
      
  build-only:
    description: Build with existing metadata
    stages: [build]
    options:
      log_level: DEBUG
```

## Advanced Stage Management

### Custom Stage Orchestration

Run stages in custom sequences for specific scenarios:

```bash
# Debug metadata issues
dbt_wrapper stage run clean metadata-extract
dbt_wrapper stage describe metadata-extract
dbt_wrapper stage run metadata-download

# Build without metadata refresh
dbt_wrapper stage run build post-scripts

# Upload and execute pre-built notebooks
dbt_wrapper stage run upload execute results
```

### Conditional Stage Execution

Use configuration to create conditional workflows:

```yaml
workflows:
  # Full workflow with optional stages
  flexible-deploy:
    description: Flexible deployment with optional validation
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute]
    options:
      log_level: INFO
      
  # Minimal workflow
  quick-build:
    description: Quick build for testing
    stages: [build]
    options:
      log_level: DEBUG
      
  # Validation-heavy workflow
  quality-check:
    description: Quality-focused workflow
    stages: [clean, metadata-extract, build, validate, post-scripts]
    options:
      log_level: INFO
      hashcheck_level: ERROR
```

## Advanced Configuration Patterns

### Environment-Specific Configurations

```yaml
# .dbt-wrapper.yml
version: '1.0'

environments:
  development:
    workspace_id: ${DEV_WORKSPACE_ID}
    lakehouse_id: ${DEV_LAKEHOUSE_ID}
    debug_mode: true
    fast_mode: true
    
  staging:
    workspace_id: ${STAGING_WORKSPACE_ID}
    lakehouse_id: ${STAGING_LAKEHOUSE_ID}
    validation_level: medium
    notification_enabled: true
    
  production:
    workspace_id: ${PROD_WORKSPACE_ID}
    lakehouse_id: ${PROD_LAKEHOUSE_ID}
    validation_level: strict
    monitoring_enabled: true
    backup_enabled: true

defaults:
  log_level: WARNING
  notebook_timeout: 1800

workflows:
  deploy-dev:
    environment: development
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts]
    options:
      log_level: INFO
      
  deploy-staging:
    environment: staging
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute]
    options:
      log_level: WARNING
      notebook_timeout: 3600
      
  deploy-prod:
    environment: production
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute, results]
    options:
      log_level: ERROR
      hashcheck_level: ERROR
      notebook_timeout: 7200
```

### Dynamic Configuration with Environment Variables

```yaml
# .dbt-wrapper.yml
workflows:
  dynamic-deploy:
    description: Dynamic deployment based on environment
    environment: ${DEPLOY_TARGET:-development}
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - build
      - post-scripts
      - ${UPLOAD_STAGE:-upload}
      - ${EXECUTE_STAGE:-execute}
    options:
      log_level: ${LOG_LEVEL:-INFO}
      notebook_timeout: ${TIMEOUT:-1800}
      hashcheck_level: ${HASHCHECK:-WARNING}
```

Usage:
```bash
# Development deployment
export DEPLOY_TARGET=development
export LOG_LEVEL=DEBUG
dbt_wrapper run --workflow dynamic-deploy

# Production deployment
export DEPLOY_TARGET=production
export LOG_LEVEL=ERROR
export TIMEOUT=3600
dbt_wrapper run --workflow dynamic-deploy
```

## CI/CD Integration Patterns

### GitHub Actions Integration

```yaml
# .github/workflows/dbt.yml
name: dbt CI/CD

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          
      - name: Install dependencies
        run: |
          pip install -e .
          
      - name: Run tests
        run: |
          dbt_wrapper test my_project
        env:
          DBT_FABRIC_WORKSPACE_ID: ${{ secrets.TEST_WORKSPACE_ID }}
          DBT_FABRIC_LAKEHOUSE_ID: ${{ secrets.TEST_LAKEHOUSE_ID }}
          
  deploy-staging:
    needs: test
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to staging
        run: |
          dbt_wrapper run --workflow staging
        env:
          DBT_FABRIC_WORKSPACE_ID: ${{ secrets.STAGING_WORKSPACE_ID }}
          DBT_FABRIC_LAKEHOUSE_ID: ${{ secrets.STAGING_LAKEHOUSE_ID }}
          
  deploy-production:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to production
        run: |
          dbt_wrapper run --workflow production
        env:
          DBT_FABRIC_WORKSPACE_ID: ${{ secrets.PROD_WORKSPACE_ID }}
          DBT_FABRIC_LAKEHOUSE_ID: ${{ secrets.PROD_LAKEHOUSE_ID }}
```

### Azure DevOps Integration

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
      - develop

pool:
  vmImage: 'ubuntu-latest'

stages:
  - stage: Test
    jobs:
      - job: RunTests
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'
              
          - script: |
              pip install -e .
              dbt_wrapper test $(dbt_project)
            displayName: 'Run dbt tests'
            env:
              DBT_FABRIC_WORKSPACE_ID: $(TEST_WORKSPACE_ID)
              DBT_FABRIC_LAKEHOUSE_ID: $(TEST_LAKEHOUSE_ID)
              
  - stage: DeployStaging
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/develop')
    dependsOn: Test
    jobs:
      - deployment: DeployToStaging
        environment: staging
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    dbt_wrapper run --workflow staging
                  displayName: 'Deploy to staging'
                  env:
                    DBT_FABRIC_WORKSPACE_ID: $(STAGING_WORKSPACE_ID)
                    DBT_FABRIC_LAKEHOUSE_ID: $(STAGING_LAKEHOUSE_ID)
                    
  - stage: DeployProduction
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
    dependsOn: Test
    jobs:
      - deployment: DeployToProduction
        environment: production
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    dbt_wrapper run --workflow production
                  displayName: 'Deploy to production'
                  env:
                    DBT_FABRIC_WORKSPACE_ID: $(PROD_WORKSPACE_ID)
                    DBT_FABRIC_LAKEHOUSE_ID: $(PROD_LAKEHOUSE_ID)
```

## Performance Optimization Patterns

### Parallel Execution Strategy

Use stage management for parallel execution:

```bash
# Terminal 1: Metadata preparation
dbt_wrapper stage run clean metadata-extract metadata-download

# Terminal 2: Meanwhile, prepare other resources
dbt_wrapper stage run pre-scripts

# Terminal 3: Run build when ready
dbt_wrapper stage run build post-scripts
```

### Caching Strategy

Skip expensive operations when possible:

```bash
# Full build (run once)
dbt_wrapper deploy my_project

# Incremental updates (run repeatedly)
dbt_wrapper deploy my_project --skip clean,metadata-extract,metadata-download

# Or use build workflow for quick iterations
dbt_wrapper build my_project --select +changed_model+
```

### Resource-Specific Optimization

```yaml
workflows:
  # Heavy computation workflow
  compute-heavy:
    stages: [metadata-download, build, post-scripts]
    options:
      notebook_timeout: 7200
      log_level: WARNING
      
  # I/O intensive workflow
  io-heavy:
    stages: [clean, metadata-extract, metadata-download, build]
    options:
      notebook_timeout: 3600
      log_level: INFO
      
  # Network intensive workflow
  network-heavy:
    stages: [upload, execute, results]
    options:
      notebook_timeout: 5400
      log_level: INFO
```

## Error Handling and Debugging

### Debug Workflow Pattern

```yaml
workflows:
  debug:
    description: Debug workflow with verbose logging
    stages: [clean, pre-scripts, metadata-extract, build]
    options:
      log_level: DEBUG
      hashcheck_level: WARNING
```

```bash
# Run debug workflow
dbt_wrapper run --workflow debug

# Examine specific stage
dbt_wrapper stage describe metadata-extract

# Run single stage for troubleshooting
dbt_wrapper stage run metadata-extract
```

### Incremental Debugging

```bash
# Start from clean state
dbt_wrapper stage run clean

# Add stages one by one
dbt_wrapper stage run pre-scripts
dbt_wrapper stage run metadata-extract
dbt_wrapper stage run metadata-download
dbt_wrapper stage run build

# Check results at each step
ls target/  # Check generated files
```

### Error Recovery Patterns

```bash
# After failure, identify last successful stage
dbt_wrapper status  # (when available)

# Resume from specific stage
dbt_wrapper stage run build post-scripts upload execute

# Or skip problematic stage temporarily
dbt_wrapper deploy my_project --skip metadata-extract
```

## Team Collaboration Patterns

### Shared Configuration Management

```bash
# Team lead creates shared config
dbt_wrapper config init --path .dbt-wrapper.team.yml

# Team members use shared config
dbt_wrapper run --workflow dev --config .dbt-wrapper.team.yml

# Individual overrides
cp .dbt-wrapper.team.yml .dbt-wrapper.yml
# Edit .dbt-wrapper.yml for personal preferences
```

### Role-Based Workflows

```yaml
# .dbt-wrapper.yml
workflows:
  # Data engineer workflow
  engineer:
    description: Full development workflow
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts]
    options:
      log_level: INFO
      
  # Analyst workflow (simplified)
  analyst:
    description: Analysis workflow
    stages: [metadata-download, build]
    options:
      log_level: WARNING
      
  # DevOps workflow (deployment focus)
  devops:
    description: Deployment workflow
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute, results]
    options:
      log_level: ERROR
      hashcheck_level: ERROR
```

### Environment Handoff Pattern

```bash
# Developer: Prepare for handoff
dbt_wrapper run --workflow dev
dbt_wrapper stage run post-scripts  # Ensure notebooks are ready

# DevOps: Deploy to staging
dbt_wrapper run --workflow staging

# QA: Validate in staging
dbt_wrapper run --workflow validate-staging

# DevOps: Deploy to production
dbt_wrapper run --workflow production
```

## Monitoring and Observability

### Execution Tracking

```bash
# Run with execution tracking
dbt_wrapper deploy my_project 2>&1 | tee execution.log

# Monitor long-running processes
dbt_wrapper status --watch  # (when available)

# Historical analysis
dbt_wrapper history --limit 10  # (when available)
```

### Custom Logging Patterns

```yaml
workflows:
  monitored-deploy:
    description: Deployment with enhanced monitoring
    stages: [clean, pre-scripts, metadata-extract, build, post-scripts, upload, execute, results]
    options:
      log_level: INFO
      notebook_timeout: 3600
      monitoring_enabled: true
```

## Integration with External Tools

### Docker Integration

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install -e .

# Default to interactive mode for flexibility
CMD ["dbt_wrapper", "run", "--interactive"]
```

```bash
# Build and run
docker build -t my-dbt-wrapper .
docker run -it \
  -e DBT_FABRIC_WORKSPACE_ID=${WORKSPACE_ID} \
  -e DBT_FABRIC_LAKEHOUSE_ID=${LAKEHOUSE_ID} \
  -v $(pwd):/app \
  my-dbt-wrapper
```

### VS Code Integration

Create tasks.json for VS Code:

```json
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "dbt: dev workflow",
      "type": "shell",
      "command": "dbt_wrapper",
      "args": ["dev", "${workspaceFolder}"],
      "group": "build",
      "problemMatcher": []
    },
    {
      "label": "dbt: deploy",
      "type": "shell",
      "command": "dbt_wrapper",
      "args": ["deploy", "${workspaceFolder}"],
      "group": "build",
      "problemMatcher": []
    },
    {
      "label": "dbt: interactive",
      "type": "shell",
      "command": "dbt_wrapper",
      "args": ["run", "--interactive"],
      "group": "build",
      "problemMatcher": []
    }
  ]
}
```

## Best Practices Summary

1. **Start Simple**: Begin with built-in workflows, then customize as needed
2. **Use Configuration**: Prefer YAML configuration over complex command lines
3. **Environment Separation**: Maintain separate configurations for different environments
4. **Stage Management**: Use stage commands for debugging and custom orchestration
5. **Team Standards**: Establish shared workflows and naming conventions
6. **Version Control**: Commit configuration files to version control
7. **Documentation**: Document custom workflows and their purposes
8. **Monitoring**: Implement logging and monitoring for production deployments
9. **Testing**: Test workflows thoroughly before production deployment
10. **Gradual Migration**: Migrate from old patterns gradually and systematically