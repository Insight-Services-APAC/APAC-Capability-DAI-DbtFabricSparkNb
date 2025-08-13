# dbt-fabricsparknb (Insight version)

The `dbt-fabricsparknb` package is a powerful dbt adapter that enables dbt to work with Microsoft Fabric **WITHOUT** requiring connection endpoints like Livy or the Datawarehouse SQL endpoint. It provides a true SaaS lakehouse experience with an intuitive workflow-based CLI.

## 🚀 Quick Start

```bash
# Install the package
uv pip install -e . -r requirements.txt

# Run development workflow
dbt_wrapper dev my_project

# Deploy to Fabric
dbt_wrapper deploy my_project
```

## 📚 Documentation

Comprehensive documentation is available at: **[GitHub Pages](https://insight-services-apac.github.io/APAC-Capability-DAI-DbtFabricSparkNb/)**

## ✨ Key Features

- **No connection endpoints required** - Works directly with Fabric notebooks
- **Intuitive workflow commands** - `dev`, `deploy`, `build`, `test` match your intent
- **Configuration as code** - YAML-based workflow definitions
- **Interactive mode** - Guided workflow selection for new users
- **Flexible stage control** - Skip or run only specific pipeline stages
- **Full dbt compatibility** - Supports all standard dbt features

## 🔧 Installation

### Prerequisites
- Python 3.12+
- Microsoft Fabric workspace with appropriate permissions
- dbt-core installation

### Install from source
```bash
# Clone the repository
git clone https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb.git
cd APAC-Capability-DAI-DbtFabricSparkNb

# Install with uv (recommended)
uv pip install -e . -r requirements.txt

# Or install with pip
pip install -e . -r requirements.txt

# Install development dependencies
uv pip install -e ".[dev]"

# Run pre-commit hooks
pre-commit install
```

## 🎯 Main Commands

### Workflow Commands
| Command | Description | Stages |
|---------|-------------|--------|
| `dbt_wrapper dev` | Development workflow | clean → pre-scripts → metadata → build → post-scripts |
| `dbt_wrapper deploy` | Full deployment | clean → pre-scripts → metadata → build → post-scripts → upload → execute |
| `dbt_wrapper build` | Minimal build | metadata-download → build |
| `dbt_wrapper build-local` | Local build only | build → post-scripts |
| `dbt_wrapper test` | Testing workflow | clean → metadata → build → validation |

### Configuration Commands
```bash
# Initialize configuration
dbt_wrapper config init

# List workflows
dbt_wrapper config list

# Validate configuration
dbt_wrapper config validate
```

### Stage Management
```bash
# List available stages
dbt_wrapper stage list

# Describe a stage
dbt_wrapper stage describe build

# Run specific stages
dbt_wrapper stage run clean build
```

## ⚙️ Configuration

Create a `.dbt-wrapper.yml` file:

```yaml
version: '1.0'

defaults:
  log_level: WARNING
  notebook_timeout: 1800
  lakehouse_config: METADATA

workflows:
  dev:
    description: Development workflow
    stages:
      - clean
      - pre-scripts
      - metadata-extract
      - build
      - post-scripts
    options:
      log_level: INFO
      upload_notebooks: false
```

## 🏗️ Architecture

This adapter is built on top of [dbt-fabricspark](https://github.com/microsoft/dbt-fabricspark) and extends it with:

- **Notebook-based execution** - Generates and executes notebooks directly in Fabric
- **Stage executor pattern** - Operations organized into selectively executable stages
- **Metadata extraction** - Automated lakehouse metadata extraction and comparison
- **Workflow management** - Powerful CLI for managing complex data pipelines

## 📋 Requirements

### Core Dependencies
- `dbt-fabricspark @ git+https://github.com/microsoft/dbt-fabricspark.git`
- `dbt-spark==1.9.3`
- `msfabricpysdkcore==0.2.8`
- `typer>=0.16.0`
- Python 3.12+

See `pyproject.toml` for complete dependency list.

## 🚧 Known Limitations

- **No schema support** - Do not enable "Lakehouse schemas (Public Preview)" in Fabric
- **Unique table names** - Tables must have unique names across lakehouses
- **Column naming** - Avoid special characters (#, %) in column names
- **Service principal** - Limited support for service principal authentication
- **Seeds** - Avoid special characters and spaces in seed file columns

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

1. Create branches with the format `feature/YourBranchName`
2. Make pull requests to the `dev` branch (not `main`)
3. Follow existing code conventions and patterns
4. Add tests for new functionality
5. Update documentation as needed

## 🐛 Reporting Issues

- Report bugs or request features: [Open an issue](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb/issues/new)
- For the upstream adapter: [dbt-fabricspark issues](https://github.com/microsoft/dbt-fabricspark/issues/new)

## 📜 License

Apache-2.0 License - see LICENSE file for details

## 🔗 Related Projects

- **Parent Adapter**: [dbt-fabricspark](https://github.com/microsoft/dbt-fabricspark) - The community version this adapter extends
- **Documentation**: [Full Documentation](https://insight-services-apac.github.io/APAC-Capability-DAI-DbtFabricSparkNb/)
- **Repository**: [GitHub Repository](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb)
