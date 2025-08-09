---

    title: "Dbt Build Process"
    excerpt: "This guide will walk you through the process of building your dbt project using the dbt-fabricsparknb package."
    sidebar_label: "Dbt Build Process"
    slug: /documentation_guide/dbt_build_process
    weight: 5

---

# Dbt Build Process

## Dbt Build Process & Dbt_Wrapper
The dbt-fabricksparknb package includes a console application that will allow you to build your dbt project and generate a series of notebooks that can be run in a Fabric workspace. This application is called `dbt_wrapper` and is a python script that is run from the command line.

The new version provides **intuitive, workflow-based commands** that make development much more efficient.

!!! Important
  
    Before running the dbt_wrapper make sure you're logged into your tenant in the PowerShell terminal using both az login. See the examples below and replace the tenant id with your own.

    ```powershell
    az login --tenant 73738727-cfc1-4875-90c2-2a7a1149ed3d --allow-no-subscriptions
    ```

    If you're encountering an az not found error, install az using the following command (powershell admin mode)
    ```powershell
    Install-Module -Name Az -Repository PSGallery -Force
    ```

!!! Note
    Make sure that you have activated your python virtual environment before running this code. 

## Development Environment Commands

For development (non-pip installed), run from the root directory:

```powershell
# View all available commands
python -m dbt_wrapper.main --help
```

### Quick Development Workflows

!!! Note
    Be sure to replace ==my_project== with the name of your dbt project folder. 

#### 🚀 Development Workflow
Perfect for rapid iteration during development:
```powershell
python -m dbt_wrapper.main dev my_project
```
*Runs: clean → pre-scripts → metadata → build → post-scripts*

#### 🔨 Build Only
For quick builds without full setup:
```powershell
python -m dbt_wrapper.main build my_project
```
*Runs: metadata-download → build*

#### 🚢 Deploy Workflow  
Complete deployment when ready for Fabric:
```powershell
python -m dbt_wrapper.main deploy my_project
```
*Runs: clean → pre-scripts → metadata → build → post-scripts → upload → execute*

!!!Tip
    - You can view the execution results of the master notebook directly in the console. To enable this, manually add the `sql_endpoint` of your default lakehouse in your `profile.yml`. (You can find the `sql_endpoint` value in your SQL connection string from the Fabric lakehouse.) 
    - Additionally, ensure that version 18 of the SQL Server driver is installed on your machine. Refer - [ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server?view=sql-server-ver16) 

    ![alt text](./../assets/images/sql_endpoint_profile.png)
    
    Here is the sample scheenshot of the execution results of the master notebook directly in the console
    ![alt text](./../assets/images/console_output.png)


## Advanced Development Usage

### Stage Management for Developers
View and control individual stages:
```powershell
# List all available stages
python -m dbt_wrapper.main stage list

# Get details about a specific stage
python -m dbt_wrapper.main stage describe build

# Run specific stages only
python -m dbt_wrapper.main stage run clean metadata-extract build
```

### Customizing Development Workflows
Skip stages you don't need during development:
```powershell
# Skip upload during development
python -m dbt_wrapper.main deploy my_project --skip upload,execute

# Run only specific stages
python -m dbt_wrapper.main deploy my_project --only build,post-scripts
```

### Configuration-Based Development
Create custom workflows for your development needs:
```powershell
# Initialize a configuration file
python -m dbt_wrapper.main config init

# List available workflows
python -m dbt_wrapper.main config list

# Run a custom workflow
python -m dbt_wrapper.main run --workflow development
```

### Development Options

#### Notebook Timeout Configuration
Increase timeout for long-running development processes:
```powershell
python -m dbt_wrapper.main deploy my_project --notebook-timeout 3600
```

!!! note
    Timeout value can be anything up to 7 days in seconds

#### Lakehouse Configuration Control
Choose how lakehouse is configured in notebooks:
```powershell
# Use code-based configuration (adds %%configure cell)
python -m dbt_wrapper.main dev my_project --lakehouse-config CODE

# Use metadata-based configuration (default)
python -m dbt_wrapper.main dev my_project --lakehouse-config METADATA
```

#### dbt Resource Selection
Use standard dbt selection for focused development:
```powershell
# Work on specific models
python -m dbt_wrapper.main dev my_project --select models/staging

# Exclude certain models during development
python -m dbt_wrapper.main dev my_project --exclude tag:external
```

### Interactive Development Mode
For guided workflow selection:
```powershell
python -m dbt_wrapper.main run --interactive
```

### Legacy Support
The old command structure is still supported but deprecated:
```powershell
python test_pre_install.py run-all my_project --pre-install  # Still works
```

!!! warning "Migration Recommended"
    Migrate to the new workflow commands for better development experience. The new commands are more intuitive and provide better control.

Review all commands available:
```powershell
python -m dbt_wrapper.main --help
```

!!! Info
    You are now ready to move to the next step in which you gain an understanding of the various kinds of notebooks generated by the adapter. Follow the [Understanding the Generated Notebooks](./generated_notebooks.md) guide.

  