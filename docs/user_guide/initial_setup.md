---
  weight: 1
---
# Environment Setup
This section outlines the steps required to setup the development environment to use this dbt-adapter as part of a dbt data transformation project.

To provide a common, cross-platform set of instructions we will first install Powershell. To facilitate the installation process we will use package managers such as [winget](https://learn.microsoft.com/en-us/windows/package-manager/winget/) for Windows, [brew](https://brew.sh/) for MacOS and `apt` for Linux.

## Core Tools Installation

!!!Tip
    Following core tools can be installed using the standard powershell or in VS Code terminal (powershell)

=== "Windows"

    ```powershell
    # Winget Installs 
    winget install Microsoft.PowerShell
    ```

=== "MacOS"

    ```bash
    brew install powershell/tap/powershell
    ```

=== "Linux"
    ```bash
    # TBA
    ```

Next we will install Python and development tools such as vscode.

=== "Windows"

    ```powershell
    # Winget Installs 
    winget install Python.Python.3.12
    winget install -e --id Microsoft.VisualStudioCode
    winget install --id Git.Git -e --source winget

    # Python Environment Manager
    Python -m pip install --user virtualenv
    ```

=== "MacOS"

    ```bash
    # Brew Installs
    brew install python@3.12
    brew install --cask visual-studio-code
    brew install git

    # Python Environment Manager
    Python -m pip install --user virtualenv


    ```

=== "Linux"
    ```bash
    # TBA
    ```

## Other tools
Now that we have pwsh installed, Make sure that you have install the following additional required tools.

- Install Azure PowerShell on Windows - Refer [Azure Powershell Doc](https://learn.microsoft.com/en-us/powershell/azure/install-azps-windows?view=azps-12.1.0&tabs=powershell&pivots=windows-psgallery) for windows (You might need to run this in admin mode)

!!! Important
    Optional packages you may need to install (Only run if you face issues)

    ```powershell
    pip config set global.trusted-host "pypi.org files.pythonhosted.org pypi.python.org"
    ```

## Source Directory & Python Env
Now lets create and activate our Python environment and install the required packages.

!!! important
    Python 3.12 or higher is required. The adapter uses `uv` as the preferred package manager for faster and more reliable dependency resolution.

!!! tip
    When executing the following, it can take a few minutes to complete on some machines. We recommend using `uv` for faster installation.


=== "Windows (Recommended with uv)"

    ```powershell
    # Ensure that you are in the pwsh shell
    pwsh

    # Install uv package manager (if not already installed)
    pip install uv

    # Clone the repository
    git clone https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb.git
    cd APAC-Capability-DAI-DbtFabricSparkNb

    # Create the Python environment
    python -m venv .venv

    # Optional step if activation fails due to security policy
    Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser
 
    # Activate the Python environment
    ./.venv/Scripts/Activate.ps1

    # Install using uv (recommended)
    uv pip install -e . -r requirements.txt

    # Or install from PyPI directly
    pip install --upgrade git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb
    ```

=== "MacOS"

    ```bash
    # Install uv package manager (if not already installed)
    pip install uv

    # Clone the repository
    git clone https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb.git
    cd APAC-Capability-DAI-DbtFabricSparkNb

    # Create the Python environment
    python3.12 -m venv .venv
    
    # Activate the Python environment
    source .venv/bin/activate

    # Install using uv (recommended)
    uv pip install -e . -r requirements.txt

    # Or install from PyPI directly
    pip install --upgrade git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb
    ```

=== "Linux"

    ```bash
    # Install uv package manager (if not already installed)
    pip install uv

    # Clone the repository
    git clone https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb.git
    cd APAC-Capability-DAI-DbtFabricSparkNb

    # Create the Python environment
    python3.12 -m venv .venv
    
    # Activate the Python environment
    source .venv/bin/activate

    # Install using uv (recommended)
    uv pip install -e . -r requirements.txt

    # Or install from PyPI directly
    pip install --upgrade git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb
    ```
!!!Tip
    To obtain a specific or the latest version of the framework packages in dbt, you need to specify the framework tags as following (example: for version 0.4.0)
    ```powershell
    pip install --upgrade --force-reinstall git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb@V0.4.0
    ```


!!! info
    You are now ready to move to the next step in which you will set up your dbt project. Follow the [Dbt Project Setup](./dbt_project_setup.md) guide.


