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

!!! tip
    When executing the following, it can take a few minutes to complete on some machines. Occasionally pip may get stuck and in such cases break the execution using ctrl-c and run the same pip again. 


=== "Windows"

    ```powershell

    # Ensure that you are in the pwsh shell
    pwsh

    # Create a new source code directory
    mkdir dbt-fabricsparknb-test #Note that the name of the directory is arbitrary... call it whatever you like
    # Navigate to the new directory
    cd dbt-fabricsparknb-test

    # Create the Python environment
    python -m venv .env

    #Optional step to run if activate.ps1 failes due to security policy
    Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser
 
    # Activate the Python environment
    ./.env/Scripts/Activate.ps1

    # Install the dbt-fabricsparknb package from the repository
    pip install --upgrade --force-reinstall git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb

    ```


=== "MacOS"

    ```powershell

    # Ensure that you are in the pwsh shell
    pwsh

    # Create a new source code directory
    mkdir dbt-fabricsparknb-test #Note that the name of the directory is arbitrary... call it whatever you like
    # Navigate to the new directory
    cd dbt-fabricsparknb-test

    # Create the Python environment
    python -m venv .env

    #Optional step to run if activate.ps1 failes due to security policy
    Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser
    
    # Activate the Python environment
    ./.env/Scripts/Activate.ps1  

    # Install the dbt-fabricsparknb package from the repository
    pip install --upgrade --force-reinstall git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb

    ```

=== "Linux"

    ```powershell

    # TBA


    ```


!!! info
    You are now ready to move to the next step in which you will set up your dbt project. Follow the [Dbt Project Setup](./dbt_project_setup.md) guide.


