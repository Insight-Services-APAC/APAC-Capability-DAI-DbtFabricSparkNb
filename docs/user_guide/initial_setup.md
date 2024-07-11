---
  weight: 1
---
# Environment Setup
This section outlines the steps required to setup the development environment to use this dbt-adapter as part of a dbt data transformation project.

To provide a common, cross-platform set of instructions we will first install Powershell. To facilitate the installation process we will use package managers such as [winget](https://learn.microsoft.com/en-us/windows/package-manager/winget/) for Windows, [brew](https://brew.sh/) for MacOS and `apt` for Linux.

## Core Tools Installation

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
    winget install -e --id Python.Python -v 3.12
    winget install -e --id Microsoft.VisualStudioCode
    winget install --id Git.Git -e --source winget

    # Python Environment Manager
    Python3 -m pip install --user virtualenv
    ```

=== "MacOS"

    ```bash
    # Brew Installs
    brew install python@3.12
    brew install --cask visual-studio-code
    brew install git

    # Python Environment Manager
    Python3 -m pip install --user virtualenv

    # TODO 
    # Add OSX AZ Copy Instructions

    ```

=== "Linux"
    ```bash
    # TBA
    ```



## Other tools
Now that we have pwsh installed, we can use it as a cross platform shell to install the additional required tools. 



=== "Windows"

    ```powershell

    # Az Copy Install - No Winget Package Available
    Invoke-WebRequest -Uri https://aka.ms/downloadazcopy-v10-windows -OutFile AzCopy.zip -UseBasicParsing
    Expand-Archive ./AzCopy.zip ./AzCopy -Force
    New-Item -ItemType "directory" -Path "$home/AzCopy"  -Force  
    Get-ChildItem ./AzCopy/*/azcopy.exe | Move-Item -Destination "$home\AzCopy\AzCopy.exe" -Force  
    $userenv = [System.Environment]::GetEnvironmentVariable("Path", "User") 
    [System.Environment]::SetEnvironmentVariable("PATH", $userenv + ";$home\AzCopy", "User")
    Remove-Item .\AzCopy\ -Force
    Remove-Item AzCopy.zip -Force

    ```

=== "MacOS"

    ```bash
    # TODO 
    # Add OSX AZ Copy Instructions

    ```

=== "Linux"
    ```bash
    # TBA
    ```


## Source Directory & Python Env
Now lets create and activate our Python environment and install the required packages.


!!! tip
    When doing pip install dbt-fabricspark below it can take a few minutes to complete on some machines. Occasionally pip may ge stuck and in such cases break the execution using ctrl-c and run the same pip again, 


=== "Windows"

    ```powershell

    # Ensure that you are in the pwsh shell
    pwsh

    # Create a new source code directory
    mkdir dbt-fabricsparknb-test #Note that the name of the directory is arbitrary... call it whatever you like
    # Navigate to the new directory
    cd dbt-fabricsparknb-test

    # Create and activate the Python environment
    python3 -m venv .env
    ./.env/Scripts/Activate.ps1   

    # Install dbt-core 
    pip install dbt-core

    # Install dbt-fabricspark
    pip install dbt-fabricspark

    # Install the dbt-fabricsparknb pre-requisites 
    pip install azure-storage-file-datalake
    pip install nbformat

    # Install the dbt-fabricsparknb package from the repository
    pip install --upgrade git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb

    ```


=== "MacOS"

    ```powershell

    # Ensure that you are in the pwsh shell
    pwsh

    # Create a new source code directory
    mkdir dbt-fabricsparknb-test #Note that the name of the directory is arbitrary... call it whatever you like
    # Navigate to the new directory
    cd dbt-fabricsparknb-test

    # Create and activate the Python environment
    python3 -m venv .env
    ./.env/bin/Activate.ps1   

    # Install dbt-core 
    pip install dbt-core

    # Install dbt-fabricspark
    pip install dbt-fabricspark

    # Install the dbt-fabricsparknb pre-requisites 
    pip install azure-storage-file-datalake
    pip install nbformat

    # Install the dbt-fabricsparknb package from the repository
    pip install --upgrade git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb


    ```

=== "Linux"

    ```powershell
    # Ensure that you are in the pwsh shell
    pwsh

    # Create a new source code directory
    mkdir dbt-fabricsparknb-test #Note that the name of the directory is arbitrary... call it whatever you like
    # Navigate to the new directory
    cd dbt-fabricsparknb-test

    # Create and activate the Python environment
    python3 -m venv .env
    ./.env/bin/Activate.ps1   

    # Install dbt-core 
    pip install dbt-core

    # Install dbt-fabricspark
    pip install dbt-fabricspark

    # Install the dbt-fabricsparknb pre-requisites 
    pip install azure-storage-file-datalake
    pip install nbformat

    # Install the dbt-fabricsparknb package from the repository
    pip install --upgrade git+https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb


```


!!! info
    You are now ready to move to the next step in which you will set up your dbt project. Follow the [Dbt Project Setup](./dbt_project_setup.md) guide.


