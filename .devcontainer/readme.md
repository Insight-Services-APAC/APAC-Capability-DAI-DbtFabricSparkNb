## Instructions for using the Spark Minimal Dev Container

This Dev Container is designed to provide a minimal setup for running a local development server with Apache Spark, SQL Server and PowerShell support. Below are the steps to set up and use this container effectively.

1. First Install Powershell. Open a terminal in the Dev Container and run the following commands:

``` bash
source ./scripts/dev_container_scripts/spark_minimal/pwsh_install.sh 
```
2. After installing PowerShell, you can start it by running:

``` bash
pwsh  
```

Next let's install some development tools. In the PowerShell terminal, run:
``` powershell
./scripts/dev_container_scripts/spark_minimal/dev_tools.ps1
```

Great! Now you have a minimal development environment set up with PowerShell and other tools.
Restart your powershell terminal to apply the changes.

``` pwsh
. $PROFILE  
```
Install SQL Server and SQL Connectivity tools. In the PowerShell terminal, run:

``` pwsh
bash ./scripts/dev_container_scripts/spark_minimal/sql_install_4_linux.sh
```

Next setup SQL Server. NOTE that in this step you will need to provide an SA password. Take note of the password as you will need to use it in the future. When asked for a SQL Version select "Enterprise (2)" and "English". In the PowerShell terminal, run:

``` pwsh
bash /opt/mssql/bin/mssql-conf setup
```
At the end of the process above you will get an error "No such file or directory: 'systemctl'". This is expected and can be ignored. SQL Server is now installed. You can start it by running:

``` pwsh
/opt/mssql/bin/sqlservr
```