---
  weight: 3
---

# Setting Up dbt

The following sections are covered in this document:

1. Clone the main Repo
    1. Clone the main repo in powershell
    2. Clone the repo in VSCode
2. dbt dependency installation


### a. Clone the main repo in powershell

Clone the main repo from the Github. Use the following pwsh code for that. 

```powershell
# Clone the repo
git clone https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb.git
```

### b. Clone the main repo in VSCode
Clone the main repo locally using Visual studio code. 

!!!Note
    If you do not get the Clone Repository option when selecting Source Control from the menu, then you have not installed GIT and will need to complete that first.

![image info](./images/CloneRepo.png)


### dbt dependency installation

!!!Info
    Ensure you remain in the PowerShell console with your virtual environment activated. 
    You can then execute the following command to install or update all the components needed for the dbt framework. Since the `requirements.txt` file is located in the root of the repository, be sure to run the command from the root directory.

```powershell
# dbt dependency installation/updation
pip install -r requirements.txt

```
!!!Note
    The installation or update process might take some time to complete and could appear to be hanging, but it is actively running. If you need to stop the process, you can restart it using the same command. It will bypass any components that are already installed.

This concludes the dbt installation.

!!! Info
    You are now ready to move to the next step in which you set up the build process. Follow the [Dbt Build Process](./dbt_build_process.md) guide.