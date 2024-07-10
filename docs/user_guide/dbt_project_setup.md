# DBT Project Setup 

## Fabric Workspace Setup

Next we will create a new dbt project and configure it to use the dbt-fabricsparknb adapter. But, before we do this we need to gather some information from the Power BI / Fabric Portal. To do this, follow the steps below:

- Open the Power BI Portal and navigate to the workspace you want to use for development. If necessary, create a new workspace.
- Ensure that the workspace is Fabric enabled. If not, enable it.
- Make sure that there is at least one Datalake in the workspace.
- Get the connection details for the workspace. This will include the workspace name, the workspace id, and the lakehouse id. The easiest way to get this information is to navigate to a file or folder in the lakehouse, click on the three dots to the right of the file or folder name, and select "Properties". Details will be displayed in the properties window. From these properties select copy url and paste it into a text editor. The workspace id is the first GUID in the URL, the lakehouse id is the second GUID in the URL. In the example below, the workspace id is `4f0cb887-047a-48a1-98c3-ebdb38c784c2` and the lakehouse id is `aa2e5f92-53cc-4ab3-9a54-a6e5b1aeb9a9`.

> https://onelake.dfs.fabric.microsoft.com/4f0cb887-047a-48a1-98c3-ebdb38c784c2/aa2e5f92-53cc-4ab3-9a54-a6e5b1aeb9a9/Files/notebooks


## Create Dbt Project
Once you have taken note of the workspace id, lakehouse id, and workspace name, you can create a new dbt project and configure it to use the dbt-fabricsparknb adapter. To do this, run the code shown below:

!> **Important** Note when asked to select the adapter choose `dbt-fabricksparknb`. During this process you will also be asked for the `workspace id`, `lakehouse id`, and `workspace name`. Use the values you gathered from the Power BI Portal. 


```powershell
# Create your dbt project directories and profiles.yml file
dbt init my_project # Note that the name of the project is arbitrary... call it whatever you like
```

The command above will create a new directory called `my_project`. Within this directory you will find a `profiles.yml` file. Open this file in your favourite text editor and note that it should look like the example below except that in your case my_project will be replaced with the name of the project you created above.:

```yaml
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'my_project'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: 'my_project'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.
models:
  test4:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view

```

The dbt init command will also update your `profiles.yml` file with a profile matching your dbt project name. Open this file in your favourite text editor using the command below:

<!-- tabs:start -->

#### **Windows**

```powershell

code  $home/.dbt/profiles.yml

```

#### **MacOS**

```powershell
code  ~/.dbt/profiles.yml
```

#### **Linux**

```powershell
code  ~/.dbt/profiles.yml
```
<!-- tabs:end -->

When run this will display a file similar to the one below. Check that your details are correct.

```yaml
test4:
  outputs:
    dev:
      auth: cli #remove
      client_id: dlkdjl #remove
      client_scrent: dlkdjl #remove
      connect_retries: 0 #remove
      connect_timeout: 0 #remove
      endpoint: dkld #remove
      lakehouse: 'lakehouse' #the name of your lakehouse
      lakehouseid: 'aa2e5f92-53cc-4ab3-9a54-a6e5b1aeb9a9' #the guid of your lakehouse
      method: livy
      schema: dbo #the schema you want to use
      tenant_id: '72f988bf-86f1-41af-91ab-2d7cd011db47' #your power bi tenant id
      threads: 1 #the number of threads to use
      type: fabricsparknb #the type of adapter to use.. always use fabricsparknb
      workspaceid: '4f0cb887-047a-48a1-98c3-ebdb38c784c2' #the guid of your workspace
  target: dev
```

Now we are ready to run our dbt project for the first time. But first we need to create a build script.

### Create a python build script that will wrap our dbt process 

This repository contains a dbt build script created in python. Make a copy of this script by copying the code found at [https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb/blob/main/test_post_install.py](https://github.com/Insight-Services-APAC/APAC-Capability-DAI-DbtFabricSparkNb/blob/main/test_post_install.py). Alternatively, you can copy the code in the code block titled [Python Build script template](#python-build-script-template) below. Paste the code into a new file in the root of your source code directory. You can create this file using vscode using the command line shown in the code block titled [New file creation in vscode](#New-file-creation-in-vscode) below.

> [!IMPORTANT]
> Be sure to change the line below replacing "testproj" with the folder name of your dbt project.</br>
> `os.environ['DBT_PROJECT_DIR'] = "testproj"`

#### New file creation in vscode

```powershell
code post_install.py
```

#### Python Build script template
```python
import subprocess
from dbt.adapters.fabricsparknb import utils as utils
import dbt
from pathlib import Path
import os
import shutil

os.environ['DBT_PROJECT_DIR'] = "testproj"  # !!!! Change this to the path of your dbt project

# Get Config and Profile Information from dbt
profile_path = Path(os.path.expanduser('~')) / '.dbt/'
profile = dbt.config.profile.read_profile(profile_path)
config = dbt.config.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])

profile_info = profile[config['profile']]
target_info = profile_info['outputs'][profile_info['target']]
lakehouse = target_info['lakehouse']

shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")

# Generate AzCopy Scripts and Metadata Extract Notebooks
utils.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])
if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks"):
    os.makedirs(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks")

utils.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
utils.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])


# Check if metaextracts directory exists in os.environ['DBT_PROJECT_DIR'] and create it if it doesn't
if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/metaextracts"):
    os.makedirs(os.environ['DBT_PROJECT_DIR'] + "/metaextracts")

# count files in metaextracts directory
if len(os.listdir(os.environ['DBT_PROJECT_DIR'] + "/metaextracts")) == 0:
    print('\033[1;33;48m', "It seems like this is the first time you are running this project. Please update the metadata extract json files in the metaextracts directory by performing the following steps:")
    print(f"1. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/upload.ps1")
    print("2. Login to the Fabric Portal and navigate to the workspace and lakehouse you are using")
    print(f"3. Manually upload the following notebook to your workspace: {os.environ['DBT_PROJECT_DIR']}/target/notebooks/import_{os.environ['DBT_PROJECT_DIR']}_notebook.ipynb. See https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks")
    print(f"4. Open the notebook in the workspace and run all cells. This will upload the generated notebooks to your workspace.")
    print(f"5. A new notebook should appear in the workspace called metadata_{os.environ['DBT_PROJECT_DIR']}_extract.ipynb. Open this notebook and run all cells. This will generate the metadata extract json files in the metaextracts directory.")
    print(f"6. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/download.ps1. This will download the metadata extract json files to the metaextracts directory.")
    print(f"7. Re-run this script to generate the model and master notebooks.")
else:
    # Call dbt build
    subprocess.run(["dbt", "build"], check=True)
    # Generate Model Notebooks and Master Notebooks
    utils.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
    utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
```

### Run the build script
Run the build script using the code below in the terminal.
```powershell
python <YourProjectName>_post_install.py
```

> [!TIP]
> The first time you run this you will be prompted to follow a series of steps that will download a set of metadata files from your Fabric Lakehouse. Be sure to follow these steps. You should only need to do them once.

> [!TIP]
> If you get an error with Azure CLI connection issues or type errors. This is because the Profile.yaml file has the incorrect adaptor set. It should be *"fabricsparknb"* not *"fabricspark"*.

### Post Build Steps & Checks

After successful execution and number of notebooks have been created in your project/target folder under notebooks. 

*import_notebook.ipynb* this will be used to import notebook files into your lakehouse.

*metadata_extract.ipynb* is used to update the metadata json files in your project. 

These 2 can be imported using the standard import notebooks function in fabric. The rest of the notebooks can be copied into your lakehouse Files/notebooks folder using Onelake explorer. 

You then open the *import_notebook.ipynb* in fabric and *Run All* to import the notebooks from the Files/Notebooks directory in fabric. 

Executing the *master_notebook.ipynb* notebook will execute all notebooks created in your project.

This concludes the Framework setup.