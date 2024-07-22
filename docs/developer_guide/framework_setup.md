# Framework Setup

### Fabric Workspace Setup

Here we will create a new dbt project and configure it to use the dbt-fabricsparknb adapter. But, before we do this we need to gather some information from the Power BI / Fabric Portal. To do this, follow the steps below:

1. Open the Power BI Portal and navigate to the workspace you want to use for development. If necessary, create a new workspace.
1. Ensure that the workspace is Fabric enabled. If not, enable it.
1. Make sure that there is at least one Datalake in the workspace.
1. Get the connection details for the workspace. 
      1. Get the lakehouse name, the workspace id, and the lakehouse id. 
      2. The lakehouse name and workspace name are easily viewed from the fabric / power bi portal.
      3. The easiest way to get this information is to 
          1. Navigate to a file or folder in the lakehouse, 
          2. click on the three dots to the right of the file or folder name, and select "Properties". Details will be displayed in the properties window. 
          3. From these properties select copy url and paste it into a text editor. The workspace id is the first GUID in the URL, the lakehouse id is the second GUID in the URL. 
          4. In the example below, the workspace id is `4f0cb887-047a-48a1-98c3-ebdb38c784c2` and the lakehouse id is `aa2e5f92-53cc-4ab3-9a54-a6e5b1aeb9a9`.

> https://onelake.dfs.fabric.microsoft.com/4f0cb887-047a-48a1-98c3-ebdb38c784c2/aa2e5f92-53cc-4ab3-9a54-a6e5b1aeb9a9/Files/notebooks


### Create Dbt Project
Once you have taken note of the workspace id, lakehouse id, and lakehouse name, you can create a new dbt project and configure it to use the dbt-fabricsparknb adapter. To do this, run the code shown below:

!> **Important** Note when asked to select the adapter choose `dbt-fabricksparknb`. If you can't see the adapter, first install the dbt-fabricsparknb package from repository. During this process you will also be asked for the `workspace id`, `lakehouse id`, and `lakehouse name`. Use the values you gathered from the Power BI Portal. 


```bash
# Create your dbt project directories and profiles.yml file
dbt init my_project # Note that the name of the project is arbitrary... call it whatever you like
```

The command above will create a new directory called `my_project`. Within this directory you will find a `dbt_project.yml` file. Open this file in your favourite text editor and note that it should look like the example below except that in your case my_project will be replaced with the name of the project you created above.:

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
# files using the ` config(...) ` macro.
models:
  test4:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view

```

The dbt init command will also update your `profiles.yml` file with a profile matching your dbt project name. Open this file in VS Code. This file can be found in *"./%USER_DIRECTORY%/.dbt/"*

When run this will display a file similar to the one below. Check that your details are correct. 

```yaml
my_project:
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

To complete the newly created project you will need to copy some directories from the project called *"ptfproj"* dbt project. Copy *ptfproj/macros/* and *ptfproj/metaextracts/* directories with their files into your new dbt project. Overwrite any directories or files if they exist. Now in metaextracts the file ListSchemas.json contains the lakehouses in your workspace. You can manually update this file.

```json
[{"namespace":"lh_raw"},{"namespace":"lh_conformed"},{"namespace":"lh_consolidated"}]
```

### Review the build python Script

To ensure a successful dbt build process, please verify that the Python script *test_pre_install.py* exists in the root directory of your repository

```python
from dbt.adapters.fabricsparknb import utils as utils
import os 
import sys

utils.RunDbtProjectArg(PreInstall=True,argv = sys.argv)
```

### Execute the build python Script
You can execute this file by passing your project name as the parameter
```bash
python test_pre_install.py my_project
```

If you get an error with Azure CLI connection issues or type errors. This is because the Profile.yaml file has the incorrect adaptor set. It should be *"fabricsparknb"* not *"fabricspark"*.

After successful execution and number of notebooks have been created in your project/target folder under notebooks. 

*import_notebook.ipynb* this will be used to import notebook files into your lakehouse.

*metadata_extract.ipynb* is used to update the metadata json files in your project. 

The above two notebooks can be imported using the standard import notebooks function in fabric. The rest of the notebooks can be copied into your lakehouse Files/notebooks folder by running the following script in pwsh. 

```powershell
#Run upload.ps1
Invoke-Expression -Command $env:DBT_PROJECT_DIR/target/pwsh/upload.ps1
```

You then open the *import_notebook.ipynb* in fabric and *Run All* to import the notebooks from the Files/Notebooks directory in fabric. 

Similar to  upload, using the following pwsh script will help you to download the metaextract files to the metaextrcats folder in repo.

```powershell
#Run upload.ps1
Invoke-Expression -Command $env:DBT_PROJECT_DIR/target/pwsh/download.ps1
```

Executing the *master_notebook.ipynb* notebook will execute all notebooks created in your project.

This concludes the Framework setup.