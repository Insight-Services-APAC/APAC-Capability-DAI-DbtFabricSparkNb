# Framework Setup

### Fabric Workspace Setup

Here we will create a new dbt project and configure it to use the dbt-fabricsparknb adapter. But, before we do this we need to gather some information from the Power BI / Fabric Portal. To do this, follow the steps below:

- Open the Power BI Portal and navigate to the workspace you want to use for development. If necessary, create a new workspace.
- Ensure that the workspace is Fabric enabled. If not, enable it.
- Make sure that there is at least one Datalake in the workspace.
- Get the connection details for the workspace. This will include the workspace name, the workspace id, and the lakehouse id. The easiest way to get this information is to navigate to a file or folder in the lakehouse, click on the three dots to the right of the file or folder name, and select "Properties". Details will be displayed in the properties window. From these properties select copy url and paste it into a text editor. The workspace id is the first GUID in the URL, the lakehouse id is the second GUID in the URL. In the example below, the workspace id is `4f0cb887-047a-48a1-98c3-ebdb38c784c2` and the lakehouse id is `aa2e5f92-53cc-4ab3-9a54-a6e5b1aeb9a9`.

> https://onelake.dfs.fabric.microsoft.com/4f0cb887-047a-48a1-98c3-ebdb38c784c2/aa2e5f92-53cc-4ab3-9a54-a6e5b1aeb9a9/Files/notebooks


### Create Dbt Project
Once you have taken note of the workspace id, lakehouse id, and workspace name, you can create a new dbt project and configure it to use the dbt-fabricsparknb adapter. To do this, run the code shown below:

!> **Important** Note when asked to select the adapter choose `dbt-fabricksparknb`. During this process you will also be asked for the `workspace id`, `lakehouse id`, and `workspace name`. Use the values you gathered from the Power BI Portal. 


```bash
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

To complete the newly created project you will need to copy some directories from the project called *"ptfproj"* dbt project. Copy *ptfproj/macros/* and *ptfproj/metaextracts/* directories with their files into your new dbt project. Overwrite any directories or files if they exist. Now in metaextracts the file ListSchemas.json contains the lakehouses in your workspace. You can manually update this file.

```json
[{"namespace":"lh_raw"},{"namespace":"lh_conformed"},{"namespace":"lh_consolidated"}]
```

### Create a build python Script

This repo contains a dbt build script created in python. Make a copy of this script from file *ptf_pre_install.py* found in the root. 

```python
import dbt.cli
import dbt.cli.flags
import dbt.config.renderer
import dbt.config.utils
import dbt.parser
import dbt.parser.manifest
import dbt.tests.util
import dbt.utils
import dbt
import dbt.adapters.fabricspark
import dbt.adapters.fabricsparknb
from dbt.adapters.fabricsparknb import utils as utils
import dbt.tests
from pathlib import Path
import os
import shutil

os.environ['DBT_PROJECT_DIR'] = "ptfproj"

profile_path = Path(os.path.expanduser('~')) / '.dbt/'
profile = dbt.config.profile.read_profile(profile_path)
config = dbt.config.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])
profile_info = profile[config['profile']]
target_info = profile_info['outputs'][profile_info['target']]
lakehouse = target_info['lakehouse']


shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
utils.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])

dbt.tests.util.run_dbt(['build'])

utils.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
```

Give it the name prefixed by your new project name. Update the DBT_PROJECT_DIR variable with your project name. Save this file.

You can execute this file:
```bash
python <YourProjectName>_pre_install.py
```

You may get the error:
```text
FileNotFoundError: [WinError 3] The system cannot find the path specified: 'ch_fabric/target'
```

You will need to add a blank folder to your project called *"target"*.

If you get an error with Azure CLI connection issues or type errors. This is because the Profile.yaml file has the incorrect adaptor set. It should be *"fabricsparknb"* not *"fabricspark"*.

After successful execution and number of notebooks have been created in your project/target folder under notebooks. 

*import_notebook.ipynb* this will be used to import notebook files into your lakehouse.

*metadata_extract.ipynb* is used to update the metadata json files in your project. 

These 2 can be imported using the standard import notebooks function in fabric. The rest of the notebooks can be copied into your lakehouse Files/notebooks folder using Onelake explorer. 

You then open the *import_notebook.ipynb* in fabric and *Run All* to import the notebooks from the Files/Notebooks directory in fabric. 

Executing the *master_notebook.ipynb* notebook will execute all notebooks created in your project.

This concludes the Framework setup.