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


