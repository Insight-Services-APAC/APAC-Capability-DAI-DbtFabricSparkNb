import shutil
import os
import dbt.config as dbtconfig
from pathlib import Path
from sysconfig import get_paths
import importlib.util
import sys
import subprocess
import dbt_wrapper.utils as mn
import dbt_wrapper.generate_files as gf
import dbt_wrapper.fabric_api as fa


@staticmethod
def PrintFirstTimeRunningMessage():
    print('\033[1;33;48m', "It seems like this is the first time you are running this project. Please update the metadata extract json files in the metaextracts directory by performing the following steps:")
    print(f"1. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/upload.ps1")
    print("2. Login to the Fabric Portal and navigate to the workspace and lakehouse you are using")
    print(f"3. Manually upload the following notebook to your workspace: {os.environ['DBT_PROJECT_DIR']}/target/notebooks/import_{os.environ['DBT_PROJECT_DIR']}_notebook.ipynb. See https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks")
    print("4. Open the notebook in the workspace and run all cells. This will upload the generated notebooks to your workspace.")
    print(f"5. A new notebook should appear in the workspace called metadata_{os.environ['DBT_PROJECT_DIR']}_extract.ipynb. Open this notebook and run all cells. This will generate the metadata extract json files in the metaextracts directory.")
    print(f"6. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/download.ps1. This will download the metadata extract json files to the metaextracts directory.")
    print("7. Re-run this script to generate the model and master notebooks.")


@staticmethod
def RunDbtProject(PreInstall=False, Upload=False):
    # Get Config and Profile Information from dbt

    if (os.environ.get('DBT_PROFILES_DIR') is not None):
        profile_path = Path(os.environ['DBT_PROFILES_DIR'])
        print(profile_path)
    else:
        profile_path = Path(os.path.expanduser('~')) / '.dbt/'
    
    if (PreInstall is True): 
        if mn.PureLibIncludeDirExists():
            raise Exception('When running pre-install development version please uninstall the pip installation by running : `pip uninstall dbt-fabricsparknb` before continuing')

    profile = dbtconfig.profile.read_profile(profile_path)
    config = dbtconfig.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])
    profile_info = profile[config['profile']]
    target_info = profile_info['outputs'][profile_info['target']]    
    lakehouse = target_info['lakehouse']

    if os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/target"):
        shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
    # Generate AzCopy Scripts and Metadata Extract Notebooks
    gf.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])
    if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks"):
        os.makedirs(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks")

    gf.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
    gf.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])

    # count files in metaextracts directory
    if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/metaextracts"):
        PrintFirstTimeRunningMessage()
    elif len(os.listdir(os.environ['DBT_PROJECT_DIR'] + "/metaextracts")) == 0:
        PrintFirstTimeRunningMessage()
    else:
        if (PreInstall is True):
            # make sure we are using the installed dbt version
            utilpath = Path(get_paths()['purelib']) / Path('dbt/tests/util.py')
            spec = importlib.util.spec_from_file_location("util.name", utilpath)
            foo = importlib.util.module_from_spec(spec)
            sys.modules["module.name"] = foo
            spec.loader.exec_module(foo)
            foo.run_dbt(['build'])
        else:
            # Call dbt build
            subprocess.run(["dbt", "build"], check=True)
            # Generate Model Notebooks and Master Notebooks
        
        gf.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
        gf.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
        curr_dir = os.getcwd()
        dbt_project_dir = os.path.join(curr_dir, os.environ['DBT_PROJECT_DIR'])
        fa.IPYNBtoFabricPYFile(dbt_project_dir)
        if (Upload):
            fa.APIUpsertNotebooks(dbt_project_dir, target_info['workspaceid'])
