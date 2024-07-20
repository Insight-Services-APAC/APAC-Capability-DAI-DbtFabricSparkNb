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


class Commands:
    def __init__(self, console):
        self.console = console
        self.profile = None
        self.config = None
        self.profile_info = None
        self.target_info = None
        self.lakehouse = None
        self.project_root = None
    
    def GetDbtConfigs(self, dbt_project_dir, dbt_profiles_dir=None):
        if len(dbt_project_dir.replace("\\", "/").split("/")) > 1:
            self.console.print(
                "Warning: :file_folder: The dbt_project_dir provided is nested and not a valid dbt project directory in windows. Copying the dbt_project_dir to the samples_tests directory.",
                style="warning",
            )
            old_dbt_project_dir = dbt_project_dir.replace("\\", "/")
            dbt_project_dir = "samples_tests"
            if os.path.exists(dbt_project_dir):
                shutil.rmtree(dbt_project_dir)
            shutil.copytree(old_dbt_project_dir, dbt_project_dir)
        os.environ["DBT_PROJECT_DIR"] = dbt_project_dir
        if (dbt_profiles_dir is not None):
            os.environ["DBT_PROFILES_DIR"] = dbt_profiles_dir
            
        if (os.environ.get('DBT_PROFILES_DIR') is not None):
            profile_path = Path(os.environ['DBT_PROFILES_DIR'])
            print(profile_path)
        else:
            profile_path = Path(os.path.expanduser('~')) / '.dbt/'
        
        self.profile = dbtconfig.profile.read_profile(profile_path)
        self.config = dbtconfig.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])
        self.profile_info = self.profile[self.config['profile']]
        self.target_info = self.profile_info['outputs'][self.profile_info['target']]
        self.lakehouse = self.target_info['lakehouse']
        self.project_name = self.config['name']
        self.dbt_project_dir = dbt_project_dir
        
    def PrintFirstTimeRunningMessage(self):
        print('\033[1;33;48m', "It seems like this is the first time you are running this project. Please update the metadata extract json files in the metaextracts directory by performing the following steps:")
        print(f"1. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/upload.ps1")
        print("2. Login to the Fabric Portal and navigate to the workspace and lakehouse you are using")
        print(f"3. Manually upload the following notebook to your workspace: {os.environ['DBT_PROJECT_DIR']}/target/notebooks/import_{os.environ['DBT_PROJECT_DIR']}_notebook.ipynb. See https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks")
        print("4. Open the notebook in the workspace and run all cells. This will upload the generated notebooks to your workspace.")
        print(f"5. A new notebook should appear in the workspace called metadata_{os.environ['DBT_PROJECT_DIR']}_extract.ipynb. Open this notebook and run all cells. This will generate the metadata extract json files in the metaextracts directory.")
        print(f"6. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/download.ps1. This will download the metadata extract json files to the metaextracts directory.")
        print("7. Re-run this script to generate the model and master notebooks.")

    def GeneratePreDbtScripts(self, PreInstall=False):
        gf.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], self.target_info['workspaceid'], self.target_info['lakehouseid'], self.lakehouse, self.config['name'])
        gf.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], self.target_info['workspaceid'], self.target_info['lakehouseid'], self.lakehouse, self.config['name'])
        gf.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], self.target_info['workspaceid'], self.target_info['lakehouseid'])
    
    def GeneratePostDbtScripts(self, PreInstall=False):         
        gf.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], self.lakehouse)
        gf.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], self.target_info['workspaceid'], self.target_info['lakehouseid'], self.lakehouse, self.config['name'])
    
    def ConvertNotebooksToFabricFormat(self):
        curr_dir = os.getcwd()
        dbt_project_dir = os.path.join(curr_dir, os.environ['DBT_PROJECT_DIR'])
        fa.IPYNBtoFabricPYFile(dbt_project_dir)
    
    def CleanProjectTargetDirectory(self):
        if os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/target"):
            shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
        # Generate AzCopy Scripts and Metadata Extract Notebooks
        
        if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks"):
            os.makedirs(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks")

    def AutoUploadNotebooksViaApi(self):
        curr_dir = os.getcwd()
        dbt_project_dir = os.path.join(curr_dir, os.environ['DBT_PROJECT_DIR'])
        fa.APIUpsertNotebooks(dbt_project_dir, self.target_info['workspaceid'])

    def BuildDbtProject(self, PreInstall=False):    
        # Check if PreInstall is True
        if (PreInstall is True):
            if mn.PureLibIncludeDirExists():
                raise Exception('When running pre-install development version please uninstall the pip installation by running : `pip uninstall dbt-fabricsparknb` before continuing')

        # count files in metaextracts directory
        if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/metaextracts"):
            self.PrintFirstTimeRunningMessage()
        elif len(os.listdir(os.environ['DBT_PROJECT_DIR'] + "/metaextracts")) == 0:
            self.PrintFirstTimeRunningMessage()
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

    def DownloadMetadata(self):
        print("Downloading Metadata via Azcopy")
        subprocess.run(["pwsh", f"./{self.dbt_project_dir}/target/pwsh/download.ps1"], check=True)

    def RunMetadataExtract(self):
        print("Running Metadata Extract")
        fa.APIRunNotebook(self.target_info['workspaceid'], f"metadata_{self.project_name}_extract")
