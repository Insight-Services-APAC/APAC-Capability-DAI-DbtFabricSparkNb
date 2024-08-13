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
from dbt_wrapper.fabric_api import FabricAPI as fa
import dbt_wrapper.fabric_sql as fas
from dbt_wrapper.log_levels import LogLevel
from dbt_wrapper.stage_executor import ProgressConsoleWrapper
from rich import print
from rich.panel import Panel



class Commands:
    def __init__(self, console):
        self.console = console
        self.profile = None
        self.config = None
        self.profile_info = None
        self.target_info = None
        self.lakehouse = None
        self.workspaceid = None
        self.project_root = None
        self.fa = fa(console=self.console)
    
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
        self.dbt_project_dir = dbt_project_dir
        if (dbt_profiles_dir is not None):
            os.environ["DBT_PROFILES_DIR"] = dbt_profiles_dir
            
        if (os.environ.get('DBT_PROFILES_DIR') is not None):
            profile_path = Path(os.environ['DBT_PROFILES_DIR'])
            self.console.print(profile_path, style="debug")
        else:
            profile_path = Path(os.path.expanduser('~')) / '.dbt/'
        
        self.profile = dbtconfig.profile.read_profile(profile_path)
        self.config = dbtconfig.project.load_raw_project(self.dbt_project_dir)
        self.profile_info = self.profile[self.config['profile']]
        self.target_info = self.profile_info['outputs'][self.profile_info['target']]
        self.lakehouse = self.target_info['lakehouse']
        if "sql_endpoint" in self.target_info.keys():
            self.sql_endpoint = self.target_info['sql_endpoint']
        else: 
            self.sql_endpoint = None

        self.project_name = self.config['name']
        #self.workspaceid = self.config['workspaceid']
       
    def PrintFirstTimeRunningMessage(self):
        print('\033[1;33;48m', "Error!")
        print(f"Directory ./{os.environ['DBT_PROJECT_DIR']}/metaextracts/ does not exist and should have been created automatically.")

    def GeneratePreDbtScripts(self, PreInstall, progress: ProgressConsoleWrapper, task_id, lakehouse_config):        
        gf.GenerateMetadataExtract(self.dbt_project_dir, self.target_info['workspaceid'], self.target_info['lakehouseid'], self.lakehouse, self.config['name'], progress=progress, task_id=task_id, lakehouse_config=lakehouse_config)
        gf.GenerateNotebookUpload(self.dbt_project_dir, self.target_info['workspaceid'], self.target_info['lakehouseid'], self.lakehouse, self.config['name'], progress=progress, task_id=task_id, lakehouse_config=lakehouse_config)
        
        gf.GenerateAzCopyScripts(self.dbt_project_dir, self.target_info['workspaceid'], self.target_info['lakehouseid'], progress=progress, task_id=task_id)
    
    def GeneratePostDbtScripts(self, PreInstall=False, progress=None, task_id=None, notebook_timeout=None, log_lakehouse=None, notebook_hashcheck=None, lakehouse_config=None): 
        try:
            log_lakehouse = self.target_info['log_lakehouse']
        except KeyError:
            log_lakehouse = self.lakehouse

        gf.SetSqlVariableForAllNotebooks(self.dbt_project_dir, self.lakehouse, progress=progress, task_id=task_id, lakehouse_config=lakehouse_config)
        gf.GenerateMasterNotebook(self.dbt_project_dir, self.target_info['workspaceid'], self.target_info['lakehouseid'], self.lakehouse, self.config['name'], progress=progress, task_id=task_id, notebook_timeout=notebook_timeout,max_worker = self.target_info['threads'], log_lakehouse=log_lakehouse, notebook_hashcheck=notebook_hashcheck, lakehouse_config=lakehouse_config)
    
    def ConvertNotebooksToFabricFormat(self, progress: ProgressConsoleWrapper, task_id=None):
        curr_dir = os.getcwd()
        dbt_project_dir = os.path.join(curr_dir, self.dbt_project_dir)
        self.fa.IPYNBtoFabricPYFile(dbt_project_dir=dbt_project_dir, progress=progress, task_id=task_id, workspace_id=self.target_info['workspaceid'], lakehouse_id=self.target_info['lakehouseid'], lakehouse=self.lakehouse)
    
    def CleanProjectTargetDirectory(self, progress: ProgressConsoleWrapper, task_id):
        if os.path.exists(self.dbt_project_dir + "/target"):
            shutil.rmtree(self.dbt_project_dir + "/target")
        # Generate AzCopy Scripts and Metadata Extract Notebooks
        
        if not os.path.exists(self.dbt_project_dir + "/target/notebooks"):
            os.makedirs(self.dbt_project_dir + "/target/notebooks")

    def AutoUploadNotebooksViaApi(self, progress: ProgressConsoleWrapper, task_id):
        curr_dir = os.getcwd()
        dbt_project_dir = os.path.join(curr_dir, self.dbt_project_dir)
        self.fa.APIUpsertNotebooks(progress=progress, task_id=task_id, dbt_project_dir=dbt_project_dir, workspace_id=self.target_info['workspaceid'])

    def BuildDbtProject(self, PreInstall=False, select="", exclude=""):
        print(Panel.fit("[blue]<<<<<<<<<<<<<<<<<<<<<<< Start of dbt build[/blue]"))
        # Check if PreInstall is True
        if (PreInstall is True):
            if mn.PureLibIncludeDirExists():
                raise Exception('When running pre-install development version please uninstall the pip installation by running : `pip uninstall dbt-fabricsparknb` before continuing')

        # count files in metaextracts directory
        if not os.path.exists(self.dbt_project_dir + "/metaextracts"):
            self.PrintFirstTimeRunningMessage()
        elif len(os.listdir(self.dbt_project_dir + "/metaextracts")) == 0:
            self.PrintFirstTimeRunningMessage()
        else:
            if (PreInstall is True):
                # make sure we are using the installed dbt version
                utilpath = Path(get_paths()['purelib']) / Path('dbt/tests/util.py')
                spec = importlib.util.spec_from_file_location("util.name", utilpath)
                foo = importlib.util.module_from_spec(spec)
                sys.modules["module.name"] = foo
                spec.loader.exec_module(foo)              
                Buildarr = ['build']
                if (len(select.strip()) > 0):
                    Buildarr.append('--select')
                    Buildarr.append(select)
                if (len(exclude.strip()) > 0):
                    Buildarr.append('--exclude')
                    Buildarr.append(exclude)

                foo.run_dbt(Buildarr)

            else:
                # Call dbt build     
                Buildarr = ['dbt', 'build']
                if (len(select.strip()) > 0):
                    Buildarr.append('--select')
                    Buildarr.append(select)
                if (len(exclude.strip()) > 0):
                    Buildarr.append('--exclude')
                    Buildarr.append(exclude)

                result = subprocess.run(Buildarr, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # Access the output and error
                output = result.stdout.decode('utf-8')
                error = result.stderr.decode('utf-8')

                self.console.print(f"Output: {output}", style="info")
                if error:
                    self.console.print(f"Error: {error}", style="error")
        print(Panel.fit("[blue]End of dbt build >>>>>>>>>>>>>>>>>>>>>>>[/blue]"))

    def DownloadMetadata(self, progress: ProgressConsoleWrapper, task_id):
        progress.print("Downloading Metadata", level=LogLevel.INFO)
        curr_dir = os.getcwd()
        dbt_project_dir = str(Path(Path(curr_dir) / Path(self.dbt_project_dir)))
        mn.DownloadMetaFiles(progress=progress, task_id=task_id, dbt_project_dir=dbt_project_dir, workspacename=self.target_info['workspaceid'], datapath=self.target_info['lakehouseid'] + "/Files/MetaExtracts/")

    def RunMetadataExtract(self, progress: ProgressConsoleWrapper, task_id):
        nb_name = f"metadata_{self.project_name}_extract"
        nb_id = self.fa.GetNotebookIdByName(workspace_id=self.target_info['workspaceid'], notebook_name=nb_name)
        if (1==1):
            progress.print("Metadata Extract Notebook Not Found in Workspace. Uploading Notebook Now", level=LogLevel.INFO)
            self.fa.APIUpsertNotebooks(progress=progress, task_id=task_id, dbt_project_dir=self.dbt_project_dir, workspace_id=self.target_info['workspaceid'], notebook_name=nb_name)
        else: 
            progress.print("Metadata Extract Notebook Found in Workspace.", level=LogLevel.INFO)            
        progress.print("Running Metadata Extract", LogLevel.INFO)
        self.fa.APIRunNotebook(progress=progress, task_id=task_id, workspace_id=self.target_info['workspaceid'], notebook_name=f"metadata_{self.project_name}_extract")

    def RunMasterNotebook(self, progress: ProgressConsoleWrapper, task_id):
        nb_name = f"master_{self.project_name}_notebook"
        self.fa.APIRunNotebook(progress=progress, task_id=task_id, workspace_id=self.target_info['workspaceid'], notebook_name=nb_name)

    def GetExecutionResults(self, progress: ProgressConsoleWrapper, task_id):
        if self.sql_endpoint is not None:
            sql = f"""
                Select a.notebook, replace(CONVERT(varchar(20), DATEADD(second, a.start_time, '1970/01/01 00:00:00'),126), 'T',' ') start_time, status, error
                from {self.lakehouse}.dbo.execution_log a 
                join 
                (
                Select top 1 batch_id, max(DATEADD(second, start_time, '1970/01/01 00:00:00')) start_time  
                from {self.lakehouse}.dbo.execution_log  
                group by batch_id 
                order by start_time desc
                ) b on a.batch_id = b.batch_id
                where a.status = 'error'
            """
            _fas = fas.FabricApiSql(console=self.console, server=self.sql_endpoint, database=self.lakehouse)
            _fas.ExecuteSQL(sql=sql, progress=progress, task_id=task_id)
        else: 
            progress.print("SQL Endpoint not found in profile. Skipping Execution Results", level=LogLevel.WARNING)

