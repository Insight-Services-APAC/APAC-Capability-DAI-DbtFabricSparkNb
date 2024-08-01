
import os
from pathlib import Path
from sysconfig import get_paths
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from dbt_wrapper.stage_executor import ProgressConsoleWrapper


@staticmethod
def PureLibIncludeDirExists():
    ChkPath = Path(get_paths()['purelib']) / Path('dbt/include/fabricsparknb/')
    return os.path.exists(ChkPath)


@staticmethod
def GetIncludeDir():
    ChkPath = Path(get_paths()['purelib']) / Path('dbt/include/fabricsparknb/')
    # print(ChkPath)
    # Does Check for the path
    if os.path.exists(ChkPath):
        return ChkPath
    else:
        path = Path(os.getcwd()) / Path('dbt/include/fabricsparknb/')
        # print(str(path))
        return (path)


def DownloadFile(progress: ProgressConsoleWrapper, task_id, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    
    print(f"File Found: {file_name}")
    
    file_client = directory_client.get_file_client(file_name)
    file_name_only = file_name.split('/')[-1]  #One drive path
    writepath = str(Path(Path(local_path) / Path(file_name_only)))
    Path(local_path).mkdir(parents=True, exist_ok=True)  #ensure directory exists
    with open(file=writepath, mode="wb") as local_file:
        download = file_client.download_file()
        local_file.write(download.readall())
        local_file.close()
        progress.progress.update(task_id=task_id, description="Downloaded "+file_name_only)


def DownloadFiles(progress: ProgressConsoleWrapper, task_id, file_system_client: FileSystemClient, directory_name: str, local_notebook_path: str):
    progress.progress.update(task_id=task_id, description="Listing metaextract files on one drive...")
    paths = file_system_client.get_paths(path=directory_name)
    for path in paths:
        if (path.name[-5:] == ".json"):
            DownloadFile(progress, task_id, file_system_client, local_notebook_path, path.name)
            


@staticmethod
def DownloadMetaFiles(progress: ProgressConsoleWrapper, task_id, dbt_project_dir, workspacename: str, datapath: str):
    progress.progress.update(task_id=task_id, description="Connecting to one drive to download meta extracts...") 
    account_name = "onelake"  # always this
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    local_notebook_path = str(Path(Path(dbt_project_dir) / Path('metaextracts')))
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    file_system_client = service_client.get_file_system_client(workspacename)
    DownloadFiles(progress, task_id, file_system_client, datapath, local_notebook_path)
    progress.progress.update(task_id=task_id, description="Completed download of meta extracts")
 
