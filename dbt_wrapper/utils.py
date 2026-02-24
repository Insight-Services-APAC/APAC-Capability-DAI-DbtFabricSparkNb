
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


def UploadFile(progress: ProgressConsoleWrapper, task_id, directory_client: DataLakeDirectoryClient, local_file_path: str, remote_file_name: str):
    """Upload a file to OneLake lakehouse"""
    file_client = directory_client.get_file_client(remote_file_name)
    with open(file=local_file_path, mode="rb") as local_file:
        file_client.upload_data(local_file.read(), overwrite=True)
        progress.progress.update(task_id=task_id, description=f"Uploaded {remote_file_name}")


def DownloadFile(progress: ProgressConsoleWrapper, task_id, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):

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
    progress.progress.update(task_id=task_id, description=f"Listing metaextract files for path {directory_name}")
    paths = file_system_client.get_paths(path=directory_name)    
    for path in paths:
        progress.progress.print(f"Found file: {path.name}")
        if (path.name[-5:] == ".json"):
            DownloadFile(progress, task_id, file_system_client, local_notebook_path, path.name)
            


@staticmethod
def UploadFileToLakehouse(progress: ProgressConsoleWrapper, task_id, workspace_name: str, lakehouse_name: str, local_file_path: str, remote_path: str):
    """Upload a single file to OneLake lakehouse

    Args:
        progress: Progress console wrapper for status updates
        task_id: Task identifier for progress tracking
        workspace_name: Name of the Fabric workspace
        lakehouse_name: Name of the lakehouse
        local_file_path: Local path to the file to upload
        remote_path: Remote path within the lakehouse Files directory
    """
    progress.progress.update(task_id=task_id, description=f"Uploading {os.path.basename(local_file_path)} to lakehouse...")
    account_name = "onelake"  # always this
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    try:
        token_credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url, credential=token_credential)
        # File system is workspace name, directory path is lakehouse_name.Lakehouse/Files/...
        file_system_client = service_client.get_file_system_client(workspace_name)
        # Construct the full path: lakehouse_name.Lakehouse/Files (OneLake naming convention)
        directory_path = f"{lakehouse_name}.Lakehouse/Files"
        directory_client = file_system_client.get_directory_client(directory_path)

        # Upload the file
        UploadFile(progress, task_id, directory_client, local_file_path, remote_path)
        progress.progress.update(task_id=task_id, description=f"Completed upload of {os.path.basename(local_file_path)}")
    except Exception as e:
        progress.progress.print(f"Error uploading file: Workspace: {workspace_name}, Lakehouse: {lakehouse_name}, LocalFile: {local_file_path}, RemotePath: {remote_path}")
        raise e


@staticmethod
def DownloadMetaFiles(progress: ProgressConsoleWrapper, task_id, dbt_project_dir, workspacename: str, datapath: str):
    progress.progress.update(task_id=task_id, description="Connecting to one drive to download meta extracts...")
    account_name = "onelake"  # always this
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    local_notebook_path = str(Path(Path(dbt_project_dir) / Path('metaextracts')))
    try:
        token_credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url, credential=token_credential)
        file_system_client = service_client.get_file_system_client(workspacename)
        DownloadFiles(progress, task_id, file_system_client, datapath, local_notebook_path)
        progress.progress.update(task_id=task_id, description="Completed download of meta extracts")
    except Exception as e:
        progress.progress.print(f"Error downloading meta extracts: Workspacename: {workspacename}, DataPath: {datapath}, LocalPath: {local_notebook_path}")
        raise e

