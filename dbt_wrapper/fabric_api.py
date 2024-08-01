from pathlib import Path
import time
import uuid
from msfabricpysdkcore import FabricClientCore
import base64
import os
import json
import typer
import hashlib
from rich.progress import Progress, SpinnerColumn, TextColumn
from dbt_wrapper.log_levels import LogLevel
from dbt_wrapper.stage_executor import ProgressConsoleWrapper

class FabricAPI:
    def __init__(self, console):
        self.console = console

    def GetFabricPlatformContent(self, displayName):
        logicalId = str(uuid.uuid4())

        platformcontent = """{
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {
        "type": "Notebook",
        "displayName": \"""" + displayName + """\",
        "description": "New notebook"
    },
    "config": {
        "version": "2.0",
        "logicalId": \"""" + logicalId + """\"
    }
    }"""
        return platformcontent

    def remove_last_line(self, py_fabric_file: str):
        with open(py_fabric_file, "r+", encoding="utf-8") as file:
            lines = file.readlines()
            file.seek(0)
            file.truncate()
            file.writelines(lines[:-1])

    # Generate py files for api update
    def IPYNBtoFabricPYFile(self, dbt_project_dir, progress, task_id):
        progress.update(task_id=task_id, description=f"Converting notebooks to Fabric PY format")
        target_dir = str(Path(dbt_project_dir) / Path("target"))
        notebooks_dir = str(Path(target_dir) / Path("notebooks"))
        notebooks_fabric_py_dir = str(Path(target_dir) / Path("notebooks_fabric_py"))
        os.makedirs(notebooks_fabric_py_dir, exist_ok=True)
        list_of_notebooks = os.listdir(notebooks_dir)
        for filename in list_of_notebooks:
            filenamewithoutext = filename[:-6]  # # remove .ipynb
            py_fabric_file = str(Path(notebooks_fabric_py_dir) / Path(filenamewithoutext + ".py"))
            # path = dbt_project_dir
            FabricPlatformContent = self.GetFabricPlatformContent(filenamewithoutext)          
            with open(py_fabric_file, "w", encoding="utf-8") as python_file:
                python_file.write("# Fabric notebook source\n\n")
                python_file.write("# METADATA ********************\n\n")
                python_file.write("# META {\n")
                python_file.write("# META   \"kernel_info\": {\n")
                python_file.write("# META     \"name\": \"synapse_pyspark\"\n")
                python_file.write("# META   }\n")
                python_file.write("# META }\n\n")
                f = open(Path(notebooks_dir) / Path(filename), "r", encoding="utf-8")
                data = json.loads(f.read())
                for cell in data['cells']:
                    if (cell["cell_type"] == "code"):
                        ParamCell = False
                        try:
                            for tag in cell["metadata"]["tags"]:
                                if (tag == "parameters"):
                                    ParamCell = True
                                    break
                        except:
                            pass
                        
                        if (cell["source"][0][:5] == "%%sql"):
                            if (ParamCell == False):
                                python_file.write("# CELL ********************\n\n")
                            else: 
                                python_file.write("# PARAMETERS CELL ********************\n\n")
                            for sourceline in cell['source']:
                                line = "# MAGIC "+ sourceline
                                python_file.write(line)
                            python_file.write("\n\n")
                            python_file.write("# METADATA ********************\n\n")
                            python_file.write("# META {\n")
                            python_file.write("# META   \"language\": \"sparksql\",\n")
                            python_file.write("# META   \"language_group\": \"synapse_pyspark\"\n")
                            python_file.write("# META }\n\n")                   
                        elif (cell["source"][0][:11] == "%%configure"):
                            if (ParamCell == False):
                                python_file.write("# CELL ********************\n\n")
                            else:
                                python_file.write("# PARAMETERS CELL ********************\n\n")
                            for sourceline in cell['source']:
                                line = "# MAGIC " + sourceline
                                python_file.write(line)
                            python_file.write("\n\n")
                            python_file.write("# METADATA ********************\n\n")
                            python_file.write("# META {\n")
                            python_file.write("# META   \"language\": \"python\",\n")
                            python_file.write("# META   \"language_group\": \"synapse_pyspark\"\n")
                            python_file.write("# META }\n\n")
                        elif (cell["source"][0][:2] == "%%"):
                            if (ParamCell == False):
                                python_file.write("# CELL ********************\n\n")
                            else: 
                                python_file.write("# PARAMETERS CELL ********************\n\n")
                            for sourceline in cell['source']:
                                line = "# MAGIC "+ sourceline
                                python_file.write(line)
                            python_file.write("\n\n")
                        else:
                            if (ParamCell == False):
                                python_file.write("# CELL ********************\n\n")
                            else: 
                                python_file.write("# PARAMETERS CELL ********************\n\n")
                            for sourceline in cell['source']:
                                python_file.write(sourceline)
                            python_file.write("\n\n")
                            python_file.write("# METADATA ********************\n\n")
                            python_file.write("# META {\n")
                            python_file.write("# META   \"language\": \"python\",\n")
                            python_file.write("# META   \"language_group\": \"synapse_pyspark\"\n")
                            python_file.write("# META }\n\n")
                    elif (cell["cell_type"] == "markdown"):
                        python_file.write("# MARKDOWN ********************\n\n")
                        for sourceline in cell['source']:
                            line = "# " + sourceline
                            python_file.write(line)
                        python_file.write("\n\n")
                    
            self.remove_last_line(py_fabric_file)
            progress.update(task_id=task_id, description=f"Completed fabric py conversion for " + filenamewithoutext)                    
        progress.update(task_id=task_id, description=f"Completed all Fabric PY conversions saved to : " + notebooks_fabric_py_dir)

    def stringToBase64(self, s):
        return base64.b64encode(s.encode('utf-8')).decode('utf-8')

    def base64ToString(self, b):
        return base64.b64decode(b).decode('utf-8')

    def GenerateNotebookContent(self, notebookcontentBase64):
        notebook_w_content = {'parts': [{'path': 'notebook-content.py', 'payload': notebookcontentBase64, 'payloadType': 'InlineBase64'}]}
        return notebook_w_content

    def findnotebookid(self, notebooks, displayname):
        for notebook in notebooks:
            if notebook.display_name == displayname:
                return notebook.id
        return -1
    
    ##Issue 87 - Added function to check if the hash value of the notebook definition against the value stored in the description
    def NotebookHashCheck(self, notebooks, displayname, notebookhash):
        for notebook in notebooks:
            if notebook.display_name == displayname:
                if notebookhash in notebook.description:
                    return 1
                    break
        return -1
    
    def APIUpsertNotebooks(self, progress: ProgressConsoleWrapper, task_id, dbt_project_dir, workspace_id, notebook_name=None):
        progress.progress.update(task_id=task_id, description="Logging in... Make sure you use `az login` to authenticate before running for the first time")
        progress.print("Uploading notebooks via API ...", level=LogLevel.INFO)
        target_dir = str(Path(dbt_project_dir) / Path("target"))
        notebooks_fabric_py_dir = os.getcwd() / Path(target_dir) / Path("notebooks_fabric_py")        
        fc = FabricClientCore(silent=True)
        workspace = fc.get_workspace_by_id(id=workspace_id)
        workspace_id = workspace.id
        servernotebooks = fc.list_notebooks(workspace_id)
        list_of_notebooks = os.listdir(notebooks_fabric_py_dir)
        if (notebook_name is not None):
            list_of_notebooks = [notebook for notebook in list_of_notebooks if notebook[:-3] == notebook_name]

        for filename in list_of_notebooks:
            with open(Path(notebooks_fabric_py_dir) / Path(filename), 'r', encoding="utf8") as file:
                notebookcontent = file.read()
                notebookname = filename[:-3]  # # remove .py
                notebookcontentBase64 = self.stringToBase64(notebookcontent)
                notebook_w_content_new = self.GenerateNotebookContent(notebookcontentBase64) 

                ##Issue 87 - Create a hash value of the notebook contents and compare it to value stored in description
                notebook_w_content_new_str = json.dump(notebook_w_content_new)
                notebookhashvalue = hashlib.sha256(notebook_w_content_new_str.encode()).hexdigest()
                notebookhashcheck = self.NotebookHashCheck(servernotebooks, notebookname, notebookhashvalue)

                # notebook_w_content = fc.get_notebook(workspace_id, notebook_name=notebookname)
                notebookid = self.findnotebookid(servernotebooks, notebookname)
                if notebookid == -1:
                    notebook = fc.create_notebook(workspace_id, definition=notebook_w_content_new, display_name=notebookname, description="Notebook Hash:" + notebookhashvalue)
                    progress.progress.update(task_id=task_id, description="Notebook created " + notebookname)
                else:
                    if notebookhashcheck == -1:
                        notebook2 = fc.update_notebook_definition(workspace_id, notebookid, definition=notebook_w_content_new, description="Notebook Hash:" + notebookhashvalue)
                        progress.progress.update(task_id=task_id, description="Notebook updated " + notebookname)
        progress.progress.update(task_id=task_id, description="Completed uploading notebooks via API")

    def GetNotebookIdByName(self, workspace_id, notebook_name):
        fc = FabricClientCore(silent=True)
        workspace = fc.get_workspace_by_id(id=workspace_id)
        workspace_id = workspace.id
        ws_items = fc.list_items(workspace_id)
        for item in ws_items:
            if item.type == 'Notebook' and item.display_name == notebook_name:
                return item.id
        return None
    
    def GetWorkspaceName(self, workspace_id):
        fc = FabricClientCore(silent=True)
        workspace = fc.get_workspace_by_id(id=workspace_id)
        return workspace.display_name

    def APIRunNotebook(self, progress: ProgressConsoleWrapper, task_id, workspace_id, notebook_name):
        fc = FabricClientCore(silent=True)
        workspace = fc.get_workspace_by_id(id=workspace_id)
        workspace_id = workspace.id
        ws_items = fc.list_items(workspace_id)
        item_found = False
        for item in ws_items:         
            if item.type == 'Notebook' and item.display_name == notebook_name:
                item_found = True
                nbfailed = False
                try: 
                    progress.progress.update(task_id=task_id, description=f"Running {item.display_name}")
                    start = time.time()                
                    ji = fc.run_on_demand_item_job(workspace_id=workspace_id, item_id=item.id, job_type="RunNotebook")
                    progress.progress.update(task_id=task_id, description=f"Running {item.display_name}")
                    time.sleep(10)
                    while ji.status == "InProgress" or ji.status == "NotStarted" or ji.status == "Failed":
                        ji = fc.get_item_job_instance(workspace_id=workspace_id, item_id=item.id, job_instance_id=ji.id)
                        if (ji.status == "Failed"):
                            nbfailed = True
                            break
                        # update progress with total runtime
                        runtime = time.time() - start
                        runtime_str = time.strftime("%H:%M:%S", time.gmtime(runtime))
                        progress.progress.update(task_id=task_id, description=f"Running {item.display_name} - {ji.status} - Total Runtime: {runtime_str}")
                        # wait for 10 seconds
                        time.sleep(10)
                    if (nbfailed is False):  
                        progress.print(f"Notebook execution of {item.display_name} completed with status {ji.status}", level=LogLevel.INFO)
                    else:
                        progress.print(f"Error running notebook {item.display_name}", level=LogLevel.ERROR)
                except Exception as e:
                    progress.print(f"Error running notebook {item.display_name} - {e}", level=LogLevel.ERROR)
                break
        if not item_found:
            progress.print(f"Notebook {notebook_name} not found in workspace {workspace_id}", level=LogLevel.ERROR)


# @staticmethod

### Generate py files and.platform files for git direct deployment
#def IPYNBtoFabricPYFileAndGitStructure(dbt_project_dir):
#    print("Converting notebooks to Fabric PY format")
#    target_dir = os.path.join(dbt_project_dir,"target")
#    notebooks_dir = os.path.join(target_dir,"notebooks")
#    list_of_notebooks = os.listdir(notebooks_dir)
#    for filename in list_of_notebooks:
#        filenamewithoutext = filename[:-6]  ## remove .ipynb
#        notebooks_fabric_py_dir = os.path.join(target_dir,"notebooks_fabric_py_git")
#        notebook_file_fabric_py_dir = os.path.join(notebooks_fabric_py_dir,filenamewithoutext+".Notebook")
#        py_fabric_file = os.path.join(notebook_file_fabric_py_dir,"notebook-content.py")
#        platform_file = os.path.join(notebook_file_fabric_py_dir,".platform")
#        os.makedirs(notebook_file_fabric_py_dir, exist_ok=True)
#        path = dbt_project_dir
#        os.makedirs(path, exist_ok=True)
#        with open(platform_file, "w", encoding="utf-8") as platform_config_file:            
#            FabricPlatformContent = GetFabricPlatformContent(filenamewithoutext)          
#            platform_config_file.write(FabricPlatformContent)
#            with open(py_fabric_file, "w", encoding="utf-8") as python_file:
#                python_file.write("# Fabric notebook source\n\n")
#                python_file.write("# METADATA ********************\n\n")
#                python_file.write("# META {\n")
#                python_file.write("# META   \"kernel_info\": {\n")
#                python_file.write("# META     \"name\": \"synapse_pyspark\"\n")
#                python_file.write("# META   }\n")
#                python_file.write("# META }\n\n")


#                f = open (filename, "r", encoding="utf-8") 
#                data = json.loads(f.read())
#                for cell in data['cells']:
#                    if (cell["cell_type"] == "code"):
#                        if (cell["source"][0][:5] == "%%sql"):
#                            python_file.write("# CELL ********************\n\n")
#                            for sourceline in cell['source']:
#                                line = "# MAGIC "+ sourceline
#                                python_file.write(line)
#                            python_file.write("\n\n")
#                            python_file.write("# METADATA ********************\n\n")
#                            python_file.write("# META {\n")
#                            python_file.write("# META   \"language\": \"sparksql\",\n")
#                            python_file.write("# META   \"language_group\": \"synapse_pyspark\"\n")
#                            python_file.write("# META }\n\n")                   
#                        elif (cell["source"][0][:11] == "%%configure"):
#                            python_file.write("# CELL ********************\n\n")
#                            for sourceline in cell['source']:
#                                line = "# MAGIC "+ sourceline
#                                python_file.write(line)
#                            python_file.write("\n\n")
#                            python_file.write("# METADATA ********************\n\n")
#                            python_file.write("# META {\n")
#                            python_file.write("# META   \"language\": \"python\",\n")
#                            python_file.write("# META   \"language_group\": \"synapse_pyspark\"\n")
#                            python_file.write("# META }\n\n")
#                        elif (cell["source"][0][:2] == "%%"):
#                            python_file.write("# CELL ********************\n\n")
#                            for sourceline in cell['source']:
#                                line = "# MAGIC "+ sourceline
#                                python_file.write(line)
#                            python_file.write("\n\n")
#                        else:
#                            python_file.write("# CELL ********************\n\n")
#                           for sourceline in cell['source']:
#                                python_file.write(sourceline)
#                            python_file.write("\n\n")
#                            python_file.write("# METADATA ********************\n\n")
#                            python_file.write("# META {\n")
#                            python_file.write("# META   \"language\": \"python\",\n")
#                            python_file.write("# META   \"language_group\": \"synapse_pyspark\"\n")
#                            python_file.write("# META }\n\n")
#                    elif (cell["cell_type"] == "markdown"):
#                        python_file.write("# MARKDOWN ********************\n\n")
#                        for sourceline in cell['source']:
#                            line = "# "+ sourceline
#                            python_file.write(line)
#                        python_file.write("\n\n")
#                
#            remove_last_line(py_fabric_file)
#        print("Completed fabric py conversion for "+filenamewithoutext)
#    print("Completed all Fabric PY conversions saved to : "+notebooks_fabric_py_dir)

#@staticmethod
#def UploadNotebook(self, directory_client: DataLakeDirectoryClient, local_dir_path: str, file_name: str):
#    file_client = directory_client.get_file_client(file_name)
#    with io.open(file=os.path.join(local_dir_path, file_name), mode="rb") as data:
#        file_client.upload_data(data, overwrite=True)


#@staticmethod
#def UploadAllNotebooks(workspacename: str, datapath: str):
#    print("Started uploading to :" + workspacename + " file path " + datapath)
#    account_name = "onelake"  # always this
#    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
#    local_notebook_path = os.environ['DBT_PROJECT_DIR'] + '/target/notebooks'
#    token_credential = DefaultAzureCredential()
#    service_client = DataLakeServiceClient(account_url, credential=token_credential)
#    file_system_client = service_client.get_file_system_client(workspacename)
#    print("File System Client Created")
#    print(datapath)
#    paths = file_system_client.get_paths(path=datapath)
#    print("\nCurrent paths in the workspace:")
#
#    for path in paths:
#        print(path.name + '\n')
#
#    # directory_client = DataLakeDirectoryClient(account_url,workspacename,datapath, credential=token_credential);
#    notebookarr = os.listdir(Path(local_notebook_path))
#
#    for notebook in notebookarr:
#        # UploadNotebook(file_system_client,directory_client,local_notebook_path,notebook)
#        print("Uploaded:" + notebook)
#    print("Completed uploading to :" + workspacename + " file path " + datapath)
#    print("Be sure to run the notebook import from Fabric")
