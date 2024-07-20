import uuid
from msfabricpysdkcore import FabricClientCore
import base64
import os
import json


@staticmethod
def GetFabricPlatformContent(displayName):
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


@staticmethod
def remove_last_line(py_fabric_file: str):
    with open(py_fabric_file, "r+", encoding="utf-8") as file:
        lines = file.readlines()
        file.seek(0)
        file.truncate()
        file.writelines(lines[:-1])


@staticmethod
# Generate py files for api update
def IPYNBtoFabricPYFile(dbt_project_dir):
    print("Converting notebooks to Fabric PY format")
    target_dir = os.path.join(dbt_project_dir, "target")
    notebooks_dir = os.path.join(target_dir, "notebooks")
    notebooks_fabric_py_dir = os.path.join(target_dir, "notebooks_fabric_py")
    os.chdir(notebooks_dir)
    os.makedirs(notebooks_fabric_py_dir, exist_ok=True)
    list_of_notebooks = os.listdir(notebooks_dir)
    for filename in list_of_notebooks:
        filenamewithoutext = filename[:-6]  # # remove .ipynb
        py_fabric_file = os.path.join(notebooks_fabric_py_dir, filenamewithoutext + ".py")
        # path = dbt_project_dir
        FabricPlatformContent = GetFabricPlatformContent(filenamewithoutext)          
        with open(py_fabric_file, "w", encoding="utf-8") as python_file:
            python_file.write("# Fabric notebook source\n\n")
            python_file.write("# METADATA ********************\n\n")
            python_file.write("# META {\n")
            python_file.write("# META   \"kernel_info\": {\n")
            python_file.write("# META     \"name\": \"synapse_pyspark\"\n")
            python_file.write("# META   }\n")
            python_file.write("# META }\n\n")
            f = open(filename, "r", encoding="utf-8")
            data = json.loads(f.read())
            for cell in data['cells']:
                if (cell["cell_type"] == "code"):
                    if (cell["source"][0][:5] == "%%sql"):
                        python_file.write("# CELL ********************\n\n")
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
                        python_file.write("# CELL ********************\n\n")
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
                        python_file.write("# CELL ********************\n\n")
                        for sourceline in cell['source']:
                            line = "# MAGIC "+ sourceline
                            python_file.write(line)
                        python_file.write("\n\n")
                    else:
                        python_file.write("# CELL ********************\n\n")
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
                        line = "# "+ sourceline
                        python_file.write(line)
                    python_file.write("\n\n")
                
        remove_last_line(py_fabric_file)
        print("Completed fabric py conversion for " + filenamewithoutext)
    print("Completed all Fabric PY conversions saved to : " + notebooks_fabric_py_dir)


def stringToBase64(s):
    return base64.b64encode(s.encode('utf-8')).decode('utf-8')


def base64ToString(b):
    return base64.b64decode(b).decode('utf-8')


def GenerateNotebookContent(notebookcontentBase64):
    notebook_w_content = {'parts': [{'path': 'notebook-content.py', 'payload': notebookcontentBase64, 'payloadType': 'InlineBase64'}]}
    return notebook_w_content


def findnotebookid(notebooks, displayname):
    for notebook in notebooks:
        if notebook.display_name == displayname:
            return notebook.id
    return -1


@staticmethod
def APIUpsertNotebooks(dbt_project_dir, workspace_id):
    print("Please ensure your terminal is authenticated with az login as the following process will attempt to upload to fabric")
    print("Uploading notebooks via API ...")
    target_dir = os.path.join(dbt_project_dir, "target")
    notebooks_fabric_py_dir = os.path.join(target_dir, "notebooks_fabric_py")
    os.chdir(notebooks_fabric_py_dir)
    fc = FabricClientCore()
    workspace = fc.get_workspace_by_id(id=workspace_id)
    workspace_id = workspace.id
    servernotebooks = fc.list_notebooks(workspace_id)
    list_of_notebooks = os.listdir(notebooks_fabric_py_dir)
    for filename in list_of_notebooks:
        with open(filename, 'r', encoding="utf8") as file:
            notebookcontent = file.read()
            notebookname = filename[:-3]  # # remove .py
            notebookcontentBase64 = stringToBase64(notebookcontent)
            notebook_w_content_new = GenerateNotebookContent(notebookcontentBase64)  

            # notebook_w_content = fc.get_notebook(workspace_id, notebook_name=notebookname)
            notebookid = findnotebookid(servernotebooks, notebookname)
            if notebookid == -1:
                notebook = fc.create_notebook(workspace_id, definition=notebook_w_content_new, display_name=notebookname)
                print("Notebook created " + notebookname)
            else: 
                notebook2 = fc.update_notebook_definition(workspace_id, notebookid, definition=notebook_w_content_new)
                print("Notebook updated " + notebookname) 
    print("Completed uploading notebooks via API")


@staticmethod
def APIRunNotebook(workspace_id, notebook_name):
    print("Please ensure your terminal is authenticated with az login as the following process will attempt to upload to fabric")
    print("Running notebook via API ...")
    fc = FabricClientCore()
    workspace = fc.get_workspace_by_id(id = workspace_id)
    workspace_id = workspace.id
    servernotebooks = fc.list_notebooks(workspace_id)
    for nb_name in servernotebooks:
            print('')
    print("Completed uploading notebooks via API")




#@staticmethod

### Generate py files and.platform files for git direct deployment
#def IPYNBtoFabricPYFileAndGitStructure(dbt_project_dir):
#    print("Converting notebooks to Fabric PY format")
#    target_dir = os.path.join(dbt_project_dir,"target")
#    notebooks_dir = os.path.join(target_dir,"notebooks")
#    os.chdir(notebooks_dir)
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
