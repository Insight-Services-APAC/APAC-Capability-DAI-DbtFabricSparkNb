import io
import re
import shutil
from jinja2 import Environment, FileSystemLoader
import nbformat as nbf
import os
import copy
import dbt.logger as logger
import dbt.tests as dbttests
import dbt.config as dbtconfig
import json
from dbt.contracts.graph.manifest import Manifest
import dbt.adapters.fabricsparknb.catalog as Catalog
from dbt.clients.system import load_file_contents
from dbt.adapters.fabricsparknb.notebook import ModelNotebook
import dbt.adapters.fabricsparknb.notebook as mn
from pathlib import Path
from sysconfig import get_paths
import importlib.util
import sys
import subprocess
from azure.identity import DefaultAzureCredential
import uuid
from msfabricpysdkcore import FabricClientCore
import base64



@staticmethod
def CheckSqlForModelCommentBlock(sql) -> bool:
    # Extract the comments from the SQL
    comments = re.findall(r'/\*(.*?)\*/', sql, re.DOTALL)

    # Convert each comment to a JSON object
    merged_json = {}
    for comment in comments:
        try:
            json_object = json.loads(comment)
            merged_json.update(json_object)
        except json.JSONDecodeError:
            logger.logger.error('Could not parse comment as JSON')
            pass

    if 'node_id' in merged_json.keys():
        return True
    else:
        return False



@staticmethod
def GenerateMasterNotebook(project_root, workspaceid, lakehouseid, lakehouse_name, project_name):
    # Iterate through the notebooks directory and create a list of notebook files
    notebook_dir = f'./{project_root}/target/notebooks/'
    notebook_files_str = [os.path.splitext(os.path.basename(f))[0] for f in os.listdir(Path(notebook_dir)) if f.endswith('.ipynb') and 'master_notebook' not in f]

    manifest = GetManifest()
    nodes_copy = SortManifest(manifest.nodes)

    notebook_files = []
    # Add sort_order attribute to each file object
    for file in notebook_files_str:
        notebook_file = {}
        matching_node = next((node for node in nodes_copy.values() if node.unique_id == file), None)
        if matching_node:
            notebook_file['name'] = file
            notebook_file['sort_order'] = matching_node.sort_order
            notebook_files.append(notebook_file)

    # Find the minimum and maximum sort_order
    min_sort_order = min(file['sort_order'] for file in notebook_files)
    max_sort_order = max(file['sort_order'] for file in notebook_files)

    # Loop from min_sort_order to max_sort_order
    for sort_order in range(min_sort_order, max_sort_order + 1):
        # Get the files with the current sort_order
        files_with_current_sort_order = [file for file in notebook_files if file['sort_order'] == sort_order]
        file_str_with_current_sort_order = [file['name'] for file in notebook_files if file['sort_order'] == sort_order]
        # Do something with the files...

        # Define the directory containing the Jinja templates
        template_dir = str((mn.GetIncludeDir()) / Path('notebooks/'))

        # Create a Jinja environment
        env = Environment(loader=FileSystemLoader(template_dir))

        # Load the template
        template = env.get_template('master_notebook_x.ipynb')

        # Render the template with the notebook_file variable
        rendered_template = template.render(notebook_files=file_str_with_current_sort_order, run_order=sort_order, lakehouse_name=lakehouse_name, project_name=project_name)

        # Parse the rendered template as a notebook
        nb = nbf.reads(rendered_template, as_version=4)

        # Write the notebook to a file
        target_file_name = f'master_{project_name}_notebook_{sort_order}.ipynb'
        with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:            
            try:
                nb_str = nbf.writes(nb)
                f.write(nb_str)
                print(f"{target_file_name} created")
            except Exception as ex:
                print(f"Error creating: {target_file_name}")
                raise ex
            

    # Define the directory containing the Jinja templates
    template_dir = str((mn.GetIncludeDir()) / Path('notebooks/'))

    # Create a Jinja environment
    env = Environment(loader=FileSystemLoader(template_dir))

    # Load the template
    template = env.get_template('master_notebook.ipynb')

    MetaHashes = Catalog.GetMetaHashes(project_root)    
    # Render the template with the notebook_file variable
    rendered_template = template.render(lakehouse_name=lakehouse_name, hashes=MetaHashes, project_name=project_name)

    # Parse the rendered template as a notebook
    nb = nbf.reads(rendered_template, as_version=4)

    # Find Markdown cell contaning # Executions for Each Run Order Below:
    insertion_point = None
    for i, cell in enumerate(nb.cells):
        if cell.cell_type == 'markdown' and cell.source.startswith('# Executions for Each Run Order Below:'):
            insertion_point = i + 1
            break
    
    for sort_order in range(min_sort_order, max_sort_order + 1):
        cell = nbf.v4.new_markdown_cell(source=f"## Run Order {sort_order}")
        nb.cells.insert((insertion_point), cell)
        insertion_point += 1
        # Create a new code cell with the SQL
        code = f'call_child_notebook("master_{project_name}_notebook_' + str(sort_order) + '", new_batch_id)'
        cell = nbf.v4.new_code_cell(source=code)
        # Add the cell to the notebook
        nb.cells.insert((insertion_point), cell)
        insertion_point += 1
   
    # Write the notebook to a file
    target_file_name = f'master_{project_name}_notebook.ipynb'
    with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:
        try:
            nb_str = nbf.writes(nb)
            f.write(nb_str)
            print(f"{target_file_name} created")
        except Exception as ex:
            print(f"Error creating: {target_file_name}")
            raise ex


def GenerateMetadataExtract(project_root, workspaceid, lakehouseid, lakehouse_name, project_name):
    notebook_dir = f'./{project_root}/target/notebooks/'
    # Define the directory containing the Jinja templates
    template_dir = str((mn.GetIncludeDir()) / Path('notebooks/'))

    # Create a Jinja environment
    env = Environment(loader=FileSystemLoader(template_dir))

    # Load the template
    template = env.get_template('metadata_extract.ipynb')

    # Render the template with the notebook_file variable
    rendered_template = template.render(workspace_id=workspaceid, lakehouse_id=lakehouseid, project_root=project_root, lakehouse_name=lakehouse_name)

    # Parse the rendered template as a notebook
    nb = nbf.reads(rendered_template, as_version=4)

    # Write the notebook to a file    
    target_file_name = f'metadata_{project_name}_extract.ipynb'
    with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:
        try:
            nb_str = nbf.writes(nb)
            f.write(nb_str)
            print(f"{target_file_name} created")
        except Exception as ex:
            print(f"Error creating: {target_file_name}")
            raise ex


def GenerateNotebookUpload(project_root, workspaceid, lakehouseid, lakehouse_name, project_name):
    notebook_dir = f'./{project_root}/target/notebooks/'
    # Define the directory containing the Jinja templates
    template_dir = str((mn.GetIncludeDir()) / Path('notebooks/'))

    # Create a Jinja environment
    env = Environment(loader=FileSystemLoader(template_dir))

    # Load the template
    template = env.get_template('import_notebook.ipynb')

    # Render the template with the notebook_file variable
    rendered_template = template.render(workspace_id=workspaceid, lakehouse_id=lakehouseid, project_root=project_root, lakehouse_name=lakehouse_name)

    # Parse the rendered template as a notebook
    nb = nbf.reads(rendered_template, as_version=4)
    
    # Write the notebook to a file    
    target_file_name = f'import_{project_name}_notebook.ipynb'
    with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:
        try:
            nb_str = nbf.writes(nb)
            f.write(nb_str)
            print(f"{target_file_name} created")
        except Exception as ex:
            print(f"Error creating: {target_file_name}")
            raise ex


def GenerateAzCopyScripts(project_root, workspaceid, lakehouseid):
    notebook_dir = f'./{project_root}/target/pwsh/'

    Path(notebook_dir).mkdir(parents=True, exist_ok=True)
    # Define the directory containing the Jinja templates
    template_dir = str((mn.GetIncludeDir()) / Path('pwsh/'))    

    # Create a Jinja environment

    env = Environment(loader=FileSystemLoader(template_dir))

    # Load the template
    template = env.get_template('upload.ps1')

    # Render the template with the notebook_file variable
    rendered_template = template.render(project_root=project_root, workspace_id=workspaceid, lakehouse_id=lakehouseid)

    # Write the notebook to a file
    with io.open(notebook_dir + 'upload.ps1', 'w') as f:
        f.write(rendered_template)
        print("upload.ps1 created")

        # Load the template
    template = env.get_template('download.ps1')

    # Render the template with the notebook_file variable
    rendered_template = template.render(project_root=project_root, workspace_id=workspaceid, lakehouse_id=lakehouseid)

    # Write the notebook to a file
    with io.open(notebook_dir + 'download.ps1', 'w') as f:
        f.write(rendered_template)
        print("download.ps1 created")


@staticmethod
def SetSqlVariableForAllNotebooks(project_root, lakehouse_name):
    # Iterate through the notebooks directory and create a list of notebook files
    notebook_dir = f'./{project_root}/target/notebooks/'
    notebook_files = [f for f in os.listdir(Path(notebook_dir)) if f.endswith('.ipynb')]

    for notebook_file in notebook_files:
        # Load the notebook
        with io.open(file=notebook_dir + notebook_file, mode='r', encoding='utf-8') as f:
            file_str = f.read()
            nb = nbf.reads(file_str, as_version=4)
        
        if notebook_file.startswith('test.'):
            node_type = 'test'
        else:
            node_type = 'model'

        mnb: ModelNotebook = ModelNotebook(nb=nb, node_type=node_type)        
        # Gather the Spark SQL from the notebook and set the sql variable
        mnb.GatherSql()
        mnb.SetTheSqlVariable()
        # always set the config in first code cell
        mnb.nb.cells[1].source = mnb.nb.cells[1].source.replace("{{lakehouse_name}}", lakehouse_name)

        # Write the notebook to a file
        target_file_name = notebook_file
        with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:
            try:
                nb_str = nbf.writes(nb)
                f.write(nb_str)
                print(f"{target_file_name} updated")
            except Exception as ex:
                print(f"Error updating: {target_file_name}")
                raise ex


@staticmethod
def GetManifest():
    # Specify the path to your manifest file
    manifest_path = os.environ['DBT_PROJECT_DIR'] + '/target/manifest.json'

    # Load the file contents
    file_contents = load_file_contents(manifest_path)

    # Parse the JSON content into a dictionary
    data = json.loads(file_contents)

    # Convert the dictionary into a Manifest object
    manifest = Manifest.from_dict(data)
    return manifest

@staticmethod
def GetFabricPlatformContent(displayName):
            logicalId = str(uuid.uuid4())  
            
            platformcontent = """{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {
    "type": "Notebook",
    "displayName": \""""+displayName+"""\",
    "description": "New notebook"
   },
   "config": {
     "version": "2.0",
     "logicalId": \""""+logicalId+"""\"
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
### Generate py files for api update
def IPYNBtoFabricPYFile(dbt_project_dir):
    print("Converting notebooks to Fabric PY format")
    target_dir = os.path.join(dbt_project_dir,"target")
    notebooks_dir = os.path.join(target_dir,"notebooks")
    notebooks_fabric_py_dir = os.path.join(target_dir,"notebooks_fabric_py")
    os.chdir(notebooks_dir)
    os.makedirs(notebooks_fabric_py_dir, exist_ok=True)
    list_of_notebooks = os.listdir(notebooks_dir)
    for filename in list_of_notebooks:
        filenamewithoutext = filename[:-6]  ## remove .ipynb
        py_fabric_file = os.path.join(notebooks_fabric_py_dir,filenamewithoutext+".py")
        path = dbt_project_dir         
        FabricPlatformContent = GetFabricPlatformContent(filenamewithoutext)          
        with open(py_fabric_file, "w", encoding="utf-8") as python_file:
            python_file.write("# Fabric notebook source\n\n")
            python_file.write("# METADATA ********************\n\n")
            python_file.write("# META {\n")
            python_file.write("# META   \"kernel_info\": {\n")
            python_file.write("# META     \"name\": \"synapse_pyspark\"\n")
            python_file.write("# META   }\n")
            python_file.write("# META }\n\n")
            f = open (filename, "r", encoding="utf-8") 
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
                            line = "# MAGIC "+ sourceline
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
        print("Completed fabric py conversion for "+filenamewithoutext)
    print("Completed all Fabric PY conversions saved to : "+notebooks_fabric_py_dir)
  

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




@staticmethod
def SortManifest(nodes_orig):
    nodes = copy.deepcopy(nodes_orig)
    sort_order = 0
    while nodes:
        # Find nodes that have no dependencies within the remaining nodes
        # nodes_without_deps = [node_id for node_id, node in nodes.items() if not any(dep in nodes for dep in node.depends_on.nodes)]

        # Initialize an empty list to store the node_ids
        nodes_without_deps = []

        # Iterate over the items in the nodes dictionary
        for node_id, node in nodes.items():
            # Initialize a flag to indicate whether a dependency is found
            has_dependency = False

            # Check if the node has a depends_on attribute
            if hasattr(node, 'depends_on'):
                # Check if depends_on has a nodes attribute
                if hasattr(node.depends_on, 'nodes'):
                    # Iterate over the nodes in depends_on
                    for dep in node.depends_on.nodes:
                        # Check if the dependency is in nodes
                        if dep in nodes:
                            has_dependency = True
                            break
                else :
                    # If the node has no depends_on attribute, it has no dependencies
                    has_dependency = False
                    print(f"Node {node_id} has no nodes attribute")
            else :
                # If the node has no depends_on attribute, it has no dependencies
                has_dependency = False
                print(f"Node {node_id} has no depends_on attribute")

            # If no dependency was found, add the node_id to the list
            if not has_dependency:
                nodes_without_deps.append(node_id)

        if not nodes_without_deps:
            raise Exception('Circular dependency detected')
        # Assign the current sort order to the nodes without dependencies
        for node_id in nodes_without_deps:
            nodes_orig[node_id].sort_order = sort_order
            del nodes[node_id]
        # Increment the sort order
        sort_order += 1
    return nodes_orig

def stringToBase64(s):
    return base64.b64encode(s.encode('utf-8')).decode('utf-8')

def base64ToString(b):
    return base64.b64decode(b).decode('utf-8')

def GenerateNotebookContent(notebookcontentBase64):
    notebook_w_content = {'parts': [{'path': 'notebook-content.py', 'payload': notebookcontentBase64, 'payloadType': 'InlineBase64'}]}
    return notebook_w_content


def findnotebookid(notebooks,displayname):
    for notebook in notebooks:
        if notebook.display_name == displayname:
            return notebook.id
    return -1

@staticmethod
def APIUpsertNotebooks(dbt_project_dir,workspace_id):
    print("Please ensure your terminal is authenticated with az login as the following process will attempt to upload to fabric")
    print("Uploading notebooks via API ...")
    target_dir = os.path.join(dbt_project_dir,"target")
    notebooks_fabric_py_dir = os.path.join(target_dir,"notebooks_fabric_py")
    os.chdir(notebooks_fabric_py_dir)
    fc = FabricClientCore()
    workspace = fc.get_workspace_by_id(id = workspace_id)
    workspace_id = workspace.id
    servernotebooks = fc.list_notebooks(workspace_id)
    list_of_notebooks = os.listdir(notebooks_fabric_py_dir)
    for filename in list_of_notebooks:
            with open(filename, 'r', encoding="utf8") as file:
                notebookcontent = file.read()
                notebookname = filename[:-3]  ## remove .py
                notebookcontentBase64 =  stringToBase64(notebookcontent)
                notebook_w_content_new =  GenerateNotebookContent(notebookcontentBase64)           

                #notebook_w_content = fc.get_notebook(workspace_id, notebook_name=notebookname)
                notebookid = findnotebookid(servernotebooks,notebookname)
                if notebookid == -1:
                    notebook = fc.create_notebook(workspace_id, definition = notebook_w_content_new, display_name=notebookname)
                    print("Notebook created "+ notebookname)
                else: 
                    notebook2 = fc.update_notebook_definition(workspace_id, notebookid,definition = notebook_w_content_new)
                    print("Notebook updated "+ notebookname) 
    print("Completed uploading notebooks via API")


@staticmethod
def APIRunNotebook(workspace_id,notebook_name):
    print("Please ensure your terminal is authenticated with az login as the following process will attempt to upload to fabric")
    print("Running notebook via API ...")
    fc = FabricClientCore()
    workspace = fc.get_workspace_by_id(id = workspace_id)
    workspace_id = workspace.id
    servernotebooks = fc.list_notebooks(workspace_id)
    for nb_name in servernotebooks:
            print('')
    print("Completed uploading notebooks via API")

def PrintFirstTimeRunningMessage():
    print('\033[1;33;48m', "It seems like this is the first time you are running this project. Please update the metadata extract json files in the metaextracts directory by performing the following steps:")
    print(f"1. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/upload.ps1")
    print("2. Login to the Fabric Portal and navigate to the workspace and lakehouse you are using")
    print(f"3. Manually upload the following notebook to your workspace: {os.environ['DBT_PROJECT_DIR']}/target/notebooks/import_{os.environ['DBT_PROJECT_DIR']}_notebook.ipynb. See https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks")
    print(f"4. Open the notebook in the workspace and run all cells. This will upload the generated notebooks to your workspace.")
    print(f"5. A new notebook should appear in the workspace called metadata_{os.environ['DBT_PROJECT_DIR']}_extract.ipynb. Open this notebook and run all cells. This will generate the metadata extract json files in the metaextracts directory.")
    print(f"6. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/download.ps1. This will download the metadata extract json files to the metaextracts directory.")
    print(f"7. Re-run this script to generate the model and master notebooks.") 


@staticmethod
def RunDbtProject(PreInstall=False,Upload=False):
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
    GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])
    if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks"):
        os.makedirs(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks")

    GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
    GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])

    # count files in metaextracts directory
    if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/metaextracts"):
        PrintFirstTimeRunningMessage()
    elif len(os.listdir(os.environ['DBT_PROJECT_DIR'] + "/metaextracts")) == 0:
        PrintFirstTimeRunningMessage()
    else:
        if (PreInstall is True):
            #make sure we are using the installed dbt version
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
        
        SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
        GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
        curr_dir = os.getcwd()
        dbt_project_dir = os.path.join(curr_dir,os.environ['DBT_PROJECT_DIR'])
        IPYNBtoFabricPYFile(dbt_project_dir)
        if (Upload == True):
            APIUpsertNotebooks(dbt_project_dir, target_info['workspaceid'])

@staticmethod
def RunDbtProjectArg(PreInstall:bool, argv:list[str]):
    Upload = False
    project_root = argv[1].replace("\\", "/")
    if len(sys.argv) == 2:    
        os.environ['DBT_PROJECT_DIR'] = project_root
        RunDbtProject(PreInstall=PreInstall,Upload=Upload)
    elif len(sys.argv) == 3:  
        os.environ['DBT_PROJECT_DIR'] = project_root
        if sys.argv[2] == "1":
            Upload = True 
        RunDbtProject(PreInstall=PreInstall,Upload=Upload)
    elif len(sys.argv) == 4:    
        os.environ['DBT_PROJECT_DIR'] = project_root
        if sys.argv[2] == "1":
            Upload = True 
        os.environ['DBT_PROFILES_DIR'] = sys.argv[3]
        RunDbtProject(PreInstall=PreInstall,Upload=Upload)
    else:
        print("Please supply at least DBT project directory as a parameter.")

        

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
