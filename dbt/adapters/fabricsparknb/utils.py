
import re
import dbt.logger
from jinja2 import Environment, FileSystemLoader
import nbformat as nbf
import os
from dataclasses import dataclass
import dbt.parser
import dbt.parser.manifest
import dbt.tests.util
import dbt.utils
import dbt
import copy
import dbt.adapters.fabricspark
import dbt.adapters.fabricsparknb 
import dbt.logger as logger
from dbt.adapters.fabricsparknb import utils as utils 
import dbt.tests
import os
import json
from dbt.contracts.graph.manifest import Manifest
from dbt.clients.system import load_file_contents

from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential




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
def GenerateMasterNotebook(project_root):
    # Iterate through the notebooks directory and create a list of notebook files
    notebook_dir = f'./{project_root}/target/notebooks/'
    notebook_files_str = [os.path.splitext(os.path.basename(f))[0] for f in os.listdir(notebook_dir) if f.endswith('.ipynb') and 'master_notebook' not in f]

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
        template_dir = 'dbt/include/fabricsparknb/'

        # Create a Jinja environment
        env = Environment(loader=FileSystemLoader(template_dir))

        # Load the template
        template = env.get_template('master_notebook.ipynb')

        # Render the template with the notebook_file variable
        rendered_template = template.render(notebook_files=file_str_with_current_sort_order)

        # Parse the rendered template as a notebook
        nb = nbf.reads(rendered_template, as_version=4)

        # Write the notebook to a file
        with open(notebook_dir + f'master_notebook_{sort_order}.ipynb', 'w') as f:
            nbf.write(nb, f)
            print (f"master_notebook_{sort_order}.ipynb created")


    #Create the master notebook
    nb = nbf.v4.new_notebook()
    cell = nbf.v4.new_code_cell(source="import mssparkutils.notebook")
    # Add the cell to the notebook
    nb.cells.append(cell)

    for sort_order in range(min_sort_order, max_sort_order + 1):
        # Create a new code cell with the SQL
        code = 'mssparkutils.notebook.run("master_notebook_'+str(sort_order)+'")'
        cell = nbf.v4.new_code_cell(source=code)
        # Add the cell to the notebook
        nb.cells.append(cell)
    
    # Write the notebook to a file
    with open(notebook_dir + f'master_notebook.ipynb', 'w') as f:
        nbf.write(nb, f)
        print (f"master_notebook.ipynb created")

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
def SortManifest(nodes_orig):
    nodes = copy.deepcopy(nodes_orig)
    sort_order = 0
    while nodes:
        # Find nodes that have no dependencies within the remaining nodes
        #nodes_without_deps = [node_id for node_id, node in nodes.items() if not any(dep in nodes for dep in node.depends_on.nodes)]
        
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


@staticmethod
def UploadNotebook(self, directory_client: DataLakeDirectoryClient, local_dir_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)
    with open(file=os.path.join(local_dir_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)

@staticmethod
def UploadAllNotebooks():
    account_name = "onelake"
    workspace_name = "Fabric_AI_Test"
    data_path = "lakesales.Lakehouse/Files/notebooks"
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    local_notebook_path = os.environ['DBT_PROJECT_DIR'] + '/target/notebooks'
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    file_system_client = service_client.get_file_system_client(workspace_name)
    directory_client = DataLakeDirectoryClient(account_url,workspace_name,"lakesales.Lakehouse/Files/notebooks", credential=token_credential);
    notebookarr = os.listdir(local_notebook_path)
    for notebook in notebookarr:
        UploadNotebook(file_system_client,directory_client,local_notebook_path,notebook)
        print("Uploaded:"+notebook)
        




