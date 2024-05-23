
import re
from dataclasses import dataclass
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
        template = env.get_template('master_notebook_x.ipynb')

        # Render the template with the notebook_file variable
        rendered_template = template.render(notebook_files=file_str_with_current_sort_order, run_order=sort_order)

        # Parse the rendered template as a notebook
        nb = nbf.reads(rendered_template, as_version=4)

        # Write the notebook to a file
        with open(notebook_dir + f'master_notebook_{sort_order}.ipynb', 'w') as f:
            nbf.write(nb, f)
            print (f"master_notebook_{sort_order}.ipynb created")

    # Define the directory containing the Jinja templates
    template_dir = 'dbt/include/fabricsparknb/'

    # Create a Jinja environment
    env = Environment(loader=FileSystemLoader(template_dir))

    # Load the template
    template = env.get_template('master_notebook.ipynb')

    # Render the template with the notebook_file variable
    rendered_template = template.render()

    # Parse the rendered template as a notebook
    nb = nbf.reads(rendered_template, as_version=4)

    for sort_order in range(min_sort_order, max_sort_order + 1):
        cell = nbf.v4.new_markdown_cell(source=f"## Run Order {sort_order}")
        nb.cells.append(cell)
        # Create a new code cell with the SQL
        code = 'mssparkutils.notebook.run("master_notebook_'+str(sort_order)+'")'
        cell = nbf.v4.new_code_cell(source=code)
        # Add the cell to the notebook
        nb.cells.append(cell)
    
    # Write the notebook to a file
    with open(notebook_dir + f'master_notebook.ipynb', 'w') as f:
        nbf.write(nb, f)
        print (f"master_notebook.ipynb created")


class ModelNotebook:
    def __init__(self, nb : nbf.NotebookNode = None, node_type = 'model'):        
        if nb is None:
            filename = f'dbt/include/fabricsparknb/{node_type}_notebook.ipynb'            
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    nb = nbf.read(f, as_version=4)
             
        self.nb: nbf.NotebookNode = nb
        self.sql: str = ""    

    def AddSqlFromExistingNotebook(self, notebook):
        for cell in notebook.cells:
            if cell.cell_type == 'code' and "\\*FABRICSPARKNB: SQLSTART*\\" in cell.source and "\\*FABRICSPARKNB: SQLEND*\\" in cell.source:
                # Define the pattern
                pattern = r'/\*FABRICSPARKNB: SQLSTART\*/(.*?)/\*FABRICSPARKNB: SQLEND\*/'

                # Search for the pattern in the SQL
                match = re.search(pattern, cell.source, re.DOTALL)

                # If a match was found, return the matched text; otherwise, return an empty string
                old_sql = match.group(1) if match else ''
                
                self.sql = old_sql
        

    def AddSql(self, sql):
        self.sql += '\n' + sql
    
    def AddCell(self, cell):        
        # Add the cell to the notebook
        self.nb.cells.append(cell)

    def GatherSql(self):
        #Concatenate all the SQL cells in the notebook
        self.sql = ""
        for cell in self.GetSparkSqlCells():
            self.sql += '\n' + cell.source.replace("%%sql","")        
    
    def SetTheSqlVariable(self):
        # Find the first code cell and set the sql variable
        for i, cell in enumerate(self.nb.cells):
            if cell.cell_type == 'markdown' and "# Declare the SQL" in cell.source:
                target_cell = self.nb.cells[i+1]
                target_cell.source = target_cell.source.replace("{{sql}}", self.sql)
                break

    def GetSparkSqlCells(self):
        # Get the existing SQL Cell from the notebook. It will be the code cell following the markdown cell containing "# SPARK SQL Cell for Debugging"
        spark_sql_cell = None
        for i, cell in enumerate(self.nb.cells):
            if cell.cell_type == 'markdown' and "# SPARK SQL Cells for Debugging" in cell.source:
                spark_sql_cells = self.nb.cells[i+1:len(self.nb.cells)]
        
        return spark_sql_cells      

    def Render(self):
        # Define the directory containing the Jinja templates
        template_dir = 'dbt/include/fabricsparknb/'

        # Create a Jinja environment
        env = Environment(loader=FileSystemLoader(template_dir))

        # Load the template
        template = env.get_template('model_notebook.ipynb')

        # Render the template with the notebook_file variable
        rendered_template = template.render(sql=self.sql.replace('\n',''))

        # Parse the rendered template as a notebook
        self.nb = nbf.reads(rendered_template, as_version=4)        


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
def UploadAllNotebooks(fabricworkspacename: str, datapath: str):
    print("Started uploading to :"+fabricworkspacename+" file path "+datapath)
    account_name = "onelake"  ##always this                 
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    local_notebook_path = os.environ['DBT_PROJECT_DIR'] + '/target/notebooks'
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    file_system_client = service_client.get_file_system_client(fabricworkspacename)
    directory_client = DataLakeDirectoryClient(account_url,fabricworkspacename,datapath, credential=token_credential);
    notebookarr = os.listdir(local_notebook_path)
    for notebook in notebookarr:
        UploadNotebook(file_system_client,directory_client,local_notebook_path,notebook)
        print("Uploaded:"+notebook)
    print("Completed uploading to :"+fabricworkspacename+" file path "+datapath)
    print("Be sure to run the notebook import from Fabric")
        




