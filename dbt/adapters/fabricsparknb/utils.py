
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
from dbt.adapters.fabricsparknb import utils as utils 
import dbt.tests
import os
import json
from dbt.contracts.graph.manifest import Manifest
from dbt.clients.system import load_file_contents




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
        nodes_without_deps = [node_id for node_id, node in nodes.items() if not any(dep in nodes for dep in node.depends_on.nodes)]
        if not nodes_without_deps:
            raise Exception('Circular dependency detected')
        # Assign the current sort order to the nodes without dependencies
        for node_id in nodes_without_deps:
            nodes_orig[node_id].sort_order = sort_order
            del nodes[node_id]
        # Increment the sort order
        sort_order += 1
    return nodes_orig



