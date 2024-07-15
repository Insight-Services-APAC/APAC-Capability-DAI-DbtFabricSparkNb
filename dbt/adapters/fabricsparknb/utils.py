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
        code = f'mssparkutils.notebook.run("master_{project_name}_notebook_' + str(sort_order) + '")'
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


@staticmethod
def RunDbtProject(PreInstall=False):
    # Get Config and Profile Information from dbt
    profile_path = Path(os.path.expanduser('~')) / '.dbt/'
    profile = dbtconfig.profile.read_profile(profile_path)
    config = dbtconfig.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])
    profile_info = profile[config['profile']]
    target_info = profile_info['outputs'][profile_info['target']]    
    lakehouse = target_info['lakehouse']

    shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
    # Generate AzCopy Scripts and Metadata Extract Notebooks
    GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])
    if not os.path.exists(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks"):
        os.makedirs(os.environ['DBT_PROJECT_DIR'] + "/target/notebooks")

    GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
    GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])

    # count files in metaextracts directory
    if len(os.listdir(os.environ['DBT_PROJECT_DIR'] + "/metaextracts")) == 0:
        print('\033[1;33;48m', "It seems like this is the first time you are running this project. Please update the metadata extract json files in the metaextracts directory by performing the following steps:")
        print(f"1. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/upload.ps1")
        print("2. Login to the Fabric Portal and navigate to the workspace and lakehouse you are using")
        print(f"3. Manually upload the following notebook to your workspace: {os.environ['DBT_PROJECT_DIR']}/target/notebooks/import_{os.environ['DBT_PROJECT_DIR']}_notebook.ipynb. See https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks")
        print(f"4. Open the notebook in the workspace and run all cells. This will upload the generated notebooks to your workspace.")
        print(f"5. A new notebook should appear in the workspace called metadata_{os.environ['DBT_PROJECT_DIR']}_extract.ipynb. Open this notebook and run all cells. This will generate the metadata extract json files in the metaextracts directory.")
        print(f"6. Run ./{os.environ['DBT_PROJECT_DIR']}/target/pwsh/download.ps1. This will download the metadata extract json files to the metaextracts directory.")
        print(f"7. Re-run this script to generate the model and master notebooks.")
    else:
        if (PreInstall is True):
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
