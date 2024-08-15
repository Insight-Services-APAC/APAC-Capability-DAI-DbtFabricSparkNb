import io
import copy
from jinja2 import Environment, FileSystemLoader
import nbformat as nbf
import os
import json
import dbt_wrapper.catalog as Catalog
from pathlib import Path
from dbt.contracts.graph.manifest import Manifest
import dbt_wrapper.utils as mn
from dbt.adapters.fabricsparknb.notebook import ModelNotebook
from dbt.clients.system import load_file_contents
from dbt_wrapper.log_levels import LogLevel
from dbt_wrapper.stage_executor import ProgressConsoleWrapper
import fnmatch


@staticmethod
def GenerateMasterNotebook(project_root, workspaceid, lakehouseid, lakehouse_name, project_name, progress: ProgressConsoleWrapper, task_id, notebook_timeout, max_worker, log_lakehouse, notebook_hashcheck, lakehouse_config):
    # If log lakehouse is None use lakehouse as default
    if log_lakehouse is None:
        log_lakehouse = lakehouse_name
    
    # Iterate through the notebooks directory and create a list of notebook files
    notebook_dir = f'./{project_root}/target/notebooks/'
    notebook_files_str = [os.path.splitext(os.path.basename(f))[0] for f in os.listdir(Path(notebook_dir)) if f.endswith('.ipynb') and 'master_notebook' not in f]

    manifest = GetManifest(progress)
    nodes_copy = SortManifest(nodes_orig=manifest.nodes, progress=progress)

    notebook_files = []
    # Add sort_order attribute to each file object
    for file in notebook_files_str:
        notebook_file = {}
        matching_node = next((node for node in nodes_copy.values() if node.unique_id == file), None)
        if matching_node:
            notebook_file['name'] = file
            notebook_file['sort_order'] = matching_node.sort_order
            notebook_files.append(notebook_file)
   
    if len(notebook_files) == 0:
        print("No notebooks found.Try checking your model configs and model specification args")
        exit(1)
    
    # Find the minimum and maximum sort_order
    min_sort_order = min(file['sort_order'] for file in notebook_files)
    max_sort_order = max(file['sort_order'] for file in notebook_files)

    # Validate and set max worker (thread) property 
    lr_max_worker = 5 # Default value (low Range)
    hr_max_worker = 20

    match max_worker:
        case _ if max_worker < lr_max_worker:
            max_worker = lr_max_worker
            msg_text = "Max worker (thread) property is lesser than the default value, default thread value of "+str(lr_max_worker)+" has been set."
            progress.print(msg_text, LogLevel.WARNING)
        case _ if max_worker >= lr_max_worker and max_worker < hr_max_worker:
            pass
        case _ if max_worker >= hr_max_worker:
            msg_text = "Max worker (thread) property is high !!\nPlease update thread property in profile.yml, if this is not expected."
            progress.print(msg_text, LogLevel.WARNING)
        case _:
           max_worker = lr_max_worker
           msg_text = "Max worker (thread) property value is not set, default thread value of "+str(lr_max_worker)+" has been set."
           progress.print(msg_text, LogLevel.WARNING)

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
        rendered_template = template.render(notebook_files=file_str_with_current_sort_order, run_order=sort_order, lakehouse_name=lakehouse_name, project_name=project_name,max_worker=max_worker, log_lakehouse=log_lakehouse)

        # Parse the rendered template as a notebook
        nb = nbf.reads(rendered_template, as_version=4)

        # Check if lakehouse_config option is set to METADATA
        lhconfig = lakehouse_config  # Assuming highcon is a boolean variable

        if lhconfig == "METADATA":
            # Find the index of the markdown cell containing "THIS IS MARKDOWN"
            index_to_remove = None
            for i, cell in enumerate(nb.cells):
                if cell.cell_type == 'markdown' and fnmatch.fnmatch(cell.source, '*(Attach Default Lakehouse Markdown Cell)*'):
                    index_to_remove = i
                    break

            # Remove the found markdown cell and the next cell
            if index_to_remove is not None:
                nb.cells.pop(index_to_remove)
                if index_to_remove < len(nb.cells):
                    nb.cells.pop(index_to_remove)  # Remove the next cell if it exists

        # Write the notebook to a file
        target_file_name = f'master_{project_name}_notebook_{sort_order}.ipynb'
        with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:            
            try:
                nb_str = nbf.writes(nb)
                f.write(nb_str)
                progress.print(f"{target_file_name} created", level=LogLevel.INFO)
            except Exception as ex:
                progress.print(f"Error creating: {target_file_name}", level=LogLevel.ERROR)
                raise ex
            
    # Define the directory containing the Jinja templates
    template_dir = str((mn.GetIncludeDir()) / Path('notebooks/'))

    # Create a Jinja environment
    env = Environment(loader=FileSystemLoader(template_dir))

    # Load the template
    template = env.get_template('master_notebook.ipynb')

    MetaHashes = Catalog.GetMetaHashes(project_root)    
    # Render the template with the notebook_file variable
    rendered_template = template.render(lakehouse_name=lakehouse_name, hashes=MetaHashes, project_name=project_name, notebook_timeout=notebook_timeout, log_lakehouse=log_lakehouse,notebook_hashcheck=notebook_hashcheck)

    # Parse the rendered template as a notebook
    nb = nbf.reads(rendered_template, as_version=4)

    # Check if lakehouse_config option is set to METADATA
    lhconfig = lakehouse_config  # Assuming highcon is a boolean variable

    if lhconfig == "METADATA":
        # Find the index of the markdown cell containing "THIS IS MARKDOWN"
        index_to_remove = None
        for i, cell in enumerate(nb.cells):
            if cell.cell_type == 'markdown' and fnmatch.fnmatch(cell.source, '*(Attach Default Lakehouse Markdown Cell)*'):
                index_to_remove = i
                break

        # Remove the found markdown cell and the next cell
        if index_to_remove is not None:
            nb.cells.pop(index_to_remove)
            if index_to_remove < len(nb.cells):
                nb.cells.pop(index_to_remove)  # Remove the next cell if it exists

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
        code = f'call_child_notebook("master_{project_name}_notebook_' + str(sort_order) + '", new_batch_id, master_notebook)'
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
            progress.print(f"{target_file_name} created", level=LogLevel.INFO)
        except Exception as ex:
            progress.print(f"Error creating: {target_file_name}", level=LogLevel.ERROR)
            raise ex


def GenerateMetadataExtract(project_root, workspaceid, lakehouseid, lakehouse_name, project_name, progress: ProgressConsoleWrapper, task_id, lakehouse_config):
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

    # Check if lakehouse_config option is set to METADATA
    lhconfig = lakehouse_config  # Assuming highcon is a boolean variable

    if lhconfig == "METADATA":
        # Find the index of the markdown cell containing "THIS IS MARKDOWN"
        index_to_remove = None
        for i, cell in enumerate(nb.cells):
            if cell.cell_type == 'markdown' and fnmatch.fnmatch(cell.source, '*(Attach Default Lakehouse Markdown Cell)*'):
                index_to_remove = i
                break

        # Remove the found markdown cell and the next cell
        if index_to_remove is not None:
            nb.cells.pop(index_to_remove)
            if index_to_remove < len(nb.cells):
                nb.cells.pop(index_to_remove)  # Remove the next cell if it exists

    # Write the notebook to a file    
    target_file_name = f'metadata_{project_name}_extract.ipynb'
    with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:
        try:
            nb_str = nbf.writes(nb)
            f.write(nb_str)
            progress.print(f"{target_file_name} created", level=LogLevel.INFO)            
        except Exception as ex:
            progress.print(f"Error creating: {target_file_name}", level=LogLevel.ERROR)
            raise ex


def GenerateNotebookUpload(project_root, workspaceid, lakehouseid, lakehouse_name, project_name, progress: ProgressConsoleWrapper, task_id, lakehouse_config):
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

    # Check if lakehouse_config option is set to METADATA
    lhconfig = lakehouse_config  # Assuming highcon is a boolean variable

    if lhconfig == "METADATA":
        # Find the index of the markdown cell containing "THIS IS MARKDOWN"
        index_to_remove = None
        for i, cell in enumerate(nb.cells):
            if cell.cell_type == 'markdown' and fnmatch.fnmatch(cell.source, '*(Attach Default Lakehouse Markdown Cell)*'):
                index_to_remove = i
                break

        # Remove the found markdown cell and the next cell
        if index_to_remove is not None:
            nb.cells.pop(index_to_remove)
            if index_to_remove < len(nb.cells):
                nb.cells.pop(index_to_remove)  # Remove the next cell if it exists
    
    # Write the notebook to a file    
    target_file_name = f'import_{project_name}_notebook.ipynb'
    with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:
        try:
            nb_str = nbf.writes(nb)
            f.write(nb_str)
            progress.print(f"{target_file_name} created", level=LogLevel.INFO)
        except Exception as ex:
            progress.print(f"Error creating: {target_file_name}", level=LogLevel.ERROR)            
            raise ex


def GenerateAzCopyScripts(project_root, workspaceid, lakehouseid, progress: ProgressConsoleWrapper, task_id):
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
        progress.print("upload.ps1 created", level=LogLevel.INFO)        

        # Load the template
    template = env.get_template('download.ps1')

    # Render the template with the notebook_file variable
    rendered_template = template.render(project_root=project_root, workspace_id=workspaceid, lakehouse_id=lakehouseid)

    # Write the notebook to a file
    with io.open(notebook_dir + 'download.ps1', 'w') as f:
        f.write(rendered_template)
        progress.print("download.ps1 created", level=LogLevel.INFO)        


@staticmethod
def SetSqlVariableForAllNotebooks(project_root, lakehouse_name, progress: ProgressConsoleWrapper, task_id, lakehouse_config):
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

        # Check if lakehouse_config option is set to METADATA
        lhconfig = lakehouse_config  # Assuming highcon is a boolean variable

        if lhconfig == "METADATA":
            # Find the index of the markdown cell containing "THIS IS MARKDOWN"
            index_to_remove = None
            for i, cell in enumerate(nb.cells):
                if cell.cell_type == 'markdown' and fnmatch.fnmatch(cell.source, '*(Attach Default Lakehouse Markdown Cell)*'):
                    index_to_remove = i
                    break

            # Remove the found markdown cell and the next cell
            if index_to_remove is not None:
                nb.cells.pop(index_to_remove)
                if index_to_remove < len(nb.cells):
                    nb.cells.pop(index_to_remove)  # Remove the next cell if it exists

        # Write the notebook to a file
        target_file_name = notebook_file
        with io.open(file=notebook_dir + target_file_name, mode='w', encoding='utf-8') as f:
            try:
                nb_str = nbf.writes(nb)
                f.write(nb_str)
                progress.print(f"{target_file_name} updated", level=LogLevel.INFO)                
            except Exception as ex:
                progress.print(f"Error updating: {target_file_name}", level=LogLevel.ERROR)                
                raise ex


@staticmethod
def GetManifest(progress: ProgressConsoleWrapper):
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
def SortManifest(nodes_orig, progress: ProgressConsoleWrapper):
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
                    progress.print(f"Node {node_id} has no nodes attribute", level=LogLevel.WARNING)                    
            else :
                # If the node has no depends_on attribute, it has no dependencies
                has_dependency = False
                progress.print(f"Node {node_id} has no depends_on attribute", level=LogLevel.WARNING)                

            # If no dependency was found, add the node_id to the list
            if not has_dependency:
                nodes_without_deps.append(node_id)

        if not nodes_without_deps:
            progress.print(f"Nodes: {nodes}", level=LogLevel.ERROR)
            raise Exception('Circular dependency detected')
        # Assign the current sort order to the nodes without dependencies
        for node_id in nodes_without_deps:
            nodes_orig[node_id].sort_order = sort_order
            del nodes[node_id]
        # Increment the sort order
        sort_order += 1
    return nodes_orig
