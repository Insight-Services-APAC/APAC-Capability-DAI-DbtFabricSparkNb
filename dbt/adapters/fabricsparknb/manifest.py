"""This module contains the Manifest class for creating and managing the manifest file in the sbt_utils module."""
from collections import deque
from dataclasses import dataclass, field
import json
import re
import copy
import dbt
import jsonpickle # type: ignore
from dbt.parser.manifest import ManifestLoader


class ManifestOperations:
    """tba"""
    def __init__():
        ManifestLoader.get_full_manifest()
    
    @dataclass
    class Model:
        """Class for representing a model in the manifest file."""
        # In use DBT model fields below
        database: str = ""
        schema: str = ""
        name: str = ""
        resource_type: str = "model"
        config:dict  = field(default_factory=dict)        
        refs: list = field(default_factory=list)
       

        # Unused DBT model fields below
        unique_id: str = ""
        tags: list = field(default_factory=list)
        package_name: str = ""
        path: str = ""
        original_file_path: str = ""
        
        fqn: str = ""
        alias: str = ""
        checksum: str = ""
        _event_status: str = ""
        description: str = ""
        columns: list = field(default_factory=list)
        meta: dict = field(default_factory=dict)
        group: str = ""
        docs: dict = field(default_factory=dict)
        patch_path: str = ""
        build_path: str = ""
        deferred: bool = False
        unrendered_config: dict = field(default_factory=dict)
        created_at: str = ""
        config_call_dict: dict = field(default_factory=dict)
        relation_name: str = ""
        raw_code: str = ""
        language: str = "sql"
        
        sources: dict = field(default_factory=dict)
        metrics: dict = field(default_factory=dict)
        depends_on: dict = field(default_factory=dict)
        compiled_path: str = ""
        compiled: bool = False
        compiled_code: str = ""
        extra_ctes_injected: bool = False
        extra_ctes: list = field(default_factory=list)
        _pre_injected_sql: str = ""
        contract: dict = field(default_factory=dict)
        access: dict = field(default_factory=dict)
        constraints: dict = field(default_factory=dict)
        version: str = ""
        latest_version: str = ""
        deprecation_date: str = ""
        defer_relation: bool = False
        root_path: str = ""
        
        #Sbt specific fields encapsulated in sbt_model attribute.. where possible move these to standard dbt model fields
        sbt_model: "Manifest.SbtModel" = field(default_factory="Manifest.SbtModel")

    @dataclass
    class SbtModel:
        """Class for representing an SBT model node in the manifest file."""
        name: str = ""
        model: str = ""
        refs: list[str] = field(default_factory=list)
        directory: str = ""
        run_order: int = 0
        last_execution_error: str = ""
        last_execution_status: str = ""
        targetdir: str = ""
        prerun_targetdir: str = ""
        rendered_sql: str = ""
        runsql: str = ""
        sql: str = "" 
    
    @dataclass
    class TopologicalSortNode:
        """Class for representing a node in the topological sort."""
        name: str
        refs: list[str]
        run_order: int = 0
    
    @dataclass
    class ManifestFile:
        """Class for representing the manifest file in the sbt_utils module."""
        metadata: dict = field(default_factory=dict)
        nodes: dict = field(default_factory=dict)
        sources: dict = field(default_factory=dict)
        metrics: dict = field(default_factory=dict)
        exposures: dict = field(default_factory=dict)
        groups: dict = field(default_factory=dict)
        macros: dict = field(default_factory=dict)
        docs: dict = field(default_factory=dict)
        parent_map: dict = field(default_factory=dict)
        child_map: dict = field(default_factory=dict)
        group_map: dict = field(default_factory=dict)
        selectors: dict = field(default_factory=dict)
        disabled: list = field(default_factory=list)        
    
   

    def save_manifest(self, project_directory):
        """Save the manifest to a file."""
        # print(project_directory + '/target/manifest.json')
        self.storage.write_file(project_directory + '/target/manifest.json',
                                jsonpickle.encode(self.manifest_file, indent=4, unpicklable=False))

    def check_predecessors(self, node_name):
        """Check if all predecessors of a node have succeeded."""
        # Find the node in the manifest
        node:Manifest.Model = next((n[1] for n in self.manifest_file.nodes.items() if n[1].name == node_name), None)
        if not node:
            raise ValueError(f"Node {node_name} not found in manifest")

        ret = all(next((n[1].sbt_model.last_execution_status == "Succeeded" for n in self.manifest_file.nodes.items() if n[1].name == ref), False) for ref in node.refs)


        # Check if all predecessors have succeeded
        return ret 
    def load_manifest(self, manifest_file):
        """Load the manifest from a file."""
        manifest_data = self.storage.get_file_content(manifest_file)
        manifest_json = json.loads(manifest_data)
        self.manifest_file = Manifest.ManifestFile(**manifest_json)
        # Iterate through nodes and cast to Model
        for node in self.manifest_file.nodes.items():
            self.manifest_file.nodes[node[0]] = Manifest.Model(**node[1])
            # Iterate through sbt_model and cast to SbtModel
            self.manifest_file.nodes[node[0]].sbt_model = Manifest.SbtModel(**self.manifest_file.nodes[node[0]].sbt_model)
        print("")
        
    def create_manifest(self, project_directory="")-> None:
        """Create the manifest file for the project by reading the models and seeds directories."""        
        self.manifest_file = Manifest.ManifestFile()
        # The regular expression pattern to match
        pattern = r'{{\s*ref\([\'"]([^\'"]*)[\'"]\)\s*}}'
        
        # Walk through all files in the directory
        files = self.storage.get_files_from_directory_recursive(
            project_directory + '/models')
        for file in files:
            file.directory = file.directory.replace("\\","/")
            references = []
            # Open each file
            contents = self.storage.get_file_content(
                file.directory + '/' + file.name)
            # Find all matches of the pattern in the contents
            matches = re.findall(pattern, contents)
            # Add the matches to the list of references
            references.extend(matches)
            references = [reference for reference in references]
            sbt_model = Manifest.SbtModel(name=file.name, model=file.name.split(
                ".")[-2], refs=references, directory=file.directory)
            self.manifest_file.nodes[file.name.split(".")[-2]] = Manifest.Model(
                name=file.name.split(".")[-2],
                refs=references,
                database="dbt",
                schema="schema",
                path = (file.directory + '/' + file.name).replace("\\","/"),
                original_file_path=(file.directory + '/' + file.name).replace("\\","/"),
                resource_type="model",
                sbt_model=sbt_model
            )

        # Add in the seeds
        seeds = self.storage.get_files_from_directory_recursive(
            project_directory + '/seeds')
        for file in seeds:
            sbt_model = Manifest.SbtModel(name=file.name, model=file.name.split(
                ".")[-2], refs=[], run_order=0, directory=file.directory)
            self.manifest_file.nodes[file.name.split(".")[-2]] = Manifest.Model(
                name=file.name.split(".")[-2],
                refs=[],
                database="dbt",
                schema="schema",
                path = file.directory + '/' + file.name,
                original_file_path=file.directory + '/' + file.name,
                resource_type="seed",
                sbt_model=sbt_model
            )
        tsort_nodes = []
        nodes_copy = copy.deepcopy(self.manifest_file.nodes)
        for node in nodes_copy.items():
            tsort_nodes.append(Manifest.TopologicalSortNode(name=node[1].name, refs=node[1].refs, run_order=0))

        #tsort_nodes = copy.deepcopy(self.manifest_file.nodes)
        tsort_nodes = Manifest.topological_sort(tsort_nodes)
        for node in self.manifest_file.nodes.items():
            # find node in tsort_nodes and add to manifest
            node[1].sbt_model.run_order = [
                tsnode.run_order for tsnode in tsort_nodes if tsnode.name == node[1].name][0]
            
        print("Manifest created successfully")


    @staticmethod
    def topological_sort(nodes):
        """Perform a topological sort on the nodes."""
        
        # Create a dictionary of nodes and their references
        # If a node has no references, assign an empty list to it
        node_refs = {node.name: node.refs if node.refs else [] for node in nodes}

        # Create a queue of nodes without any references
        # Each item in the queue is a tuple containing a node and its run order
        queue = deque((node, 0) for node in nodes if len(node.refs)==0)

        # Process the queue until it's empty
        while queue:
            # Pop a node and its run order from the queue
            node, run_order = queue.popleft()

            # Assign the run order to the node
            node.run_order = run_order

            # Loop through the nodes to find any references to the current node
            for other_node in nodes:
                # Check if the other node has references and the current node is one of them
                if other_node.refs and node.name in other_node.refs:
                    # Remove the current node from the references of the other node
                    other_node.refs.remove(node.name)

                    # Check if the other node has no more references, its run order is 0, and it's not already in the queue
                    if not other_node.refs and other_node.run_order == 0 and other_node not in [n for n, _ in queue]:
                        # Add the other node to the queue with a run order one greater than the current node's run order
                        queue.append((other_node, run_order + 1))

        # Return the nodes with their run orders assigned
        return nodes
