from dataclasses import dataclass
from logging import makeLogRecord
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
from collections import deque

os.environ['DBT_PROJECT_DIR'] = "./testproj"

dbt.tests.util.run_dbt(['run'])

utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'])


# Specify the path to your manifest file
manifest_path = os.environ['DBT_PROJECT_DIR'] + '/target/manifest.json'

# Load the file contents
file_contents = load_file_contents(manifest_path)

# Parse the JSON content into a dictionary
data = json.loads(file_contents)

# Convert the dictionary into a Manifest object
manifest = Manifest.from_dict(data)

def assign_sort_order(nodes_orig):
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

nodes_copy = assign_sort_order(manifest.nodes)


# Print the nodes with their sort orders
for node in manifest.nodes.values():
    print(node.unique_id, node.sort_order)