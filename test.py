from dataclasses import dataclass
from logging import makeLogRecord
import dbt.cli
import dbt.cli.flags
import dbt.config.renderer
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
from dbt.config import RuntimeConfig
from dbt.cli.flags import Flags, convert_config
from dbt.cli.main import cli
import dbt.config



os.environ['DBT_PROJECT_DIR'] = "./testproj"

#v = dbt.tests.util.get_project_config()

dbt.tests.util.run_dbt(['run'])

utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'])


