from dataclasses import dataclass
from logging import makeLogRecord
import dbt.cli
import dbt.cli.flags
import dbt.config.renderer
import dbt.config.utils
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
from pathlib import Path
import os
import json
from dbt.contracts.graph.manifest import Manifest
from dbt.clients.system import load_file_contents
from collections import deque
from dbt.config import RuntimeConfig
from dbt.cli.flags import Flags, convert_config
from dbt.cli.main import cli




os.environ['DBT_PROJECT_DIR'] = "./testproj"

dbt.tests.util.run_dbt(['build'])


profile_path = Path(os.environ['USERPROFILE']) / '.dbt/'
profile = dbt.config.profile.read_profile(profile_path)
config = dbt.config.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])
profile_info = profile[config['profile']]
target_info = profile_info['outputs'][profile_info['target']]
lakehouse = target_info['lakehouse']
workspacename = target_info['workspacename']
lakehousedatapath = target_info['lakehousedatapath'] 
lakehousedatapathfull = lakehouse+".Lakehouse"+lakehousedatapath
utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'])
# Note: for following to run you must have access to the following fabric workspace and datalake
# add profile.yml must be same structure as assets\profiles.yml in this repo note new attributes have been added 
utils.UploadAllNotebooks(workspacename,lakehousedatapathfull)  ##uploads notebooks to onelake
# spark notebook to conevrt uploaded files into notebooks is assets\PySparkIngestNotebooks.ipynb
# the above notebook needs assets\assets\fabricnotebookutil.py


