import dbt.cli
import dbt.cli.flags
import dbt.config.renderer
import dbt.config.utils
import dbt.docs.source.conf
import dbt.parser
import dbt.parser.manifest
import dbt.tests.util
import dbt.utils
import dbt
import dbt.adapters.fabricspark
import dbt.adapters.fabricsparknb
from dbt.adapters.fabricsparknb import utils as utils
import dbt.tests
from pathlib import Path
import os
import shutil

import dbt.docs

import dbt.docs.source

os.environ['DBT_PROJECT_DIR'] = "testproj"

profile_path = Path(os.path.expanduser('~')) / '.dbt/'
profile = dbt.config.profile.read_profile(profile_path)
config = dbt.config.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])
profile_info = profile[config['profile']]
target_info = profile_info['outputs'][profile_info['target']]
print(target_info['lakehouse'])
lakehouse = target_info['lakehouse']


shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
utils.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])

dbt.tests.util.run_dbt(['seed'])


utils.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
utils.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])
utils.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse, config['name'])


