import dbt.cli
import dbt.cli.flags
import dbt.config.renderer
import dbt.config.utils
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

os.environ['DBT_PROJECT_DIR'] = "ptfproj"
curr_dir = os.getcwd()

profile_path = Path(os.path.expanduser('~')) / '.dbt/'
profile = dbt.config.profile.read_profile(profile_path)
config = dbt.config.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])
profile_info = profile[config['profile']]
target_info = profile_info['outputs'][profile_info['target']]
lakehouse = target_info['lakehouse']

dbt_project_dir = os.path.join(curr_dir,os.environ['DBT_PROJECT_DIR'])

shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
utils.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])

dbt.tests.util.run_dbt(['build'])

utils.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.IPYNBtoFabricPYFile(dbt_project_dir)

