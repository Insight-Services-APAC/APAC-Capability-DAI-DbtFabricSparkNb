import subprocess
from dbt.adapters.fabricsparknb import utils as utils
import dbt
from pathlib import Path
import os
import shutil

os.environ['DBT_PROJECT_DIR'] = "testproj"

profile_path = Path(os.path.expanduser('~')) / '.dbt/'
profile = dbt.config.profile.read_profile(profile_path)
config = dbt.config.project.load_raw_project(os.environ['DBT_PROJECT_DIR'])

profile_info = profile[config['profile']]
target_info = profile_info['outputs'][profile_info['target']]
lakehouse = target_info['lakehouse']

shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
utils.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])
# Call dbt build
subprocess.run(["dbt", "build"], check=True)

utils.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)


