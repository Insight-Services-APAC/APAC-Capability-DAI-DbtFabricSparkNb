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
workspacename = target_info['workspacename']


lakehousedatapath = target_info['lakehousedatapath']
lakehousedatapathfull = lakehouse + ".Lakehouse" + lakehousedatapath

shutil.rmtree(os.environ['DBT_PROJECT_DIR'] + "/target")
utils.GenerateAzCopyScripts(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'])
# Call dbt build
subprocess.run(["dbt", "build"], check=True)

utils.SetSqlVariableForAllNotebooks(os.environ['DBT_PROJECT_DIR'], lakehouse)
utils.GenerateMasterNotebook(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateMetadataExtract(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)
utils.GenerateNotebookUpload(os.environ['DBT_PROJECT_DIR'], target_info['workspaceid'], target_info['lakehouseid'], lakehouse)

# Note: for following to run you must have access to the following fabric workspace and datalake
# add profile.yml must be same structure as assets\profiles.yml in this repo note new attributes have been added
# utils.UploadAllNotebooks(workspacename,lakehousedatapathfull)  ##uploads notebooks to onelake
# spark notebook to conevrt uploaded files into notebooks is assets\PySparkIngestNotebooks.ipynb
# the above notebook needs assets\assets\fabricnotebookutil.py
