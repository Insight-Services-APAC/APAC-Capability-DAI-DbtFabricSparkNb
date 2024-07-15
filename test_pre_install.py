from dbt.adapters.fabricsparknb import utils as utils
import os 
import sys

os.environ['DBT_PROJECT_DIR'] = sys.argv[1] 
utils.RunDbtProject(PreInstall=True)