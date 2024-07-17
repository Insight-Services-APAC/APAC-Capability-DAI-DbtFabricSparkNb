from dbt.adapters.fabricsparknb import utils as utils
import os 
import sys

os.environ['DBT_PROJECT_DIR'] = sys.argv[1] 
if len(sys.argv) > 2:    
    print("setting")
    os.environ['DBT_PROFILES_DIR'] = sys.argv[2]

utils.RunDbtProject(PreInstall=True)