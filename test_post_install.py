from dbt.adapters.fabricsparknb import utils as utils
import os 
import sys


os.environ['DBT_PROJECT_DIR'] = sys.argv[1] 
if len(sys.argv) > 2:    
    os.environ['DBT_PROFILES_DIR'] = sys.argv[2]

utils.RunDbtProject(PreInstall=False)