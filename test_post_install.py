from dbt.adapters.fabricsparknb import utils as utils
import os 
import sys

Upload = False
if len(sys.argv) == 2:    
    os.environ['DBT_PROJECT_DIR'] = sys.argv[1] 
    utils.RunDbtProject(PreInstall=False,Upload=Upload)
elif len(sys.argv) == 3:  
    os.environ['DBT_PROJECT_DIR'] = sys.argv[1]   
    if sys.argv[2] == "1":
        Upload = True 
    utils.RunDbtProject(PreInstall=False,Upload=Upload)
elif len(sys.argv) == 4:    
    os.environ['DBT_PROJECT_DIR'] = sys.argv[1]   
    if sys.argv[2] == "1":
       Upload = True 
    os.environ['DBT_PROFILES_DIR'] = sys.argv[3]
    utils.RunDbtProject(PreInstall=False,Upload=Upload)
else:
    print("Please supply at least DBT project directory as a parameter.")

