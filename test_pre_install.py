from dbt.adapters.fabricsparknb import utils as utils
import os 
import sys

utils.RunDbtProjectArg(PreInstall=True,argv = sys.argv)