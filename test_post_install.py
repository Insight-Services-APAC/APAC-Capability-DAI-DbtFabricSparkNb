from dbt.adapters.fabricsparknb import utils as utils
import os 
import sys

utils.RunDbtProjectArg(PreInstall=False,argv = sys.argv)


