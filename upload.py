from dbt.adapters.fabricsparknb import utils as utils
import os

os.environ['DBT_PROJECT_DIR'] = "./testproj"
utils.UploadAllNotebooks()