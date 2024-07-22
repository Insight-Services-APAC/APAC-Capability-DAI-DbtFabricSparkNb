import io
import json


@staticmethod
def GetMetaHashes(project_root):
    # Open the file
    with io.open(project_root + '/metaextracts/MetaHashes.json', 'r') as file:
        # Load JSON data from file
        data = json.load(file)

    return data
