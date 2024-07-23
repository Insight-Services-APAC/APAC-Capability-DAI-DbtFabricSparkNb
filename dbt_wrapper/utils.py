
import os
from pathlib import Path
from sysconfig import get_paths


@staticmethod
def PureLibIncludeDirExists():
    ChkPath = Path(get_paths()['purelib']) / Path('dbt/include/fabricsparknb/')
    return os.path.exists(ChkPath)


@staticmethod
def GetIncludeDir():
    ChkPath = Path(get_paths()['purelib']) / Path('dbt/include/fabricsparknb/')
    # print(ChkPath)
    # Does Check for the path
    if os.path.exists(ChkPath):
        return ChkPath
    else:
        path = Path(os.getcwd()) / Path('dbt/include/fabricsparknb/')
        # print(str(path))
        return (path)
