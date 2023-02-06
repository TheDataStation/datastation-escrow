import glob
import os
import pathlib

from dsapplicationregistration import register
from common import general_utils

@register()
def f1():
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = set(glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True))

    contents = {}
    for file in files:
        # print(file)
        if pathlib.Path(file).is_file():
            with open(file, "rb") as cur_file:
                content = cur_file.read().rstrip(b'\x00')
                contents[file] = content
            # print("read ", file)
    return contents

@register()
def f2():
    return f1()

@register()
def f3():
    return f1()

@register()
def f4():
    return f1()

@register()
def f5():
    return f1()