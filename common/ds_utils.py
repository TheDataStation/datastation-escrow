import os
import pathlib
from common import general_utils

# Returns the all the file names that are accessible to the caller (in absolute paths)
def get_all_files():
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = general_utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = []
    DE_dir_name = os.listdir(mount_path)
    for i in range(len(DE_dir_name)):
        DE_dir_name[i] = os.path.join(str(mount_path), str(DE_dir_name[i]))
        if len(os.listdir(DE_dir_name[i])) > 0:
            files.append(os.path.join(str(DE_dir_name[i]), str(os.listdir(DE_dir_name[i])[0])))
    return files
