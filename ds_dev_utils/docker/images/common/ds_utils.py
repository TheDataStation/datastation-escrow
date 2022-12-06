import os
import pathlib

# Returns the all the file names that are accessible to the caller (in absolute paths)
def get_all_files():
    # ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    # ds_config = general_utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    # mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    mount_path = pathlib.Path("/mnt/data")
    files = []
    DE_dir_name = os.listdir(mount_path)
    for i in range(len(DE_dir_name)):
        DE_dir_name[i] = os.path.join(str(mount_path), str(DE_dir_name[i]))
        if len(os.listdir(DE_dir_name[i])) > 0:
            files.append(os.path.join(str(DE_dir_name[i]), str(os.listdir(DE_dir_name[i])[0])))
    return files

# Returns the files names accessible to the caller, specified by a list.
def get_specified_files(DE_id):
    # ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    # ds_config = general_utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    # mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    mount_path = pathlib.Path("/mnt/data")
    files = []
    for cur_id in DE_id:
        DE_dir_name = os.path.join(str(mount_path), str(cur_id))
        files.append(os.path.join(str(DE_dir_name), str(os.listdir(DE_dir_name)[0])))
    return files
