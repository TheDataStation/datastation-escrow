import os
import pathlib
from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from ds import DataStation


# Returns the all the file names that are accessible to the caller (in absolute paths)
def get_all_files():
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = general_utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    print(mount_path)
    files = []
    DE_dir_name = os.listdir(mount_path)
    print(mount_path)
    print(DE_dir_name)
    for i in range(len(DE_dir_name)):
        DE_dir_name[i] = os.path.join(str(mount_path), str(DE_dir_name[i]))
        if len(os.listdir(DE_dir_name[i])) > 0:
            files.append(os.path.join(str(DE_dir_name[i]), str(os.listdir(DE_dir_name[i])[0])))
    return files


# Returns the files names accessible to the caller in absolute paths, specified by a list.
def get_specified_files(DE_id):
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = general_utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = []
    for cur_id in DE_id:
        DE_dir_name = os.path.join(str(mount_path), str(cur_id))
        files.append(os.path.join(str(DE_dir_name), str(os.listdir(DE_dir_name)[0])))
    return files


# Default implementation for upload data.
def upload_dataset(ds: DataStation,
                   username,
                   data_name,
                   data_in_bytes,
                   data_type,
                   optimistic,
                   original_data_size=None):
    ds.upload_dataset(username, data_name, data_in_bytes, data_type, optimistic, original_data_size)

# Default implementation for upload policy.
def upload_policy(ds: DataStation, username, policy: Policy):
    ds.upload_policy(username, policy)
