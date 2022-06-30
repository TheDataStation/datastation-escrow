import glob
import yaml
import pathlib
import os

def parse_config(path_to_config):
    with open(path_to_config) as config_file:
        res_config = yaml.load(config_file, Loader=yaml.FullLoader)
    return res_config

def get_all_files():
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = set(glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True))

    results = []
    for file in files:
        # print(file)
        if pathlib.Path(file).is_file():
            results.append(file)

    return results