import os.path
import pathlib
import pickle
import time

from dsapplicationregistration import register
import glob
from common import utils

@register()
def train_covid_model():
    print("starting covid model")
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    f_name_in_order = []
    for file in set(files):
        if pathlib.Path(file).is_file():
            f_name_in_order.append(file)
    f_name_in_order.sort(key=lambda x: int(x.split('/')[-2]))
    print(f_name_in_order)

