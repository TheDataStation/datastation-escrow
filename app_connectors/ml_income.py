import os.path
import pathlib
import numpy as np

from dsapplicationregistration import register
import glob
from common import utils


@register()
def train_income_model():
    """train a logistic regression model on income data"""
    print("starting train model")
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    for file in set(files):
        if pathlib.Path(file).is_file():
            print(file)
            print(np.load(file))
    return "result"
