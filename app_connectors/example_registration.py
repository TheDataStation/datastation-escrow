import os.path
import pathlib

from dsapplicationregistration import register
import glob
from common import utils

@register()
def preprocess():
    """preprocess all the data"""
    print("preprocess called")
    # files = glob.glob("/Users/zhiruzhu/Desktop/data_station/DataStation/SM_storage/**/*", recursive=True)
    ds_config = utils.parse_config("data_station_config.yaml")
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    # print(set(files))
    for file in set(files):
        # print(file)
        if pathlib.Path(file).is_file():
            with open(file, "r") as cur_file:
                cur_file.readline()
            print("read ", file)

    return 0


@register(depends_on=[preprocess])
def modeltrain():
    """trains the model"""
    res = preprocess()
    print("modeltrain called")
    return 0


@register(depends_on=[modeltrain])
def predict(accuracy: int,
            num_times: int):
    """submits input to get predictions"""
    res = modeltrain()
    print("Prediction accuracy is "+str(accuracy)+" percent :(")
    print("Please try "+str(num_times)+" times more!")
    return 0
