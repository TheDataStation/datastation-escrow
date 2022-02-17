import os.path
import pathlib

from dsapplicationregistration import register
import glob
from common import utils


@register()
def preprocess():
    """preprocess all the data"""
    print("preprocess called")
    # print(pathlib.Path(__file__).parent.resolve())
    # print(os.getcwd())
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    # print(set(files))
    for file in set(files):
        # print(file)
        if pathlib.Path(file).is_file():
            with open(file, "r") as cur_file:
                content = cur_file.readline()
                # print(content)
            print("read ", file)

    return "preprocess result"


@register(depends_on=[preprocess])
def modeltrain():
    """trains the model"""
    res = preprocess()
    print("modeltrain called")
    return "modeltrain result"


@register(depends_on=[modeltrain])
def predict(accuracy: int,
            num_times: int):
    """submits input to get predictions"""
    res = modeltrain()
    print("Prediction accuracy is " + str(accuracy) + " percent :(")
    print("Please try " + str(num_times) + " times more!")
    return "predict result"