import csv
import os.path
import pathlib

from dsapplicationregistration import register
import glob
from common import general_utils

@register
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    for file in set(files):
        if pathlib.Path(file).is_file():
            csv_file = open(file)
            reader = csv.reader(csv_file)
            print(len(list(reader)))
