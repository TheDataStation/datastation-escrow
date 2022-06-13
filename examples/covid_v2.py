import os
import pathlib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from dsapplicationregistration import register
from common import utils

@register()
def covid_v2():
    print("Start covid model version two")

    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = []
    DE_dir_name = os.listdir(mount_path)
    for i in range(len(DE_dir_name)):
        DE_dir_name[i] = os.path.join(str(mount_path), str(DE_dir_name[i]))
        files.append(os.path.join(str(DE_dir_name[i]), str(os.listdir(DE_dir_name[i])[0])))
    # print(files)

    # Start the ML code

    # Read the metadata from train.txt (an open file that everyone can read)

    train_size = 0.9

    train_df = pd.read_csv('integration_tests/test_metadata/train.txt', sep="\s+", header=None)
    train_df.columns = ['patient id', 'filename', 'class', 'data source']
    train_df = train_df.drop(['patient id', 'data source'], axis=1)

    train_df, valid_df = train_test_split(train_df, train_size=train_size, random_state=0)

    # Let's try to use flow_from_dataframe, but change the content in the dataframe to be their absolute paths

    # Build dictionary from file name to absolute path
    name_path_dict = {}
    for f in files:
        f_name = f.split("/")[-1]
        name_path_dict[f_name] = f

    # update the df columns
    for _, row in train_df.iterrows():
        row['filename'] = name_path_dict[row['filename']]

    # right before flow_from_dataframe
