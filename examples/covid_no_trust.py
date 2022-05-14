import os.path
import pathlib
import numpy as np
import pandas as pd
import pickle
import time
from sklearn.model_selection import train_test_split
from PIL import Image
import io


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
    f_name_id_dict = {}
    for file in set(files):
        if pathlib.Path(file).is_file():
            f_name_short = file.split('/')[-1]
            f_id = file.split('/')[-2]
            f_name_id_dict[f_name_short] = f_id

    # # Checking that the dictionary is correct
    # print(f_name_id_dict)

    # Start the ML code

    # Read the metadata from train.txt (an open file that everyone can read)

    train_size = 0.75

    train_df = pd.read_csv('integration_tests/test_metadata/train.txt', sep=" ", header=None)
    train_df.columns = ['patient id', 'filename', 'class', 'data source']
    train_df = train_df.drop(['patient id', 'data source'], axis=1)

    train_df, valid_df = train_test_split(train_df, train_size=train_size, random_state=0)

    # Now create the train_data and train_label that will be used for ImageDataGenerator.flow
    train_data = list()
    train_label = list()

    valid_data = list()
    valid_label = list()

    for _, row in train_df.iterrows():
        cur_file_path = os.path.join(str(mount_path), f_name_id_dict.get(row["filename"]), row["filename"])
        f = open(cur_file_path, "rb")
        cur_file_bytes = f.read()
        # These bytes should be decrpted already, we convert it to object
        image_np_arr = pickle.loads(cur_file_bytes)
        f.close()
        image = Image.fromarray(image_np_arr)
        image.show()
        if row["class"] == "positive":
            train_label.append(1)
        else:
            train_label.append(0)

    for _, row in valid_df.iterrows():
        if row["class"] == "positive":
            valid_label.append(1)
        else:
            valid_label.append(0)

    print(train_label)
    print(valid_label)

