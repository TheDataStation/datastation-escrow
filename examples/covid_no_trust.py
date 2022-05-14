import os
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

    start_time = time.time()
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    # files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    files = []
    DE_dir_name = os.listdir(mount_path)
    for i in range(len(DE_dir_name)):
        DE_dir_name[i] = os.path.join(str(mount_path), str(DE_dir_name[i]))
        files.append(os.path.join(str(DE_dir_name[i]), str(os.listdir(DE_dir_name[i])[0])))

    f_name_id_dict = {}
    for file in set(files):
        if pathlib.Path(file).is_file():
            f_name_short = file.split('/')[-1]
            f_id = file.split('/')[-2]
            f_name_id_dict[f_name_short] = f_id

    # # Checking that the dictionary is correct
    # print(f_name_id_dict)

    # Start the ML code

    # Read the metadata from training_covid.txt (an open file that everyone can read)

    train_size = 0.9

    train_df = pd.read_csv('integration_tests/test_metadata/train.txt', sep=" ", header=None)
    train_df.columns = ['patient id', 'filename', 'class', 'data source']
    train_df = train_df.drop(['patient id', 'data source'], axis=1)

    train_df, valid_df = train_test_split(train_df, train_size=train_size, random_state=0)

    # Now create the train_data and train_label that will be used for ImageDataGenerator.flow
    train_data = list()
    train_label = list()

    valid_data = list()
    valid_label = list()

    counter = 0

    dec_start = time.time()

    for _, row in train_df.iterrows():
        cur_file_path = os.path.join(str(mount_path), f_name_id_dict.get(row["filename"]), row["filename"])
        f = open(cur_file_path, "rb")
        cur_file_bytes = f.read()
        # These bytes should be decrpted already, we convert it to object
        image_np_arr = pickle.loads(cur_file_bytes)
        f.close()
        train_data.append(image_np_arr)
        if row["class"] == "positive":
            train_label.append(1)
        else:
            train_label.append(0)
        counter += 1
        print(counter)

    for _, row in valid_df.iterrows():
        cur_file_path = os.path.join(str(mount_path), f_name_id_dict.get(row["filename"]), row["filename"])
        f = open(cur_file_path, "rb")
        cur_file_bytes = f.read()
        # These bytes should be decrpted already, we convert it to object
        image_np_arr = pickle.loads(cur_file_bytes)
        f.close()
        valid_data.append(image_np_arr)
        if row["class"] == "positive":
            valid_label.append(1)
        else:
            valid_label.append(0)

    # print(np.shape(train_data))
    # print(np.shape(valid_data))
    # print(train_label)
    # print(valid_label)

    # Let's first finish the code here.

    print("data station overhead is", time.time() - start_time)
