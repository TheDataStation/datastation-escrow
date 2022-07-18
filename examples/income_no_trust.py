# Logistic regression model for the income dataset, no trust version.

import os.path
import pathlib
import numpy as np
from sklearn.linear_model import LogisticRegression
import time
import pickle

from dsapplicationregistration import register
import glob
from common import general_utils

@register()
def train_income_model():
    """train a logistic regression model on income data"""
    print("starting income model")
    prev_time = time.time()
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = general_utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    Xtrains = []
    ytrains = []
    f_name_in_order = []
    for file in set(files):
        if pathlib.Path(file).is_file():
            f_name_in_order.append(file)
    f_name_in_order.sort(key=lambda x: int(x.split('/')[-2]))
    for file in f_name_in_order:
        f = open(file, "rb")
        cur_file_bytes = f.read()
        f.close()
        # These bytes should be decrpted already, we convert it to object
        cur_np_ele = pickle.loads(cur_file_bytes)
        array_to_append = file.split('/')[-1].split('.')[0][-1]
        if array_to_append == "X":
            Xtrains.append(cur_np_ele)
        else:
            ytrains.append(cur_np_ele)
    Xtrains = np.concatenate(Xtrains)
    ytrains = np.concatenate(ytrains)
    income_model = LogisticRegression()
    income_model.fit(Xtrains, ytrains)
    print(time.time()-prev_time)

    # f = open("demofile.txt", "w")
    # f.write("Writing new content!")
    # f.close()

    return income_model
