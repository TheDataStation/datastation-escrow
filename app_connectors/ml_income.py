import os.path
import pathlib
import numpy as np
import pickle
from sklearn.linear_model import LogisticRegression

from dsapplicationregistration import register
import glob
from common import utils

# This version here is for full_trust. We may or may not need this.

# @register()
# def train_income_model():
#     """train a logistic regression model on income data"""
#     print("starting train model")
#     ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
#     ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
#     mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
#     files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
#     Xtrains = []
#     ytrains = []
#     for file in set(files):
#         if pathlib.Path(file).is_file():
#             array_to_append = file.split('/')[-1].split('.')[0][-1]
#             cur_np_ele = np.load(file)
#             if array_to_append == "X":
#                 Xtrains.append(cur_np_ele)
#             else:
#                 ytrains.append(cur_np_ele)
#     Xtrains = np.concatenate(Xtrains)
#     ytrains = np.concatenate(ytrains)
#     income_model = LogisticRegression()
#     income_model.fit(Xtrains, ytrains)
#     print(income_model)
#     return income_model

# This version here is for no_trust.

# @register()
# def train_income_model():
#     """train a logistic regression model on income data"""
#     print("starting train model")
#     ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
#     ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
#     mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
#     files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
#     # Xtrains = []
#     # ytrains = []
#     print(set(files))
#     for file in set(files):
#         if pathlib.Path(file).is_file():
#             print(file)
#             print(os.path.getsize(file))
#             f = open(file, "rb")
#             cur_file_bytes = f.read()
#             f.close()
#             # with open(file, "rb") as f:
#             #     cur_file_bytes = f.read()
#             # cur_file = open(file, "rb")
#             # cur_file_bytes = cur_file.read()
#             # cur_file.close()
#             print("file operation is done")
#             # These bytes should be decrpted already, we convert it to object
#             np_obj = pickle.loads(cur_file_bytes)
#             print(np_obj)
#             # array_to_append = file.split('/')[-1].split('.')[0][-1]
#             # cur_np_ele = np.load(file)
#             # if array_to_append == "X":
#             #     Xtrains.append(cur_np_ele)
#             # else:
#             #     ytrains.append(cur_np_ele)
#     # Xtrains = np.concatenate(Xtrains)
#     # ytrains = np.concatenate(ytrains)
#     # income_model = LogisticRegression()
#     # income_model.fit(Xtrains, ytrains)
#     # return income_model

@register()
def train_income_model():
    """train a logistic regression model on income data"""
    print("starting train model")
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    Xtrains = []
    ytrains = []
    for file in set(files):
        if pathlib.Path(file).is_file():
            print(file)
            cur_np_ele = np.load(file)
            # array_to_append = file.split('/')[-1].split('.')[0][-1]
            # cur_np_ele = np.load(file)
            # if array_to_append == "X":
            #     Xtrains.append(cur_np_ele)
            # else:
            #     ytrains.append(cur_np_ele)
    # Xtrains = np.concatenate(Xtrains)
    # ytrains = np.concatenate(ytrains)
    # income_model = LogisticRegression()
    # income_model.fit(Xtrains, ytrains)
    # print(income_model)
    # return income_model