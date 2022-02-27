# This script loads a state of the data station needed to test out ML example
import pathlib
import sys
import main
import os
import shutil
import time
import math
import random
from csv import writer
import numpy as np

from Interceptor import interceptor
from common import utils
from models.user import *
from models.policy import *
from common.utils import parse_config

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # Read in the configuration file
    test_config = parse_config(sys.argv[1])

    # System initialization

    ds_config = utils.parse_config("data_station_config.yaml")
    app_config = utils.parse_config("app_connector_config.yaml")

    ds_storage_path = str(pathlib.Path(ds_config["storage_path"]).absolute())
    mount_point = str(pathlib.Path(ds_config["mount_path"]).absolute())

    client_api = main.initialize_system(ds_config, app_config)

    # Remove the code block below if testing out durability of log
    log_path = client_api.log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # We upload 8 users, one holds each partition of the data (X and y)
    num_users = 8

    for cur_num in range(num_users):
        cur_uname = "user" + str(cur_num)
        client_api.create_user(User(user_name=cur_uname, password="string"))

    # Clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # For each user, we upload his partition of the data (2 data elements, both X and y)
    for cur_num in range(num_users):

        # Log in the current user and get a token
        cur_uname = "user" + str(cur_num)
        cur_token = client_api.login_user(cur_uname, "string")["access_token"]

        # Upload his partition X of the data
        cur_train_X = "test/ml_file_full_trust/training_income/train" + str(cur_num) + "_X.npy"
        cur_file_X = open(cur_train_X, "rb")
        cur_file_bytes = cur_file_X.read()
        cur_optimistic_flag = False
        name_to_upload = "train" + str(cur_num) + "_X.npy"
        cur_res = client_api.upload_dataset(name_to_upload,
                                            cur_file_bytes,
                                            "file",
                                            cur_optimistic_flag,
                                            cur_token, )
        cur_file_X.close()

        # Upload his partition y of the data
        cur_train_y = "test/ml_file_full_trust/training_income/train" + str(cur_num) + "_y.npy"
        cur_file_y = open(cur_train_y, "rb")
        cur_file_bytes = cur_file_y.read()
        cur_optimistic_flag = False
        name_to_upload = "train" + str(cur_num) + "_y.npy"
        cur_res = client_api.upload_dataset(name_to_upload,
                                            cur_file_bytes,
                                            "file",
                                            cur_optimistic_flag,
                                            cur_token, )
        cur_file_y.close()

        # Add a policy saying user with id==1 can call train_income_model on the datasets
        client_api.upload_policy(Policy(user_id=1, api="train_income_model", data_id=cur_num*2+1), cur_token)
        client_api.upload_policy(Policy(user_id=1, api="train_income_model", data_id=cur_num*2+2), cur_token)

    # In here, DB construction is done. We just need to call train_income_model

    # Use token for user0
    cur_token = client_api.login_user("user0", "string")["access_token"]
    res_model = client_api.call_api("train_income_model", cur_token, "optimistic")
    print(res_model)
    print("Model returned is: ")
    print(res_model.coef_, res_model.intercept_)

    # After we get the model back, we test its accuracy
    x_test = np.load("test/ml_file_full_trust/testing_income/test_X.npy")
    y_test = np.load("test/ml_file_full_trust/testing_income/test_y.npy")
    accuracy = res_model.score(x_test, y_test)
    print("Model accuracy is "+str(accuracy))

    client_api.shut_down(ds_config)
