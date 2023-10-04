# This script loads a state of the data station needed to test out income example in full_trust.
import pathlib
import sys
import os
import shutil
import numpy as np

from common import general_utils
from ds import DataStation
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # System initialization

    ds_config = general_utils.parse_config("data_station_config.yaml")
    app_config = general_utils.parse_config("app_connector_config.yaml")

    ds_storage_path = str(pathlib.Path(ds_config["storage_path"]).absolute())
    mount_point = str(pathlib.Path(ds_config["mount_path"]).absolute())

    ds = DataStation(ds_config, app_config)

    # Remove the code block below if testing out durability of log
    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # We upload 8 users, one holds each partition of the data (X and y)
    num_users = 8

    for cur_num in range(num_users):
        cur_uname = "user" + str(cur_num)
        ds.create_user(User(user_name=cur_uname, password="string"))

    # Clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # For each user, we upload his partition of the data (2 data elements, both X and y)
    # and the corresponding policy.
    data_id_counter = 1
    for cur_num in range(num_users):

        # Get the current username
        cur_uname = "user" + str(cur_num)

        # We have two types of data: X and y
        train_type = ["X", "y"]
        for cur_type in train_type:
            cur_train = "integration_tests/ml_file_full_trust/training_income/train" + str(cur_num) + "_" + cur_type + ".npy"
            cur_file = open(cur_train, "rb")
            cur_file_bytes = cur_file.read()
            cur_optimistic_flag = False
            name_to_upload = "train" + str(cur_num) + "_" + cur_type + ".npy"
            # Upload data
            ds.register_de(cur_uname,
                           name_to_upload,
                           cur_file_bytes,
                              "file",
                           cur_optimistic_flag,
                           )
            cur_file.close()
            # Upload policy
            ds.upload_policy(cur_uname, Policy(user_id=1, api="train_income_model", data_id=data_id_counter))
            data_id_counter += 1

    # In here, DB construction is done. We just need to call train_income_model

    # Use token for user0
    res_model = ds.call_api("user0",
                            "train_income_model",
                            "optimistic")
    # print("Model returned is: ")
    # print(res_model.coef_, res_model.intercept_)

    # After we get the model back, we test its accuracy
    x_test = np.load("integration_tests/ml_file_full_trust/testing_income/test_X.npy")
    y_test = np.load("integration_tests/ml_file_full_trust/testing_income/test_y.npy")
    accuracy = res_model.score(x_test, y_test)
    print("Model accuracy is "+str(accuracy))

    ds.shut_down()
