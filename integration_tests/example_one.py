import pathlib
import sys
import main
import os
import shutil
import time
import math
import random

from common import utils
from models.user import User
from models.policy import Policy
from common.utils import parse_config

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # Step 0: System initialization

    ds_config = utils.parse_config("data_station_config.yaml")
    app_config = utils.parse_config("app_connector_config.yaml")

    ds_storage_path = str(pathlib.Path(ds_config["storage_path"]).absolute())
    mount_point = str(pathlib.Path(ds_config["mount_path"]).absolute())

    client_api = main.initialize_system(ds_config, app_config)

    log_path = client_api.log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Step 1: We create two new users of the Data Station: jerry and david
    client_api.create_user(User(user_name="jerry", password="string"))
    client_api.create_user(User(user_name="david", password="123456"))

    # Step 2: Jerry logs in and uploads three datasets
    cur_token = client_api.login_user("jerry", "string")["access_token"]
    for cur_num in range(3):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "integration_tests/test_file_full_trust/train-" + str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        name_to_upload = "file-" + str(cur_num + 1)
        cur_res = client_api.upload_dataset(name_to_upload,
                                            cur_file_bytes,
                                            "file",
                                            cur_optimistic_flag,
                                            cur_token, )

    # Step 3: jerry creates a policy saying that david can discover how many lines his files have
    # for two files out of the three
    policy_one = Policy(user_id=2, api="line_count", data_id=1)
    client_api.upload_policy(policy_one, cur_token)
    policy_two = Policy(user_id=2, api="line_count", data_id=3)
    client_api.upload_policy(policy_two, cur_token)

    # Step 4: david logs in and looks at the available apis
    cur_token = client_api.login_user("david", "123456")["access_token"]
    list_of_apis = client_api.get_all_apis(cur_token)
    print("All available APIs are:")
    print(list_of_apis)

    # Step 5: david calls the API line_count
    client_api.call_api("line_count", cur_token, "pessimistic")

    # Step 6: checking the auditable log
    client_api.read_full_log()

    # Last step: shut down the Data Station
    client_api.shut_down(ds_config)

