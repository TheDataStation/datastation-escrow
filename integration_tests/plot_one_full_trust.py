# This script loads a complete state of the data station
import pathlib
import sys
import main
import os
import shutil
import time
import math
import random
from csv import writer

from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from common.general_utils import parse_config

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

    # Adding new users

    client_api.create_user(User(user_name="jerry", password="string"))
    client_api.create_user(User(user_name="lucy", password="123456"))
    client_api.create_user(User(user_name="david", password="string"))

    # Log in a user to get a token

    cur_token = client_api.login_user("jerry", "string")["access_token"]

    # Look at all available APIs and APIDependencies

    list_of_apis = client_api.get_all_apis(cur_token)
    list_of_api_dependencies = client_api.get_all_api_dependencies(cur_token)
    print(list_of_apis)
    print(list_of_api_dependencies)

    # Upload datasets

    # First clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Set up the data elements we will upload

    num_files = test_config["num_files"]
    opt_data_proportion = test_config["opt_data_proportion"]
    list_of_data_ids = []

    for cur_num in range(num_files):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "integration_tests/test_file_full_trust/train-" + str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        if random.random() < opt_data_proportion:
            cur_optimistic_flag = True
        name_to_upload = "file-" + str(cur_num + 1)
        cur_res = client_api.register_dataset(name_to_upload,
                                              cur_file_bytes,
                                            "file",
                                              cur_optimistic_flag,
                                              cur_token, )
        if cur_res.status == 0:
            list_of_data_ids.append(cur_res.data_id)
        cur_file.close()

    # Upload Policies

    data_with_policy_proportion = test_config["data_with_policy_proportion"]
    num_data_with_policy = math.floor(data_with_policy_proportion * len(list_of_data_ids))

    policy_proportion = test_config["policy_proportion"]
    policy_created = 0

    # Let's try to upload the policies in a bulk fashion
    policy_array = []
    for api_picked in list_of_apis:
        for i in range(num_data_with_policy):
            if random.random() < policy_proportion:
                policy_created += 1
                cur_policy = Policy(user_id=1, api=api_picked, data_id=list_of_data_ids[i])
                policy_array.append(cur_policy)

    client_api.bulk_upload_policies(policy_array, cur_token)

    print("Number of policies created is: " + str(policy_created))

    # call available APIs

    pathlib.Path.mkdir(pathlib.Path("./numbers/"), exist_ok=True)
    numbers_file_name = "numbers/" + app_config["connector_name"] + ".csv"

    print("Start counting overheads for data users:")
    prev_time = time.time()

    num_calls = test_config["num_calls"]
    for _ in range(num_calls):
        cur_run_overhead = client_api.call_api("f1", cur_token, "optimistic")
        # print(cur_run_overhead)
        # with open(numbers_file_name, 'a') as f:
        #     writer_object = writer(f)
        #     writer_object.writerow(cur_run_overhead)

    cur_time = time.time()
    print("Calling APIs done")
    print("--- %s seconds ---" % (cur_time - prev_time))
    prev_time = cur_time

    # # take a look at the log
    # client_api.read_full_log()

    client_api.shut_down(ds_config)
