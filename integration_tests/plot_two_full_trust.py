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

from common import utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from common.utils import parse_config

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    if os.path.exists("owner_overhead.txt"):
        os.remove("owner_overhead.txt")

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

    # We record the total number of DB operations performed

    total_db_ops = 0

    # Before we get started, let's create synthetic file to be used for this experiment
    data_size = test_config["num_kb_size"]
    num_chars = data_size * 1024

    with open('owner_overhead.txt', 'wb') as f:
        for i in range(num_chars):
            f.write(b'\x01')

    # Start recording the overhead: first, initialize an overhead list

    overhead = []
    prev_time = time.time()

    # Adding new users

    num_users = test_config["num_users"]

    for cur_num in range(num_users):
        cur_uname = "user" + str(cur_num+1)
        client_api.create_user(User(user_name=cur_uname, password="string"))
        total_db_ops += 1

    # Log in a user to get a token

    cur_token = client_api.login_user("user1", "string")["access_token"]

    # Record time
    cur_time = time.time()
    cur_cost = cur_time - prev_time
    overhead.append(cur_cost)
    prev_time = cur_time

    # Look at all available APIs and APIDependencies

    list_of_apis = client_api.get_all_apis(cur_token)
    list_of_api_dependencies = client_api.get_all_api_dependencies(cur_token)
    print(list_of_apis)

    # Upload datasets

    # First clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Then set up the data elements we will upload

    num_files = test_config["num_files"]
    opt_data_proportion = test_config["opt_data_proportion"]
    list_of_data_ids = []

    for cur_num in range(num_files):
        cur_file = open("owner_overhead.txt", "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        if random.random() < opt_data_proportion:
            cur_optimistic_flag = True
        name_to_upload = "file-" + str(cur_num + 1)
        cur_res = client_api.upload_dataset(name_to_upload,
                                            cur_file_bytes,
                                            "file",
                                            cur_optimistic_flag,
                                            cur_token, )
        total_db_ops += 1
        if cur_res.status == 0:
            list_of_data_ids.append(cur_res.data_id)
        cur_file.close()

    # Record time
    cur_time = time.time()
    cur_cost = cur_time - prev_time
    overhead.append(cur_cost)
    prev_time = cur_time

    # Upload Policies

    data_with_policy_proportion = test_config["data_with_policy_proportion"]
    num_data_with_policy = math.floor(data_with_policy_proportion * len(list_of_data_ids))

    policy_proportion = test_config["policy_proportion"]
    policy_created = 0

    # Uploading the policies one by one
    policy_array = []
    for api_picked in list_of_apis:
        for i in range(num_data_with_policy):
            if random.random() < policy_proportion:
                client_api.upload_policy(Policy(user_id=1, api=api_picked, data_id=list_of_data_ids[i]), cur_token)
                total_db_ops += 1

    # Record time
    cur_time = time.time()
    cur_cost = cur_time - prev_time
    overhead.append(cur_cost)
    prev_time = cur_time

    # Write overhead to csv file
    db_res_name = "u" + str(num_users) + "d" + str(num_files) + "s" + str(data_size)
    db_call_res_file = "numbers/" + db_res_name + ".csv"
    if os.path.exists(db_call_res_file):
        os.remove(db_call_res_file)
    with open(db_call_res_file, 'a') as f:
        writer_object = writer(f)
        writer_object.writerow(overhead)

    # Before shutdown, let's look at the total number of DB ops (insertions) that we did
    print("Total number of DB insertions is: "+str(total_db_ops))
    client_api.shut_down(ds_config)

    os.remove("owner_overhead.txt")
