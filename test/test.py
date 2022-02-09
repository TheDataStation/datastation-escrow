# This script loads a complete state of the data station
import sys
import main
import os
import shutil
import time
import math
import random

from common import utils
from models.user import *
from models.policy import *
from common.utils import parse_config

if __name__ == '__main__':


    # TODO 1: vary what's inside of the dependency graph (api and api dependencies)
    # TODO 2: vary how we select the APIs (right now we are uniformly selecting the APIs
    # TODO 2: we can do 1) uniform 2) those with few descendants 3) those with many descendants

    # Get start time of the script
    prev_time = time.time()

    # Read in the configuration file
    test_config = parse_config(sys.argv[1])

    # System initialization

    ds_config = utils.parse_config("data_station_config.yaml")
    app_config = utils.parse_config("app_connector_config.yaml")

    client_api = main.initialize_system(ds_config, app_config)

    # cur_time = time.time()
    # print("System initialization done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Adding new users

    client_api.create_user(User(user_name="jerry", password="string"))
    client_api.create_user(User(user_name="lucy", password="123456"))
    client_api.create_user(User(user_name="david", password="string"))

    cur_time = time.time()
    print("User addition done")
    print("--- %s seconds ---" % (cur_time - prev_time))
    prev_time = cur_time

    # Log in a user to get a token

    cur_token = client_api.login_user("jerry", "string")["access_token"]

    # cur_time = time.time()
    # print("Log in done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Look at all available APIs and APIDependencies

    list_of_apis = client_api.get_all_apis(cur_token)
    list_of_api_dependencies = client_api.get_all_api_dependencies(cur_token)
    print(list_of_apis)
    print(list_of_api_dependencies)

    # cur_time = time.time()
    # print("Looking at dependency graph done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Upload datasets

    # First clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    num_files = test_config["num_files"]
    list_of_data_ids = []

    for cur_num in range(num_files):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "test/test_file/train-" + str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic = False
        name_to_upload = "file-" + str(cur_num)
        cur_res = client_api.upload_dataset(name_to_upload,
                                            cur_file_bytes,
                                            "file",
                                            cur_optimistic,
                                            cur_token,)
        if cur_res.status == 0:
            list_of_data_ids.append(cur_res.data_id)
        cur_file.close()

    # print(list_of_data_ids)

    cur_time = time.time()
    print("Uploading datasets done")
    print("--- %s seconds ---" % (cur_time - prev_time))
    prev_time = cur_time

    # Upload Policies

    data_with_policy_proportion = test_config["data_with_policy_proportion"]
    num_data_with_policy = math.floor(data_with_policy_proportion * len(list_of_data_ids))

    # Right now for each dataset, we pick one API for it to create a policy
    # TODO: change this to something configurable

    # Idea: enumerate all combinations of APIs and data_ids, then choose each with a probability
    # this probability should be in workload_config

    policy_proportion = test_config["policy_proportion"]
    policy_created = 0

    for api_picked in list_of_apis:
        for i in range(num_data_with_policy):
            if random.random() < policy_proportion:
                policy_created += 1
                client_api.upload_policy(Policy(user_id=1, api=api_picked, data_id=list_of_data_ids[i]), cur_token)

    cur_time = time.time()
    print("Uploading policies done")
    print("--- %s seconds ---" % (cur_time - prev_time))
    print("Number of policies created is: " + str(policy_created))
    print("Expected number of policies is: " + str(math.floor(num_data_with_policy*len(list_of_apis)*policy_proportion)))
    prev_time = cur_time

    # call available APIs
    client_api.call_api("preprocess", cur_token)
    print("preprocess finished\n")
    client_api.call_api("modeltrain", cur_token)
    print("modeltrain finished\n")
    client_api.call_api("predict", cur_token, 10, 5)
    print("predict finished\n")

    cur_time = time.time()
    print("Calling APIs done")
    print("--- %s seconds ---" % (cur_time - prev_time))
    prev_time = cur_time

    # take a look at the log
    client_api.print_log()
