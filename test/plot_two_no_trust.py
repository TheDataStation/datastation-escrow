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

from Interceptor import interceptor
from common import utils
from models.user import *
from models.policy import *
from common.utils import parse_config
from crypto import cryptoutils as cu

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

    # Remove the code block below if testing out durability of wal
    wal_path = client_api.write_ahead_log.wal_path
    if os.path.exists(wal_path):
        os.remove(wal_path)

    # Save data station's public key
    ds_public_key = client_api.key_manager.ds_public_key

    # Initialize an overhead list

    overhead = []

    # We record the total number of DB operations performed

    total_db_ops = 0

    # Adding new users

    num_users = test_config["num_users"]

    # We generate keys outside the loop because we don't want to count its time

    cipher_sym_key_list = []
    public_key_list = []

    for cur_num in range(num_users):
        sym_key = cu.generate_symmetric_key()
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)

    # Start counting user addition time
    prev_time = time.time()

    for cur_num in range(num_users):
        cur_uname = "user" + str(cur_num+1)
        client_api.create_user(User(user_name=cur_uname, password="string"),
                               cipher_sym_key_list[cur_num],
                               public_key_list[cur_num], )
        total_db_ops += 1

    # # Taking a look at the keys that are stored
    # print(client_api.key_manager.agents_symmetric_key)
    # print(client_api.key_manager.agents_public_key)

    # Log in a user to get a token

    cur_token = client_api.login_user("user1", "string")["access_token"]

    # Record time
    cur_time = time.time()
    cur_cost = cur_time - prev_time
    overhead.append(cur_cost)

    # Look at all available APIs and APIDependencies

    list_of_apis = client_api.get_all_apis(cur_token)
    list_of_api_dependencies = client_api.get_all_api_dependencies(cur_token)
    print(list_of_apis)

    # Upload datasets

    # First clear test_file_no_trust

    no_trust_folder = 'test/test_file_no_trust'
    for filename in os.listdir(no_trust_folder):
        file_path = os.path.join(no_trust_folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Now we create the encrypted files

    for cur_num in range(6):
        cur_plain_name = "test/test_file_full_trust/train-" + str(cur_num + 1) + ".csv"
        cur_user_sym_key = client_api.key_manager.agents_symmetric_key[1]
        cur_plain_file = open(cur_plain_name, 'rb').read()
        ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(cur_plain_file)
        cur_cipher_name = "test/test_file_no_trust/train-" + str(cur_num + 1) + ".csv"
        cur_cipher_file = open(cur_cipher_name, "wb")
        cur_cipher_file.write(ciphertext_bytes)
        cur_cipher_file.close()

    # Proceeding to actually uploads the datasets

    prev_time = time.time()

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
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "test/test_file_full_trust/train-" + str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
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

    # # Write overhead to csv file
    # db_res_name = "u" + str(num_users) + "d" + str(num_files) + "full"
    # db_call_res_file = "numbers/" + db_res_name + ".csv"
    # if os.path.exists(db_call_res_file):
    #     os.remove(db_call_res_file)
    # with open(db_call_res_file, 'a') as f:
    #     writer_object = writer(f)
    #     writer_object.writerow(overhead)

    # Before shutdown, let's look at the total number of DB ops (insertions) that we did
    print("Total number of DB insertions is: "+str(total_db_ops))
    client_api.shut_down(ds_config)

    print(overhead)
