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
from crypto import cryptoutils as cu

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    if os.path.exists("owner_overhead.txt"):
        os.remove("owner_overhead.txt")

    if os.path.exists("owner_overhead_encrypted.txt"):
        os.remove("owner_overhead_encrypted.txt")

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

    # We record the total number of DB operations performed

    total_db_ops = 0

    # Before we get started, let's create synthetic file to be used for this experiment
    data_size = test_config["num_kb_size"]
    num_chars = data_size * 1024

    with open('owner_overhead.txt', 'wb') as f:
        for i in range(num_chars):
            f.write(b'\x01')

    # Specifying result file destinations
    db_res_name = "no_" + str(data_size) + "kb"
    db_call_res_file = "numbers/" + db_res_name + ".csv"
    if os.path.exists(db_call_res_file):
        os.remove(db_call_res_file)

    # Setting up the number of new users

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

    # Start recording the overhead: first, initialize an overhead list

    user_overhead = []
    user_time = time.time()

    for cur_num in range(num_users):
        cur_uname = "user" + str(cur_num+1)
        client_api.create_user(User(user_name=cur_uname, password="string"),
                               cipher_sym_key_list[cur_num],
                               public_key_list[cur_num], )
        total_db_ops += 1
        new_time = time.time()
        user_overhead.append(new_time - user_time)
        user_time = new_time

    # # Taking a look at the keys that are stored
    # print(client_api.key_manager.agents_symmetric_key)
    # print(client_api.key_manager.agents_public_key)

    # Log in a user to get a token

    cur_token = client_api.login_user("user1", "string")["access_token"]

    # Record time
    with open(db_call_res_file, 'a') as f:
        writer_object = writer(f)
        writer_object.writerow(user_overhead)

    # Look at all available APIs and APIDependencies

    list_of_apis = client_api.get_all_apis(cur_token)
    list_of_api_dependencies = client_api.get_all_api_dependencies(cur_token)
    print(list_of_apis)

    # Upload datasets

    num_files = test_config["num_files"]

    # Start recording DE encryption overhead: first, initialize an overhead list

    encrypt_overhead = []
    encrypt_time = time.time()

    # We first create the encrypted files
    # Note: in here we mock the time to encrypt num_files # of files
    # by encrypting owner_overhead.txt num_files # of times

    for cur_num in range(num_files):
        cur_user_sym_key = client_api.key_manager.agents_symmetric_key[1]
        cur_plain_file = open("owner_overhead.txt", 'rb').read()
        ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(cur_plain_file)
        cur_cipher_file = open("owner_overhead_encrypted.txt", "wb")
        cur_cipher_file.write(ciphertext_bytes)
        cur_cipher_file.close()
        new_time = time.time()
        encrypt_overhead.append(new_time - encrypt_time)
        encrypt_time = new_time

    # Record file encryption time
    with open(db_call_res_file, 'a') as f:
        writer_object = writer(f)
        writer_object.writerow(encrypt_overhead)

    # Proceeding to actually uploads the datasets

    # First clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Then set up the data elements we will upload

    opt_data_proportion = test_config["opt_data_proportion"]
    list_of_data_ids = []

    # Start recording DE creation overhead: first, initialize an overhead list

    data_overhead = []
    data_time = time.time()

    for cur_num in range(num_files):
        cur_file = open("owner_overhead_encrypted.txt", "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        if random.random() < opt_data_proportion:
            cur_optimistic_flag = True
        name_to_upload = "file-" + str(cur_num + 1)
        cur_res = client_api.register_data(name_to_upload,
                                           cur_file_bytes,
                                            "file",
                                           cur_optimistic_flag,
                                           cur_token, )
        total_db_ops += 1
        if cur_res.status == 0:
            list_of_data_ids.append(cur_res.data_id)
        cur_file.close()
        new_time = time.time()
        data_overhead.append(new_time - data_time)
        data_time = new_time

    # Record time
    with open(db_call_res_file, 'a') as f:
        writer_object = writer(f)
        writer_object.writerow(data_overhead)

    # Upload Policies

    data_with_policy_proportion = test_config["data_with_policy_proportion"]
    num_data_with_policy = math.floor(data_with_policy_proportion * len(list_of_data_ids))

    policy_proportion = test_config["policy_proportion"]
    policy_created = 0

    # Start recording policy creation overhead: first, initialize an overhead list

    policy_overhead = []
    policy_time = time.time()

    # Uploading the policies one by one
    for api_picked in list_of_apis:
        for i in range(num_data_with_policy):
            if random.random() < policy_proportion:
                client_api.upload_policy(Policy(user_id=1, api=api_picked, data_id=list_of_data_ids[i]), cur_token)
                total_db_ops += 1
                new_time = time.time()
                policy_overhead.append(new_time - policy_time)
                policy_time = new_time

    # Record time
    with open(db_call_res_file, 'a') as f:
        writer_object = writer(f)
        writer_object.writerow(policy_overhead)

    # Before shutdown, let's look at the total number of DB ops (insertions) that we did
    print("Total number of DB insertions is: " + str(total_db_ops))
    client_api.shut_down(ds_config)

    os.remove("owner_overhead.txt")
    os.remove("owner_overhead_encrypted.txt")
