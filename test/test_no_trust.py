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
from crypto import cryptoutils as cu

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # Get start time of the script
    prev_time = time.time()

    # Read in the configuration file
    test_config = parse_config(sys.argv[1])

    # System initialization

    ds_config = utils.parse_config("data_station_config.yaml")
    app_config = utils.parse_config("app_connector_config.yaml")

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

    # print("Data Station's public key is:")
    # print(ds_public_key)

    # cur_time = time.time()
    # print("System initialization done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Adding new users

    list_of_users = ["jerry", "lucy", "david"]

    # In no trust mode, when a user registers, he needs to upload his symmetric key and public key as well

    for cur_user in list_of_users:
        # Create a new symmetric key
        sym_key = cu.generate_symmetric_key()
        # print("Symmetric in the plain is:")
        # print(sym_key)

        # Create a new private/public key pair
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        # print("User's public key is:")
        # print(cur_public_key)

        # Now we encrypt the symmetric key with data station's public key
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        # print("Cipher symmetric key is:")
        # print(cipher_sym_key)

        # Quickly check to see if everything works correctly
        # dec_sym_key = cu.decrypt_data_with_private_key(cipher_sym_key, client_api.key_manager.ds_private_key)
        # print("Decrypted symmetric key is:")
        # print(dec_sym_key)

        # TODO: Now uploading both the user's public key, and the encrypted symmetric key to the DS
        client_api.create_user(User(user_name=cur_user, password="string"), cipher_sym_key, cur_public_key)

    # Taking a look at the keys that are stored
    # print(client_api.key_manager.agents_symmetric_key)
    # print(client_api.key_manager.agents_public_key)

    # cur_time = time.time()
    # print("User addition done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

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

    # TODO: before we test uploading datasets,
    #       we first have to create the encrypted files using the newly generated symmetric keys

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
        cur_plain_name = "test/test_file_full_trust/train-" + str(cur_num+1) + ".csv"
        cur_user_sym_key = client_api.key_manager.agents_symmetric_key[1]
        cur_plain_file = open(cur_plain_name, 'rb').read()
        ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(cur_plain_file)
        cur_cipher_name = "test/test_file_no_trust/train-" + str(cur_num+1) + ".csv"
        cur_cipher_file = open(cur_cipher_name, "wb")
        cur_cipher_file.write(ciphertext_bytes)
        cur_cipher_file.close()

    # Proceeding to actually uploads the datasets

    # First clear SM_storage

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    num_files = test_config["num_files"]
    opt_data_proportion = test_config["opt_data_proportion"]
    list_of_data_ids = []

    for cur_num in range(num_files):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "test/test_file_no_trust/train-" + str(cur_file_index) + ".csv"
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
                                            cur_token,)
        if cur_res.status == 0:
            list_of_data_ids.append(cur_res.data_id)
        cur_file.close()

    # print(list_of_data_ids)

    # cur_time = time.time()
    # print("Uploading datasets done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # # Uncomment the next block to verify end-to-end file sharing
    # # Trying out retrieving datasets
    #
    # # Let's first get a new token
    # cur_token = client_api.login_user("lucy", "string")["access_token"]
    # # Then get the encrypted file
    # data_retrieved = client_api.retrieve_data_by_id(1, cur_token)
    # # Then decrypt it using the current user's sym key
    # cur_user_sym_key = client_api.key_manager.agents_symmetric_key[2]
    # data_plain = cu.decrypt_data_with_symmetric_key(data_retrieved, cur_user_sym_key)
    # print(data_plain)

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
    print("Expected number of policies is: " + str(
        math.floor(num_data_with_policy * len(list_of_apis) * policy_proportion)))
    prev_time = cur_time

    # call available APIs

    client_api.call_api("preprocess", cur_token, "optimistic")
    print("preprocess finished\n")
    client_api.call_api("modeltrain", cur_token, "optimistic")
    print("modeltrain finished\n")
    client_api.call_api("predict", cur_token, "pessimistic", 10, 5)
    print("predict finished\n")

    cur_time = time.time()
    print("Calling APIs done")
    print("--- %s seconds ---" % (cur_time - prev_time))
    prev_time = cur_time

    # take a look at the log

    client_api.read_full_log()

    # take a look at the WAL

    client_api.read_wal()
