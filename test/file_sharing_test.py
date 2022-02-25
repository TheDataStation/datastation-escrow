# This script loads a complete state of the data station
import string
import sys

import numpy as np

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


def clear_dir(dir_name):
    for filename in os.listdir(dir_name):
        file_path = os.path.join(dir_name, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


if __name__ == '__main__':

    num_files_list = [10, 50, 100]
    num_functions_list = [5, 10, 50, 100]
    num_iters = 20

    for num_functions in num_functions_list:

        with open("app_connector_config.yaml", 'w') as f:
            connector_module_path = "app_connectors/file_sharing{}.py".format(num_functions)
            f.write("connector_name: \"file_sharing\"\nconnector_module_path: \"{}\"".format(connector_module_path))
            f.flush()
            os.fsync(f.fileno())

        for num_files in num_files_list:
        # for num_functions in num_functions_list:

            result = np.zeros((num_iters, 3))

            for iter_num in range(num_iters):

                print("num_functions={} num_files={} iter_num={}".format(num_files, num_functions, iter_num))

                # In the beginning we always remove the existing DB
                if os.path.exists("data_station.db"):
                    os.remove("data_station.db")
                    print("removed data_station.db")

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

                # Adding new users
                list_of_users = ["zhiru"]

                # In no trust mode, when a user registers, he needs to upload his symmetric key and public key as well
                for cur_user in list_of_users:
                    # Create a new symmetric key
                    sym_key = cu.generate_symmetric_key()

                    # Create a new private/public key pair
                    cur_private_key, cur_public_key = cu.generate_private_public_key_pair()

                    # Now we encrypt the symmetric key with data station's public key
                    cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)

                    # TODO: Now uploading both the user's public key, and the encrypted symmetric key to the DS
                    client_api.create_user(User(user_name=cur_user, password="string"), cipher_sym_key, cur_public_key)

                # Log in a user to get a token
                cur_token = client_api.login_user("zhiru", "string")["access_token"]

                # Look at all available APIs and APIDependencies
                list_of_apis = client_api.get_all_apis(cur_token)
                # list_of_api_dependencies = client_api.get_all_api_dependencies(cur_token)
                # print("main list_of_apis:", list_of_apis)
                # print(app_config["connector_module_path"])
                assert len(list_of_apis) == num_functions

                # First clear test_file_no_trust
                test_files_dir = "test/test_file_sharing/"
                clear_dir(test_files_dir)

                # generate file content of size 10K
                # random_bytes = np.random.bytes(10000)
                random_bytes = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10000)).encode()

                # Now we create the encrypted files
                for cur_num in range(100):
                    cur_user_sym_key = client_api.key_manager.get_agent_symmetric_key(agent_id=1)
                    # content = "test" + str(cur_num)
                    encrypted_content = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(random_bytes)
                    with open(test_files_dir + "test-" + str(cur_num + 1) + ".txt", 'wb') as f:
                        f.write(encrypted_content)

                # Proceeding to actually uploads the datasets

                # First clear SM_storage
                clear_dir(ds_config["storage_path"])

                start_time = time.time()

                list_of_data_ids = []
                for cur_num in range(num_files):
                    with open(test_files_dir + "test-" + str(cur_num + 1) + ".txt", 'rb') as f:
                        content = f.read()
                        cur_optimistic_flag = False
                        name_to_upload = "file-" + str(cur_num + 1)
                        cur_res = client_api.upload_dataset(name_to_upload,
                                                            content,
                                                            "file",
                                                            cur_optimistic_flag,
                                                            cur_token)
                        if cur_res.status == 0:
                            list_of_data_ids.append(cur_res.data_id)

                # print("list_of_data_ids:")
                # print(list_of_data_ids)

                upload_dataset_time = time.time() - start_time

                start_time = time.time()

                # sampled_apis = random.sample(list_of_apis, num_functions)
                policies_to_upload = []
                for api in list_of_apis:
                    # print(api)
                    for id in list_of_data_ids:
                        policies_to_upload.append(Policy(user_id=1,
                                                         api=api,
                                                         data_id=id))
                # client_api.bulk_upload_policies(policies_to_upload, cur_token)
                for policy in policies_to_upload:
                    res = client_api.upload_policy(policy, cur_token)
                    # print(res.message)

                upload_policy_time = time.time() - start_time

                start_time = time.time()

                random_api = random.choice(list_of_apis)
                res, api_result = client_api.call_api(random_api, cur_token, "pessimistic")

                call_api_time = time.time() - start_time

                # print("len(api_result):")
                # print(len(api_result))
                assert len(api_result) == num_files

                result[iter_num] = [upload_dataset_time, upload_policy_time, call_api_time]

                # take a look at the log

                # client_api.read_full_log()

                # take a look at the WAL

                # client_api.read_wal()

                # For testing: save the agents' symmetric keys
                # if os.path.exists("symmetric_keys.pkl"):
                #     os.remove("symmetric_keys.pkl")
                # client_api.save_symmetric_keys()

                client_api.shut_down(ds_config)

            result_file_name = "numbers/file_sharing_{}_{}".format(num_functions, num_files)
            np.save(result_file_name, result)
