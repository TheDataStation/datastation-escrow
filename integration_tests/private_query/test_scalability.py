import string

import numpy as np
import pandas as pd

import main
import os
import shutil
import time
import random

from common import utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from crypto import cryptoutils as cu


def clear_dir(dir_name):
    for filename in os.listdir(dir_name):
        file_path = os.path.join(dir_name, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


if __name__ == '__main__':

    num_runs_experiment = 10

    risk_group_percentile = -1
    risk_group_size = 100
    eps_list = list(np.arange(0.001, 0.01, 0.001, dtype=float))
    eps_list += list(np.arange(0.01, 0.1, 0.01, dtype=float))
    eps_list += list(np.arange(0.1, 1, 0.1, dtype=float))
    eps_list += list(np.arange(1, 11, 1, dtype=float))
    num_runs = 5
    q = 0.05
    test = "mw"

    # query_string = "SELECT COUNT(*) FROM adult WHERE income == '>50K' AND education_num == 13 AND age == 25"
    # query_string = "SELECT marital_status, COUNT(*) FROM adult WHERE race == 'Asian-Pac-Islander' AND age >= 30 AND
    # age <= 40 GROUP BY marital_status"
    # query_string = "SELECT COUNT(*) FROM adult WHERE native_country != 'United-States' AND sex == 'Female'"
    # query_string = "SELECT AVG(hours_per_week) FROM adult WHERE workclass == 'Federal-gov' OR workclass == 'Local-gov' or workclass == 'State-gov'"
    # query_string = "SELECT SUM(fnlwgt) FROM adult WHERE capital_gain > 0 AND income == '<=50K' AND occupation ==
    # 'Sales'"

    queries = ["SELECT COUNT(*) FROM adult WHERE income == '>50K' AND education_num == 13 AND age == 25",
               "SELECT marital_status, COUNT(*) FROM adult WHERE race == 'Asian-Pac-Islander' AND age >= 30 AND age <= 40 GROUP BY marital_status",
               "SELECT COUNT(*) FROM adult WHERE native_country != 'United-States' AND sex == 'Female'",
               "SELECT AVG(hours_per_week) FROM adult WHERE workclass == 'Federal-gov' OR workclass == 'Local-gov' or workclass == 'State-gov'",
               "SELECT SUM(fnlwgt) FROM adult WHERE capital_gain > 0 AND income == '<=50K' AND occupation == 'Sales'"
               ]

    root_dir = "/home/cc/DataStation/integration_tests/private_query/"
    # root_dir = "/Users/zhiruzhu/Desktop/data_station/DataStation/integration_tests/private_query"

    data_list = ["adult_1000", "adult_10000", "adult_100000", "adult_1000000"]


    for data in data_list:

        data_path = f"{root_dir}/data/{data}.csv"
        encrypted_path = f"{root_dir}/encrypted_data/{data}.csv"


        # initialize system

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

        # clear SM_storage
        clear_dir(ds_config["storage_path"])

        # Save data station's public key
        ds_public_key = client_api.key_manager.ds_public_key


        def create_user(user_name):
            # Create a new symmetric key
            sym_key = cu.generate_symmetric_key()

            # Create a new private/public key pair
            cur_private_key, cur_public_key = cu.generate_private_public_key_pair()

            # Now we encrypt the symmetric key with data station's public key
            cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)

            # now uploading both the user's public key, and the encrypted symmetric key to the DS
            response = client_api.create_user(User(user_name=user_name, password=""), cipher_sym_key, cur_public_key)
            return response


        # create data owner
        data_owner_name = "data_owner"

        # In no trust mode, when a user registers, he needs to upload his symmetric key and public key as well
        response = create_user(user_name=data_owner_name)
        if response.status != 0:
            print(response.message)
            exit()
        data_owner_id = response.user_id

        # Log in to get a token
        cur_token = client_api.login_user(data_owner_name, "")["access_token"]
        cur_user_sym_key = client_api.key_manager.get_agent_symmetric_key(agent_id=data_owner_id)

        # Now we create the encrypted file
        start_time = time.time()

        with open(data_path, 'rb') as f:
            content = f.read()
            encrypted_content = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(content)
            with open(encrypted_path, 'wb') as f:
                f.write(encrypted_content)

        elapsed = time.time() - start_time

        print(f"encryption time: {elapsed} s")
        # encryption_time_list.append(elapsed)

        start_time = time.time()

        # upload the encrypted file
        with open(encrypted_path, 'rb') as f:
            content = f.read()
            cur_optimistic_flag = False
            name_to_upload = f"{data}.csv"
            cur_res = client_api.upload_dataset(name_to_upload,
                                                content,
                                                "file",
                                                cur_optimistic_flag,
                                                cur_token,
                                                original_data_size=len(content))
            if cur_res.status != 0:
                print(cur_res.message)
                exit()
            data_id = cur_res.data_id

        elapsed = time.time() - start_time

        print(f"upload time: {elapsed} s")
        # upload_time_list.append(elapsed)

        # create policies for data users who want to query data

        # create a data user
        response = create_user("zhiru")
        if response.status != 0:
            print(response.message)
            exit()
        user_id = response.user_id

        list_of_apis = client_api.get_all_apis(cur_token)
        policies_to_upload = []
        # allow this user to access the aggregated data only, using read_catalog or query_sql function
        policies_to_upload.append(Policy(user_id=user_id,
                                         api="read_catalog",
                                         data_id=data_id))
        policies_to_upload.append(Policy(user_id=user_id,
                                         api="find_epsilon",
                                         data_id=data_id))
        client_api.bulk_upload_policies(policies_to_upload, cur_token)
        # res = client_api.upload_policy(Policy(user_id=user_id,
        #                                 api="find_epsilon",
        #                                 data_id=data_id), cur_token)

        # users can call the read_catalog api
        cur_token = client_api.login_user("zhiru", "")["access_token"]
        catalog = client_api.call_api(api="read_catalog",
                                      token=cur_token,
                                      exec_mode="pessimistic")
        # should only see the schema for the aggregate data
        print("catalog:")
        print(catalog)

        print("data:", data)

        time_list = []
        res_eps_list = []

        for i in range(num_runs_experiment):

            print("i:", i)

            times = []
            res_eps = []

            for sql_query in queries:

                print("query:", sql_query)

                start_time = time.time()
                res = client_api.call_api("find_epsilon", cur_token, "pessimistic",
                                          list(catalog.keys())[0],
                                          sql_query,
                                          risk_group_percentile,
                                          risk_group_size,
                                          eps_list,
                                          num_runs,
                                          q,
                                          test)
                elapsed = time.time() - start_time
                print(f"time: {elapsed} s")
                times.append(elapsed)

                if res is not None:
                    best_eps, dp_result = res
                    print("best eps:", best_eps)
                    res_eps.append(best_eps)
                else:
                    print("can't find epsilon")
                    res_eps.append(None)

            time_list.append(times)
            res_eps_list.append(res_eps)

        with open(f"{data}_time.log", "w") as f:
            for times in time_list:
                f.write(str(times)[1:-1] + "\n")

        with open(f"{data}_eps.log", "w") as f:
            for eps in res_eps_list:
                f.write(str(eps)[1:-1] + "\n")

        client_api.shut_down(ds_config)
