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

    num_runs = 10

    encryption_time_list = []
    upload_time_list = []
    query1_time_list = []
    query2_time_list = []
    query3_time_list = []

    for i in range(num_runs):

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

        data_path = "/home/zhiru_uchicago_edu/DataStation/integration_tests/private_query/data/adult_age_race_sex_income.csv"
        encrypted_path = "/home/zhiru_uchicago_edu/DataStation/integration_tests/private_query/encrypted_data" \
                         "/adult_age_race_sex_income_encrpypted.csv"


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
        encryption_time_list.append(elapsed)

        start_time = time.time()

        # upload the encrypted file
        with open(encrypted_path, 'rb') as f:
            content = f.read()
            cur_optimistic_flag = False
            name_to_upload = "adult.csv"
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
        upload_time_list.append(elapsed)

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
                                         api="private_query_sql",
                                         data_id=data_id))
        client_api.bulk_upload_policies(policies_to_upload, cur_token)

        # users can call the read_catalog api
        cur_token = client_api.login_user("zhiru", "")["access_token"]
        catalog = client_api.call_api(api="read_catalog",
                                      token=cur_token,
                                      exec_mode="pessimistic")
        # should only see the schema for the aggregate data
        print("catalog:")
        print(catalog)

        # then call the private_query_sql api

        sql_query = "SELECT COUNT(*) AS cnt FROM adult WHERE age >= 20 AND age <= 30"
        print("query1: ", sql_query)
        start_time = time.time()
        query_result = client_api.call_api("private_query_sql", cur_token, "pessimistic",
                                           sql_query=sql_query,
                                           file_to_query=list(catalog.keys())[0],
                                           epsilon=0.1)
        elapsed = time.time() - start_time
        print(f"query1 time: {elapsed} s")
        print("query1 result:")
        print(query_result)

        query1_time_list.append(elapsed)

        #
        sql_query = "SELECT COUNT(*) AS cnt FROM adult GROUP BY race"
        print("query2: ", sql_query)
        start_time = time.time()
        query_result = client_api.call_api("private_query_sql", cur_token, "pessimistic",
                                           sql_query=sql_query,
                                           file_to_query=list(catalog.keys())[0],
                                           epsilon=0.1)
        elapsed = time.time() - start_time
        print(f"query2 time: {elapsed} s")
        print("query2 result:")
        print(query_result)

        query2_time_list.append(elapsed)

        sql_query = "SELECT COUNT(*) AS cnt FROM adult WHERE sex == 'Female' AND income == '>50K'"
        print("query3: ", sql_query)
        start_time = time.time()
        query_result = client_api.call_api("private_query_sql", cur_token, "pessimistic",
                                           sql_query=sql_query,
                                           file_to_query=list(catalog.keys())[0],
                                           epsilon=0.1)
        elapsed = time.time() - start_time
        print(f"query3 time: {elapsed} s")
        print("query3 result:")
        print(query_result)

        query3_time_list.append(elapsed)

        client_api.shut_down(ds_config)

    with open("log_ds.txt", "w") as f:
        f.write(str(encryption_time_list)[1:-1] + "\n")
        f.write(str(upload_time_list)[1:-1] + "\n")
        f.write(str(query1_time_list)[1:-1] + "\n")
        f.write(str(query2_time_list)[1:-1] + "\n")
        f.write(str(query3_time_list)[1:-1] + "\n")
