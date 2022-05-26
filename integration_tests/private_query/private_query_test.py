import string

import numpy as np

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

    # get keys

    # create users

    # Save data station's public key
    ds_public_key = client_api.key_manager.ds_public_key

    # Adding data owners
    num_data_owners = 2
    data_owners = [str(i) for i in range(num_data_owners)]
    data_owner_ids = []

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

    # In no trust mode, when a user registers, he needs to upload his symmetric key and public key as well
    for cur_user in data_owners:
        response = create_user(user_name=cur_user)
        if response.status == 0:
            data_owner_ids.append(response.user_id)
        else:
            print(response.message)
            exit()

    # one of the user upload catalog (not needed in crypte)
    # without changing the code structure / databases we can just upload the catalog like a normal data element
    # and save its id

    data_dir = "/Users/zhiruzhu/Desktop/data_station/DataStation/integration_tests/private_query/data/"
    encrypted_data_dir = "/Users/zhiruzhu/Desktop/data_station/DataStation/integration_tests/private_query" \
                         "/encrypted_data/"

    cur_token = client_api.login_user(data_owners[0], "")["access_token"]

    cur_user_sym_key = client_api.key_manager.get_agent_symmetric_key(agent_id=data_owner_ids[0])

    with open(data_dir + "PUMS.yaml", 'rb') as f:
        content = f.read()
        encrypted_content = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(content)
        with open(encrypted_data_dir + "PUMS.yaml", 'wb') as f:
            f.write(encrypted_content)

    with open(encrypted_data_dir + "PUMS.yaml", 'rb') as f:
        content = f.read()
        cur_optimistic_flag = False
        name_to_upload = "PUMS.yaml"
        cur_res = client_api.upload_dataset(name_to_upload,
                                                      content,
                                                      "file",
                                                      cur_optimistic_flag,
                                                      cur_token,
                                                      original_data_size=len(content))
        if cur_res.status != 0:
            print(cur_res.message)
            exit()

        catalog_id = cur_res.data_id


    # each user upload their encrypted file (csv)

    # create a new aggregator component inside DS (not here)

    # TODO: retrieve current schema and pass into Aggregator.
    schema = ["age", "sex", "educ", "race", "income", "married", "pid"]

    # inside the upload function (pass in the flag aggregate = True):
    # 1. the aggregator will first check if its schema is compatible with the catalog (not needed in crypte)
    # 2. retrieve the existing aggregated data from storage manager
    # note: there is always ONLY one data element (besides the catalog) inside storage!!! Because any uploaded
    # dataset will get combined
    # 3. combine with the existing data (csv file)
    # 4. remove the old data from storage
    # 5. upload the newly combined data to storage (keep the same id, or not)
    # 6. return data id

    cur_aggregated_data_id = 0

    for i in range(num_data_owners):

        # Log in a user to get a token
        cur_token = client_api.login_user(str(i), "")["access_token"]

        # Now we create the encrypted files
        cur_user_sym_key = client_api.key_manager.get_agent_symmetric_key(agent_id=data_owner_ids[i])

        with open(data_dir + "PUMS-" + str(i) + ".csv", 'rb') as f:
            content = f.read()
            encrypted_content = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(content)
            with open(encrypted_data_dir + "PUMS-" + str(i) + ".csv", 'wb') as f:
                f.write(encrypted_content)

        with open(encrypted_data_dir + "PUMS-" + str(i) + ".csv", 'rb') as f:
            content = f.read()
            cur_optimistic_flag = False
            name_to_upload = "PUMS.csv" # same name
            cur_res = client_api.upload_aggregate_dataset(name_to_upload,
                                                          schema,
                                                          content,
                                                          "file",
                                                          cur_optimistic_flag,
                                                          cur_token,
                                                          original_data_size=len(content))
            if cur_res.status == 0:
                cur_aggregated_data_id = cur_res.data_id
            else:
                print(cur_res.message)
                exit()

    # create policies for both the catalog and the aggregated data for data users who want to query data
    # (or just upload data in open mode)

    # create a data user
    response = create_user("zhiru")
    if response.status != 0:
        print(response.message)
        exit()
    user_id = response.user_id
    cur_token = client_api.login_user("zhiru", "")["access_token"]

    list_of_apis = client_api.get_all_apis(cur_token)
    policies_to_upload = []
    # for api in list_of_apis:
    policies_to_upload.append(Policy(user_id=user_id,
                                     api="read_catalog",
                                     data_id=catalog_id))
    policies_to_upload.append(Policy(user_id=user_id,
                                     api="query_sql",
                                     data_id=cur_aggregated_data_id))

    client_api.bulk_upload_policies(policies_to_upload, cur_token)

    # users can call the read_catalog api
    catalog = client_api.call_api("read_catalog")

    # then call the query_sql api
    sql_query = "SELECT COUNT(*) FROM PUMS.PUMS"
    api_result = client_api.call_api("query_sql", cur_token, "pessimistic", sql_query=sql_query)

