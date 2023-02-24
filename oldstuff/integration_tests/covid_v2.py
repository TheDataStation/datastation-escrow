import pathlib
import glob
import main
import os
import shutil
import PIL.Image as Image
from keras.preprocessing.image import ImageDataGenerator
import numpy as np
import pandas as pd
import pickle
import time

from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from crypto import cryptoutils as cu

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # System initialization

    ds_config = general_utils.parse_config("data_station_config.yaml")
    app_config = general_utils.parse_config("app_connector_config.yaml")

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

    # Adding new users

    # In the proto version of the code, we upload 4 users, each holds on image
    num_users = 5

    # We generate keys outside the loop because we don't want to count its time

    cipher_sym_key_list = []
    public_key_list = []

    for cur_num in range(num_users):
        sym_key = cu.generate_symmetric_key()
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        cur_uname = "user" + str(cur_num)
        client_api.create_user(User(user_name=cur_uname, password="string"),
                               cipher_sym_key_list[cur_num],
                               public_key_list[cur_num], )

    # print(client_api.key_manager.agents_symmetric_key)

    # Clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Upload the DEs

    files = glob.glob(os.path.join("integration_tests/ml_file_full_trust/training_covid", "**/**/**/*"), recursive=True)
    f_name_list = []
    for file in set(files):
        f_name_list.append(file)

    cur_user_index = 1
    cur_user_count = 0
    cur_token = client_api.login_user("user" + str(cur_user_index-1), "string")["access_token"]
    cur_user_sym_key = client_api.key_manager.agents_symmetric_key[cur_user_index]
    limit_per_user = 100

    for cur_image in f_name_list:
        if cur_user_count == limit_per_user:
            cur_user_index += 1
            cur_user_count = 0
            cur_token = client_api.login_user("user" + str(cur_user_index - 1), "string")["access_token"]
            cur_user_sym_key = client_api.key_manager.agents_symmetric_key[cur_user_index]
        cur_user_count += 1
        with open(cur_image, "rb") as file:
            plaintext_bytes = file.read()
            ciphertext_bytes = cu.encrypt_data_with_symmetric_key(plaintext_bytes, cur_user_sym_key)
            cur_upload_res = client_api.register_data(cur_image,
                                                      ciphertext_bytes,
                                                       "file",
                                                      False,
                                                      cur_token, )
            client_api.upload_policy(Policy(user_id=1, api="covid_v2", data_id=cur_upload_res.data_id),
                                     cur_token)

    # We proceed to actually calling the model training API

    # Use token for user0
    cur_token = client_api.login_user("user0", "string")["access_token"]

    client_api.call_api("covid_v2", cur_token, "optimistic")

    # Shutting down

    client_api.shut_down(ds_config)