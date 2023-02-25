import pathlib
import main
import os
import shutil
import numpy as np
import pickle

from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from crypto import cryptoutils as cu
from crypto import key_manager

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
    # print(ds_public_key)

    # Adding new users

    # We upload 8 users, one holds each partition of the data (X and y)
    num_users = 8

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

    # Adding datasets and policies

    # First clear ml_file_no_trust/training_income

    no_trust_folder = 'integration_tests/ml_file_no_trust/training_income'
    for filename in os.listdir(no_trust_folder):
        file_path = os.path.join(no_trust_folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Now we create the encrypted files

    for cur_num in range(num_users):
        # We have two types of data: X and y
        train_type = ["X", "y"]
        for cur_type in train_type:
            cur_train = "integration_tests/ml_file_full_trust/training_income/train" + str(cur_num) \
                        + "_" + cur_type + ".npy"
            cur_user_sym_key = client_api.key_manager.agents_symmetric_key[cur_num+1]
            # Load np object
            cur_np_obj = np.load(cur_train)
            # np object to pkl bytes
            cur_pkl_obj = pickle.dumps(cur_np_obj)
            # pkl bytes encrypted
            ciphertext_bytes = cu.encrypt_data_with_symmetric_key(cur_pkl_obj, cur_user_sym_key)
            # write encrypted bytes to file
            cur_cipher_name = "integration_tests/ml_file_no_trust/training_income/train" \
                              + str(cur_num) + "_" + cur_type + ".pkl"
            cur_cipher_file = open(cur_cipher_name, "wb")
            cur_cipher_file.write(ciphertext_bytes)
            cur_cipher_file.close()

    # For each user, we upload his partition of the data (2 data elements, both X and y)
    data_id_counter = 1
    for cur_num in range(num_users):
        # Log in the current user and get a token
        cur_uname = "user" + str(cur_num)
        cur_token = client_api.login_user(cur_uname, "string")["access_token"]

        # We have two types of data: X and y
        train_type = ["X", "y"]
        for cur_type in train_type:
            cur_train = "integration_tests/ml_file_no_trust/training_income/train" \
                        + str(cur_num) + "_" + cur_type + ".pkl"
            cur_file = open(cur_train, "rb")
            cur_file_bytes = cur_file.read()
            cur_optimistic_flag = False
            name_to_upload = "train" + str(cur_num) + "_" + cur_type + ".pkl"
            cur_res = client_api.register_data(name_to_upload,
                                               cur_file_bytes,
                                                "file",
                                               cur_optimistic_flag,
                                               cur_token, )
            cur_file.close()
            # Upload the policies
            client_api.upload_policy(Policy(user_id=1, api="train_income_model", data_id=data_id_counter), cur_token)
            data_id_counter += 1

    # In here, DB construction is done. We just need to call train_income_model

    # Use token for user0
    cur_token = client_api.login_user("user0", "string")["access_token"]
    res_model = client_api.call_api("train_income_model", cur_token, "optimistic")
    # Decrypte the result using caller's own symmetric key
    res_model = cu.from_bytes(cu.decrypt_data_with_symmetric_key(res_model,
                                                                 client_api.key_manager.get_agent_symmetric_key(1)))
    # print(res_model)
    # print("Model returned is: ")
    # print(res_model.coef_, res_model.intercept_)

    # After we get the model back, we test its accuracy
    x_test = np.load("integration_tests/ml_file_full_trust/testing_income/test_X.npy")
    y_test = np.load("integration_tests/ml_file_full_trust/testing_income/test_y.npy")
    accuracy = res_model.score(x_test, y_test)
    print("Model accuracy is " + str(accuracy))

    client_api.shut_down(ds_config)
