import os

from main import initialize_system
from common.general_utils import clean_test_env
from crypto import cryptoutils as cu


if __name__ == '__main__':

    # Clean up
    clean_test_env()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    ds = initialize_system(ds_config)

    ds_public_key = ds.key_manager.ds_public_key

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    wal_path = ds.write_ahead_log.wal_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # Step 1: Agent creation.
    cipher_sym_key_list = []
    public_key_list = []
    num_users = 2
    for i in range(num_users):
        sym_key = b'oHRZBUvF_jn4N3GdUpnI6mW8mts-EB7atAUjhVNMI58='
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        ds.create_agent(f"bank{i+1}", "string", cipher_sym_key_list[i], public_key_list[i], )

    bank1_token = ds.login_agent("bank1", "string")["data"]
    bank2_token = ds.login_agent("bank2", "string")["data"]

    # Step 2: Register and upload DEs.
    for i in range(2):
        if i == 0:
            cur_token = bank1_token
        else:
            cur_token = bank2_token
        cur_train_de = f"integration_new/test_files/banking_p/fraud_train_{i}.csv"
        f = open(cur_train_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes, ))
        cur_test_de = f"integration_new/test_files/banking_p/fraud_test_{i}.csv"
        f = open(cur_test_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes, ))

    # TODO: continue from here
    print("Before exit!!!")
    exit()

    # Contract 1: show schema of all training data
    dest_agents = [1, 2]
    des = [1, 3]
    # The two lines below tests upload_cmr
    ds.call_api(bank1_token, "propose_contract", dest_agents, des, "show_schema", des)
    # ds.call_api(bank1_token, "upload_cmr", 0, 1, "show_schema")
    ds.call_api(bank2_token, "upload_cmr", 1, 0, "show_schema")
    # ds.call_api(bank1_token, "approve_contract", 1)
    # ds.call_api(bank2_token, "approve_contract", 1)
    print(ds.call_api(bank1_token, "show_schema", des))

    # Contract 2: an attempt to privately integrate data
    dest_agents = [1, 2]
    data_elements = [1, 3]
    ds.call_api(bank1_token, "propose_contract", dest_agents, data_elements, "check_column_compatibility",
                1, 3,
                ["amt", "gender", "city", "state", "city_pop_thousands", "dob"],
                ["dollar_amount", "gender", "city", "state", "city_population", "customer_date_of_birth"])
    ds.call_api(bank1_token, "approve_contract", 2)
    ds.call_api(bank2_token, "approve_contract", 2)
    print(ds.call_api(bank1_token, "check_column_compatibility", 1, 3,
                      ["amt", "gender", "city", "state", "city_pop_thousands", "dob"],
                      ["dollar_amount", "gender", "city", "state", "city_population", "customer_date_of_birth"]))

    # Contract 3: Bank1 thinks that he understands more about the columns, but want to see more.
    # So he requests a sample. This contract will not get approved, because the other bank does not want to show sample.
    dest_agents = [1]
    data_elements = [3]
    ds.call_api(bank1_token, "propose_contract", dest_agents, data_elements, "share_sample",
                3, 50)
    # ds.call_api(bank2_token, "approve_contract", 3)
    # res = ds.call_api(bank1_token, "share_sample", 3, 50)
    # print(res)

    # Contract 4: Bank2 instead uploads a new synthetic dataset, give it to bank1 for integration purposes.
    cur_train_de = f"integration_new/test_files/banking_p/synth_train_1.csv"
    f = open(cur_train_de, "rb")
    plaintext_bytes = f.read()
    f.close()
    print(ds.call_api(bank2_token, "upload_data_in_csv", plaintext_bytes, ))

    dest_agents = [1]
    data_elements = [5]
    ds.call_api(bank2_token, "propose_contract", dest_agents, data_elements, "share_de", 5)
    ds.call_api(bank2_token, "approve_contract", 4)
    print(ds.call_api(bank1_token, "share_de", 5))

    # Bank1 looks at the synthetic dataset bank2 shared, and decide on the format of DEs both of them should use
    # for training the model.
    # The schema is: gender (1/0), lat, long, age, merch_lat, merch_long, city_pop_thousands, amt (all number)
    # Bank1 also tells bank2 to convert their data into this format and upload.
    # Let's assume they finished the transformation locally.
    for i in range(2):
        if i == 0:
            cur_token = bank1_token
        else:
            cur_token = bank2_token
        cur_train_de = f"integration_new/test_files/banking_p/clean_train_{i}.csv"
        f = open(cur_train_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))
        cur_test_de = f"integration_new/test_files/banking_p/clean_test_{i}.csv"
        f = open(cur_test_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))

    # The final contract: combine the data and train the model.
    dest_agents = [1, 2]
    data_elements = [6, 7, 8, 9]
    ds.call_api(bank1_token, "propose_contract", dest_agents, data_elements, "train_model_with_conditions",
                "is_fraud", [6, 8], [7, 9], 2000, 0.95)
    ds.call_api(bank1_token, "approve_contract", 5)
    ds.call_api(bank2_token, "approve_contract", 5)
    res = ds.call_api(bank1_token, "train_model_with_conditions", "is_fraud", [6, 8], [7, 9], 2000, 0.95)
    print(res.coef_)

    # Last step: shut down the Data Station
    ds.shut_down()