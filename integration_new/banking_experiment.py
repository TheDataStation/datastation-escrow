import os
import numpy as np
import random
import pandas as pd
import time

from main import initialize_system
from common.general_utils import clean_test_env
from crypto import cryptoutils as cu


def banking_data_gen(num_agents=5, num_MB=1, output_dir="integration_new/test_files/banking_p/exp"):
    def generate_indicator(vector_size, proportion_of_ones):
        num_ones = int(vector_size * proportion_of_ones)
        random_vector = np.concatenate([np.zeros(vector_size - num_ones), np.ones(num_ones)])
        np.random.shuffle(random_vector)
        return random_vector
    random.seed(42)
    num_records = 10000 * num_MB
    # Need to generate independent vars: amt, gender, lat, long, merch_lat, merch_long, city_pop_thousands, age
    amt_ranges = [(1, 10), (20, 100), (100, 500)]
    amt_probs = [0.47, 0.48, 0.05]
    city_pop_thousands_ranges = [(1, 5), (10, 200), (500, 1500)]
    city_pop_probs = [0.2, 0.6, 0.2]
    age_ranges = [(18, 30), (31, 50), (51, 80)]
    age_probs = [0.6, 0.35, 0.05]
    amt_arr = []
    lat_arr = []
    long_arr = []
    merch_lat_arr = []
    merch_long_arr = []
    city_pop_thousands_arr = []
    age_arr = []
    for _ in range(num_records):
        selected_range = random.choices(amt_ranges, weights=amt_probs)[0]
        amount = random.randint(selected_range[0], selected_range[1])
        amt_arr.append(amount)
        lat = random.uniform(-100, 100)
        long = random.uniform(-100, 100)
        merch_lat = random.uniform(-100, 100)
        merch_long = random.uniform(-100, 100)
        lat_arr.append(lat)
        long_arr.append(long)
        merch_lat_arr.append(merch_lat)
        merch_long_arr.append(merch_long)
        selected_range = random.choices(city_pop_thousands_ranges, weights=city_pop_probs)[0]
        city_pop = random.randint(selected_range[0], selected_range[1])
        city_pop_thousands_arr.append(city_pop)
        selected_range = random.choices(age_ranges, weights=age_probs)[0]
        age = random.randint(selected_range[0], selected_range[1])
        age_arr.append(age)

    gender_arr = generate_indicator(num_records, 0.5)
    matrix = np.column_stack((amt_arr,
                              gender_arr,
                              lat_arr,
                              long_arr,
                              merch_lat_arr,
                              merch_long_arr,
                              city_pop_thousands_arr,
                              age_arr))
    # Need to generate the dependent variable: is_fraud
    is_fraud_arr = []
    coefficients = np.array([0.5, 3, 0, 0, 0, 0, 0.1, -5])
    linear_combination = np.dot(matrix, coefficients)
    probabilities = 1 / (1 + np.exp(-linear_combination))

    for probs in probabilities:
        if probs == 1:
            is_fraud_arr.append(1)
        else:
            is_fraud_arr.append(0)

    matrix_with_label = np.column_stack((matrix, is_fraud_arr))
    # Convert matrices to Pandas DataFrames
    col_names = ["amt",
                 "gender",
                 "lat",
                 "long",
                 "merch_lat",
                 "merch_long",
                 "city_pop_thousands",
                 "age",
                 "is_fraud"]
    final_df = pd.DataFrame(matrix_with_label, columns=[f'{col_names[i]}'
                                                        for i in range(matrix_with_label.shape[1])])

    # Display the resulting DataFrame
    print("Resulting DataFrame:")
    print(final_df[:5])

    # Partition the data and write to files
    num_train_per_bank = int(num_records * (4/25))
    num_test_per_bank = int(num_records * (1/25))
    prev_index = 0
    for i in range(num_agents):
        cur_train_partition = final_df[prev_index: prev_index+num_train_per_bank]
        cur_test_partition = final_df[prev_index+num_train_per_bank: prev_index+num_train_per_bank+num_test_per_bank]
        prev_index = prev_index+num_train_per_bank+num_test_per_bank
        cur_train_path = os.path.join(output_dir, f"train_{i}.csv")
        cur_test_path = os.path.join(output_dir, f"test_{i}.csv")
        cur_train_partition.to_csv(cur_train_path, index=False)
        cur_test_partition.to_csv(cur_test_path, index=False)


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

    # Step 1: Agent creation. Let's use 5 agents for short-circuiting purposes.
    cipher_sym_key_list = []
    public_key_list = []
    num_users = 5
    for i in range(num_users):
        sym_key = b'oHRZBUvF_jn4N3GdUpnI6mW8mts-EB7atAUjhVNMI58='
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        ds.create_agent(f"bank{i+1}", "string", cipher_sym_key_list[i], public_key_list[i], )

    bank1_token = ds.login_agent("bank1", "string")["data"]
    bank2_token = ds.login_agent("bank2", "string")["data"]
    bank3_token = ds.login_agent("bank3", "string")["data"]
    bank4_token = ds.login_agent("bank4", "string")["data"]
    bank5_token = ds.login_agent("bank5", "string")["data"]

    # Step 2: Generate the data. They will be stored to integration_new/test_files/banking_p/exp folder.
    banking_data_gen(num_agents=5, num_MB=100)

    # Step 3: Upload the data
    token_dict = {0: bank1_token, 1: bank2_token, 2: bank3_token, 3: bank4_token, 4: bank5_token}
    for i in range(num_users):
        cur_token = token_dict[i]
        cur_train_de = f"integration_new/test_files/banking_p/exp/train_{i}.csv"
        f = open(cur_train_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))
        cur_test_de = f"integration_new/test_files/banking_p/exp/test_{i}.csv"
        f = open(cur_test_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))

    # To test short-circuiting: first approve two contracts
    dest_agents = [1]
    data_elements = [1, 2, 3, 4, 5, 6]
    ds.call_api(bank1_token, "propose_contract", dest_agents, data_elements, "train_model_with_conditions",
                "is_fraud", 1000, 0.95)
    ds.call_api(bank2_token, "approve_contract", 1)
    ds.call_api(bank3_token, "approve_contract", 1)

    # dest_agents = [1]
    # data_elements = [1, 2, 7, 8, 9, 10]
    # ds.call_api(bank1_token, "propose_contract", dest_agents, data_elements, "train_model_with_conditions",
    #             "is_fraud", [1, 7, 9], [2, 8, 10], 1000, 0.95)
    # ds.call_api(bank4_token, "approve_contract", 2)
    # ds.call_api(bank5_token, "approve_contract", 2)

    # # Test: running the contracts
    # res = ds.call_api(bank1_token, "train_model_with_conditions", "is_fraud", [1, 3, 5], [2, 4, 6], 1000, 0.95)
    # print(res)
    # res = ds.call_api(bank1_token, "train_model_with_conditions", "is_fraud", [1, 7, 9], [2, 8, 10], 1000, 0.95)
    # print(res)

    # For recording time: run it 10 times and 50 times
    start_time = time.time()
    for _ in range(10):
        # res = ds.call_api(bank1_token, "train_model_with_conditions",
        #                   "is_fraud", [1, 3, 5, 7, 9], [2, 4, 6, 8, 10], 1000, 0.95)
        res = ds.call_api(bank1_token, "train_model_with_conditions",
                          "is_fraud", 1000, 0.95)
    print("Time for 10 runs:", time.time() - start_time)
    print(res)

    # Last step: shut down the Data Station
    ds.shut_down()
