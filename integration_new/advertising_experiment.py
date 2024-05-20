import os
import numpy as np
import random
import pandas as pd
from faker import Faker
import time

from main import initialize_system
from common.general_utils import clean_test_env
from crypto import cryptoutils as cu

def advertising_data_gen(num_MB=1, output_dir="integration_new/test_files/advertising_p/exp"):
    def generate_indicator(vector_size, proportion_of_ones):
        num_ones = int(vector_size * proportion_of_ones)
        random_vector = np.concatenate([np.zeros(vector_size - num_ones), np.ones(num_ones)])
        np.random.shuffle(random_vector)
        return random_vector
    fake = Faker()
    Faker.seed(42)
    np.random.seed(42)
    num_records = int(20000 * num_MB)
    # Generate random first names, last names, and email
    random_first_names = [fake.first_name() for _ in range(num_records)]
    random_last_names = [fake.last_name() for _ in range(num_records)]
    random_emails = [fake.email() for _ in range(num_records)]
    less_than_twenty_five = generate_indicator(num_records, 0.7)
    male = generate_indicator(num_records, 0.5)
    live_in_states = generate_indicator(num_records, 0.9)
    married = generate_indicator(num_records, 0.2)
    liked_game_page = generate_indicator(num_records, 0.05)
    matrix = np.column_stack((less_than_twenty_five,
                              male,
                              live_in_states,
                              married,
                              liked_game_page))
    # Coefficients
    coefficients = np.array([1, 2, 0.01, -5, 3])
    linear_combination = np.dot(matrix, coefficients)

    # Apply the logistic function to obtain probabilities
    probabilities = 1 / (1 + np.exp(-linear_combination))
    labels = []
    for probs in probabilities:
        if probs > 0.95:
            label = np.random.choice([0, 1], size=1, p=[0.8, 0.2])
            labels.append(label[0])
        else:
            labels.append(0)
    # print(labels[:10])
    matrix_with_label = np.column_stack((matrix, labels))
    # Convert matrices to Pandas DataFrames
    df1 = pd.DataFrame(random_first_names, columns=['first_name'])
    df2 = pd.DataFrame(random_last_names, columns=['last_name'])
    df3 = pd.DataFrame(random_emails, columns=['email'])
    indicator_col_names = ["less_than_twenty_five",
                           "male",
                           "live_in_states",
                           "married",
                           "liked_games_page",
                           "clicked_on_ad"]
    df4 = pd.DataFrame(matrix_with_label, columns=[f'{indicator_col_names[i]}'
                                                   for i in range(matrix_with_label.shape[1])])

    # Concatenate DataFrames horizontally
    result_df = pd.concat([df1, df2, df3, df4], axis=1)

    # # Display the resulting DataFrame
    # print("Resulting DataFrame:")
    # print(result_df)
    youtube_to_write = ['first_name', 'last_name', "email", "male", "clicked_on_ad"]
    facebook_to_write = ['first_name', 'last_name', "email", "male",
                         'less_than_twenty_five', 'live_in_states', "married", 'liked_games_page']
    facebook_path = os.path.join(output_dir, "facebook.csv")
    youtube_path = os.path.join(output_dir, "youtube.csv")
    result_df[facebook_to_write].to_csv(facebook_path, index=False)
    result_df[youtube_to_write].to_csv(youtube_path, index=False)

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

    # Step 1: Agent creation. 2 agents: facebook and youtube.
    cipher_sym_key_list = []
    public_key_list = []
    agents = ["facebook", "youtube"]
    for i in range(len(agents)):
        sym_key = b'oHRZBUvF_jn4N3GdUpnI6mW8mts-EB7atAUjhVNMI58='
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        ds.create_agent(agents[i], "string", cipher_sym_key_list[i], public_key_list[i], )

    facebook_token = ds.login_agent("facebook", "string")["data"]
    youtube_token = ds.login_agent("youtube", "string")["data"]

    # Step 2: Generate the data. They will be stored to integration_new/test_files/advertising_p/exp folder.
    advertising_data_gen()

    # Step 2: Upload the data.
    for agent in agents:
        if agent == "facebook":
            cur_token = facebook_token
        else:
            cur_token = youtube_token
        agent_de = f"integration_new/test_files/advertising_p/exp/{agent}.csv"
        f = open(agent_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))

    # Step 3: Train the joint model.
    dest_agents = [1]
    data_elements = [1, 2]
    f = "train_model_over_joined_data"
    model_name = "logistic_regression"
    label_name = "clicked_on_ad"
    # "select youtube.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad from facebook inner join youtube on facebook.first_name = youtube.first_name and facebook.last_name = youtube.last_name"
    query = "select youtube.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad " \
            "from facebook inner join youtube " \
            "on facebook.first_name = youtube.first_name " \
            "and facebook.last_name = youtube.last_name " \
            "and jaro_similarity(facebook.email, youtube.email) > 0.9"
    print(ds.call_api(facebook_token, "propose_contract",
                      dest_agents, data_elements, f, label_name, query))
    print(ds.call_api(facebook_token, "approve_contract", 1))
    print(ds.call_api(youtube_token, "approve_contract", 1))

    # res_1 = ds.call_api(facebook_token, "train_model_over_joined_data", label_name, query)
    # print(res_1.coef_)

    # For recording time: run it 10 times
    start_time = time.time()
    for _ in range(10):
        run_start_time = time.time()
        res = ds.call_api(facebook_token, "train_model_over_joined_data", label_name, query)
        run_end_time = time.time()
        # experiment_time_arr[0] is docker start time; experiment_time_arr[1] is f_end_time;
        print(res["experiment_time_arr"][0] - run_start_time,
              res["experiment_time_arr"][1] - res["experiment_time_arr"][0],
              run_end_time-res["experiment_time_arr"][1])
    print("Time for 10 runs", time.time() - start_time)

    # Last step: shut down the Data Station
    ds.shut_down()
