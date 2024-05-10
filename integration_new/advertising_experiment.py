import os
import numpy as np
import random
import pandas as pd

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

    # Step 2: Upload the data.
    for agent in agents:
        if agent == "facebook":
            cur_token = facebook_token
        else:
            cur_token = youtube_token
        agent_de = f"integration_new/test_files/advertising_p/{agent}.csv"
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
            "on facebook.first_name = youtube.first_name and facebook.last_name = youtube.last_name"
    print(ds.call_api(facebook_token, "propose_contract",
                      dest_agents, data_elements, f, label_name, query))
    print(ds.call_api(facebook_token, "approve_contract", 1))
    print(ds.call_api(youtube_token, "approve_contract", 1))

    res = ds.call_api(facebook_token, "train_model_over_joined_data", label_name, query)
    print(res.coef_)

    # Last step: shut down the Data Station
    ds.shut_down()
