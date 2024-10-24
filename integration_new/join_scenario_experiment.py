import os
import sys
import numpy as np
import csv
import pandas as pd
from faker import Faker
import time

from main import initialize_system
from common.general_utils import clean_test_env
from crypto import cryptoutils as cu


if __name__ == '__main__':

    # Experiment setups
    num_MB = int(sys.argv[1])
    if sys.argv[2] == "False":
        save_intermediate = False
    else:
        save_intermediate = True
    model_name = sys.argv[3]

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

    # # Step 0.5: Generate the data (only need to run once).
    # # They will be stored to integration_new/test_files/advertising_p/exp folder.
    # advertising_data_gen(num_MB)

    # Step 1: Agent creation. 2 agents: facebook and youtube.
    cipher_sym_key_list = []
    public_key_list = []
    agents = ["1", "2"]
    for i in range(len(agents)):
        sym_key = b'oHRZBUvF_jn4N3GdUpnI6mW8mts-EB7atAUjhVNMI58='
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        ds.create_agent(agents[i], "string", cipher_sym_key_list[i], public_key_list[i], )

    agent_1_token = ds.login_agent("1", "string")["data"]
    agent_2_token = ds.login_agent("2", "string")["data"]

    # Step 2: Upload the data.
    for agent in agents:
        if agent == "1":
            cur_token = agent_1_token
        else:
            cur_token = agent_2_token
        agent_de = f"../tpch_workdir/{num_MB}/split0.5/orders{agent}.tbl"
        # agent_de = f"integration_new/test_files/advertising_p/exp/{agent}_{int(num_MB)}.csv"
        f = open(agent_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))

    # Step 3: Train the joint model.
    dest_agents = [1]
    data_elements = [1, 2]
    f = "run_query"
    label_name = "clicked_on_ad"
    # "select youtube.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad from facebook inner join youtube on facebook.first_name = youtube.first_name and facebook.last_name = youtube.last_name"
    query = "select youtube.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad " \
            "from facebook inner join youtube " \
            "on facebook.first_name = youtube.first_name " \
            "and facebook.last_name = youtube.last_name " \
            "and jaro_similarity(facebook.email, youtube.email) > 0.9"
    query = """
CREATE TABLE ORDERS1  ( O_ORDERKEY       INTEGER NOT NULL,
            O_CUSTKEY        INTEGER NOT NULL,
            O_ORDERSTATUS    CHAR(1) NOT NULL,
            O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
            O_ORDERDATE      DATE NOT NULL,
            O_ORDERPRIORITY  CHAR(15) NOT NULL,  
            O_CLERK          CHAR(15) NOT NULL, 
            O_SHIPPRIORITY   INTEGER NOT NULL,
            O_COMMENT        VARCHAR(79) NOT NULL);

CREATE TABLE ORDERS2  ( O_ORDERKEY       INTEGER NOT NULL,
            O_CUSTKEY        INTEGER NOT NULL,
            O_ORDERSTATUS    CHAR(1) NOT NULL,
            O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
            O_ORDERDATE      DATE NOT NULL,
            O_ORDERPRIORITY  CHAR(15) NOT NULL,  
            O_CLERK          CHAR(15) NOT NULL, 
            O_SHIPPRIORITY   INTEGER NOT NULL,
            O_COMMENT        VARCHAR(79) NOT NULL);

COPY ORDERS1 FROM '{de1_filepath}' DELIMITER '|';
COPY ORDERS2 FROM '{de2_filepath}' DELIMITER '|';

SELECT COUNT(*) FROM orders1 JOIN orders2 ON o1.o_custkey = o2.o_custkey"""
    print(ds.call_api(agent_1_token, "propose_contract",
                      dest_agents, data_elements, f,
                      # function parameters
                      query))
    print(ds.call_api(agent_1_token, "approve_contract", 1))
    print(ds.call_api(agent_2_token, "approve_contract", 1))

    # res_1 = ds.call_api(facebook_token, "train_model_over_joined_data", label_name, query)
    # print(res_1.coef_)

    # For recording time: run it 11 times (extra times for warmup)
    if num_MB == 7:
        num_MB = 10
    start_time = time.time()
    for _ in range(2):
        run_start_time = time.time()
        res = ds.call_api(agent_1_token, f, model_name, label_name, query)
        run_end_time = time.time()
        # 1: fixed overhead 2: join DE time 3: model train time 4: fixed overhead
        with open(f"numbers/intermediate/{num_MB}_{model_name}_{save_intermediate}.csv", "a") as file:
            writer = csv.writer(file)
            if res["result"] is not None:
                writer.writerow([res["experiment_time_arr"][0] - run_start_time,
                                 res["result"][1] - res["experiment_time_arr"][0],
                                 res["experiment_time_arr"][1] - res["result"][1],
                                 run_end_time-res["experiment_time_arr"][1]])
    print("Time for all runs", time.time() - start_time)

    # Last step: shut down the Data Station
    ds.shut_down()
