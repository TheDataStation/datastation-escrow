import os
import shutil

from main import initialize_system

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    folders = ['SM_storage', 'Staging_storage']
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    # Step 0: System initialization

    ds_config = "data_station_config.yaml"
    app_config = "app_connector_config.yaml"

    ds = initialize_system(ds_config, app_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # Step 1: We create two new users of the Data Station
    num_users = 2
    for i in range(num_users):
        ds.create_user(f"user{i}", "string")

    # Step 2: Each user uploads their share of the dataset.
    table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    num_tables = len(table_names)
    for i in range(num_users):
        for tbl in table_names:
            filename = f"integration_new/test_files/sql/{tbl}{i}.csv"
            f = open(filename, "rb")
            file_bytes = f.read()
            register_res = ds.call_api(f"user{i}", "register_data", None, None, f"user{i}",
                                       f"{tbl}.csv", "file", f"{tbl}.csv", False, )
            ds.call_api(f"user{i}", "upload_data", None, None, f"user{i}",
                        register_res.de_id, file_bytes, )

    # Step 3: user0 suggests a share saying all users can run all functions in share
    agents = [1, 2]
    functions = ["select_star", "tpch_1", "tpch_2", "tpch_3", "tpch_4", "tpch_5", "tpch_6", "tpch_7",
                 "tpch_8", "tpch_9", "tpch_10", "tpch_11", "tpch_12", "tpch_13", "tpch_14", "tpch_15", "tpch_16"]
    total_des = num_users * num_tables
    data_elements = list(range(1, total_des + 1))
    ds.call_api("user0", "suggest_share", None, None, "user0", agents, functions, data_elements)

    # Step 4: all users acknowledge the share
    cur_de_id = 1
    for i in range(num_users):
        for j in range(num_tables):
            ds.call_api(f"user{i}", "ack_data_in_share", None, None, f"user{i}", cur_de_id, 1)
            cur_de_id += 1

    # Step 5: user0 calls functions
    select_star_res = ds.call_api("user0", "select_star", 1, "pessimistic", "nation")
    print("Result of select star from nation is:", select_star_res)
    # for i in range(1, len(functions)):
    #     tpch_res = ds.call_api("user0", f"tpch_{i}", 1, "pessimistic")
    #     print(f"Result of TPC_H {i} is:", tpch_res)
    tpch_res = ds.call_api("user0", f"tpch_16", 1, "pessimistic")
    print(f"Result is:", tpch_res)

    # Last step: shut down the Data Station
    ds.shut_down()
