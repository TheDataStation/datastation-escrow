import os
import shutil
import json

from main import initialize_system
from common.pydantic_models.user import User

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

    # Step 1: We create two new users of the Data Station: jerry and david
    ds.create_user(User(user_name="jerry", password="string"))
    ds.create_user(User(user_name="david", password="123456"))

    # Step 2: Jerry logs in and uploads two datasets, first a file, then a postgres table
    file_path = "integration_new/test_files/plaintext/train-1.csv"
    cur_file = open(file_path, "rb")
    file_bytes = cur_file.read()
    optimistic_flag_1 = False
    de_name_1 = "file-1"
    de_access_param_1 = "file-1"
    register_res = ds.call_api("jerry",
                               "register_data",
                               None,
                               None,
                               "jerry",
                               de_name_1,
                               "file",
                               de_access_param_1,
                               optimistic_flag_1, )

    upload_res = ds.call_api("jerry",
                             "upload_data",
                             None,
                             None,
                             "jerry",
                             register_res.data_id,
                             file_bytes, )

    de_name_2 = "postgres_table"
    de_access_param_obj = {"credentials": "host='psql-mock-database-cloud.postgres.database.azure.com' "
                                          "dbname='booking1677197406169yivvxvruhvmbvqzh' "
                                          "user='gyhksqubzvuzcrdbhovifciu@psql-mock-database-cloud' "
                                          "password='mbaqbkpppcmhsdbqukeutann'",
                           "query": "SELECT * FROM bookings"}
    de_access_param_2 = json.dumps(de_access_param_obj)
    optimistic_flag_2 = False
    register_res = ds.call_api("jerry",
                               "register_data",
                               None,
                               None,
                               "jerry",
                               de_name_2,
                               "postgres",
                               de_access_param_2,
                               optimistic_flag_2, )

    # Step 3: jerry suggests a share saying david can access these two des.
    agents = [2]
    functions = ["retrieve_data"]
    data_elements = [1, 2]
    ds.call_api("jerry", "suggest_share", None, None, "jerry", agents, functions, data_elements)

    # Step 4: jerry acknowledges this share
    for data_id in data_elements:
        ds.call_api("jerry", "ack_data_in_share", None, None, "jerry", data_id, 1)

    # Step 5: david calls the API line_count. He runs it in optimistic mode.
    line_count_res = ds.call_api("david", "retrieve_data", 1, "pessimistic")
    print("The result of retrieving data is:", line_count_res)

    # Last step: shut down the Data Station
    ds.shut_down()
