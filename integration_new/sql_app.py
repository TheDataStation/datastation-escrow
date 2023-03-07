import os
import shutil

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

    # Step 2: Jerry and David each uploads their dataset.
    test1_name = "integration_new/test_files/sql/test1.csv"
    test1 = open(test1_name, "rb")
    test1_bytes = test1.read()
    register_res = ds.call_api("jerry", "register_data", None, None, "jerry", "test1.csv", "file", "test1.csv", False, )
    ds.call_api("jerry", "upload_data", None, None, "jerry", register_res.data_id, test1_bytes, )

    test2_name = "integration_new/test_files/sql/test2.csv"
    test2 = open(test2_name, "rb")
    test2_bytes = test2.read()
    register_res = ds.call_api("david", "register_data", None, None, "david", "test2.csv", "file", "test2.csv", False, )
    ds.call_api("david", "upload_data", None, None, "david", register_res.data_id, test2_bytes, )

    # Step 3: jerry suggests a share saying they can both run retrieve_column
    agents = [1, 2]
    functions = ["get_column"]
    data_elements = [1, 2]
    ds.call_api("jerry", "suggest_share", None, None, "jerry", agents, functions, data_elements)

    # Step 4: they both acknowledge this share
    ds.call_api("jerry", "ack_data_in_share", None, None, "jerry", 1, 1)
    ds.call_api("david", "ack_data_in_share", None, None, "david", 2, 1)

    # Step 5: david calls the API get_column.
    get_column_res = ds.call_api("david", "get_column", 1, "pessimistic", 1, "PersonID")
    print("The result of get column is:", get_column_res)

    # Last step: shut down the Data Station
    ds.shut_down()
