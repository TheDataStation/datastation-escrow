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
    print("creating tables")
    jerry_tables = ["customer", "lineitem", "partsupp", "region", "supplier"]
    for tbl in jerry_tables:
        filename = f"data/{tbl}.csv"
        f = open(filename, "rb")
        file_bytes = f.read()
        register_res = ds.call_api("jerry", "register_data", None, None, "jerry",
                                   f"{tbl}.csv", "file", f"{tbl}.csv", False, )
        ds.call_api("jerry", "upload_data", None, None, "jerry",
                    register_res.data_id, file_bytes, )

    david_tables = ["nation", "orders", "part"]
    for tbl in david_tables:
        filename = f"data/{tbl}.csv"
        f = open(filename, "rb")
        file_bytes = f.read()
        register_res = ds.call_api("david", "register_data", None, None, "david",
                                   f"{tbl}.csv", "file", f"{tbl}.csv", False, )
        ds.call_api("david", "upload_data", None, None, "david",
                    register_res.data_id, file_bytes, )
    print("created tables")

    # Step 3: jerry suggests a share saying they can both run retrieve_column
    agents = [1, 2]
    functions = ["column_intersection", "compare_distributions"]
    data_elements = [1, 2, 3, 4, 5, 6, 7, 8]
    ds.call_api("jerry", "suggest_share", None, None, "jerry", agents,
                functions, data_elements)

    # Step 4: they both acknowledge this share
    ds.call_api("jerry", "ack_data_in_share", None, None, "jerry", 1, 1)
    ds.call_api("jerry", "ack_data_in_share", None, None, "jerry", 2, 1)
    ds.call_api("jerry", "ack_data_in_share", None, None, "jerry", 3, 1)
    ds.call_api("jerry", "ack_data_in_share", None, None, "jerry", 4, 1)
    ds.call_api("jerry", "ack_data_in_share", None, None, "jerry", 5, 1)
    ds.call_api("david", "ack_data_in_share", None, None, "david", 6, 1)
    ds.call_api("david", "ack_data_in_share", None, None, "david", 7, 1)
    ds.call_api("david", "ack_data_in_share", None, None, "david", 8, 1)

    # Step 5: david calls the SQL sharing APIs.
    ix = ds.call_api("david", "column_intersection", 1, "pessimistic", 1,
                     "column0", None, 1, "column0")
    print("The result of column intersection is:", ix)

    compare_dstr_res = ds.call_api("jerry", "compare_distributions", 1,
                                   "pessimistic", 2, "column00", 2,
                                   "column00")
    print("The result of comparing distritbutions is:", compare_dstr_res)

    # Last step: shut down the Data Station
    ds.shut_down()
