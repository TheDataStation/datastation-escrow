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

    # Step 1: We create two new users of the Data Station: jerry and david
    ds.create_user("user0", "string")
    ds.create_user("user1", "123456")

    # Step 2: Jerry and David each uploads their dataset.
    table_names = ["salary", "customer"]
    for tbl in table_names:
        for i in range(2):
            filename = f"integration_new/test_files/sql/{tbl}{i}.csv"
            f = open(filename, "rb")
            file_bytes = f.read()
            register_res = ds.call_api(f"user{i}", "register_data", None, None, f"user{i}",
                                       f"{tbl}{i}.csv", "file", f"{tbl}{i}.csv", False, )
            ds.call_api(f"user{i}", "upload_data", None, None, f"user{i}",
                        register_res.de_id, file_bytes, )

    # Last step: shut down the Data Station
    ds.shut_down()
