import os
import shutil

from main import initialize_system
from crypto import cryptoutils as cu

def cleanup():
    if os.path.exists("data_station.db"):
        os.remove("data_station.db")
    folders = ['SM_storage']
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)


if __name__ == '__main__':

    # Clean up
    cleanup()

    # Step 0: System initialization

    ds_config = "data_station_config.yaml"
    app_config = "app_connector_config.yaml"
    ds = initialize_system(ds_config, app_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    wal_path = ds.write_ahead_log.wal_path
    if os.path.exists(log_path):
        os.remove(log_path)

    ds_public_key = ds.key_manager.ds_public_key

    # Step 1: Create new agents
    cipher_sym_key_list = []
    public_key_list = []
    num_users = 2
    for i in range(num_users):
        sym_key = b'oHRZBUvF_jn4N3GdUpnI6mW8mts-EB7atAUjhVNMI58='
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        ds.create_user(f"user{i}", "string", cipher_sym_key_list[i], public_key_list[i], )

    # Step 2: Create and upload data elements.
    # We use the 6 csv files in integration_new/test_files/titanic_p
    for i in range(6):
        cur_f = f"integration_new/test_files/titanic_p/f{i}.csv"
        f = open(cur_f, "rb")
        plaintext_bytes = f.read()
        f.close()
        cur_user_sym_key = ds.key_manager.agents_symmetric_key[1]
        ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(plaintext_bytes)
        u_id = i // 3
        register_res = ds.call_api(f"user{u_id}", "register_de",
                                   f"f{i}.csv", "file", f"f{i}.csv", True, )
        ds.call_api(f"user{u_id}", "upload_de",
                    register_res["de_id"], ciphertext_bytes, )

    res = ds.call_api("user0", "list_discoverable_des")
    print(f"Result of listing discoverable data elements is {res}")

    # Step 3: Agent suggesting shares
    agents = [1]
    data_elements = [4]
    ds.call_api("user0", "suggest_share", agents, data_elements, "print_first_row", 4)

    # Approval agent calls show_share() to see content of the share
    share_obj = ds.call_api("user1", "show_share", 1)
    print(share_obj)

    # Step 4: Agents approving the share.
    ds.call_api(f"user1", "approve_share", 1)

    # Step 5: user calls execute share
    print_first_row_res = ds.call_api("user0", "execute_share", 1)
    print("Result of printing first row is:", print_first_row_res)

    # Last step: shut down the Data Station
    ds.shut_down()
