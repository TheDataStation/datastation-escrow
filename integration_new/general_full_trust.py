from main import initialize_system
from common.general_utils import clean_test_env


if __name__ == '__main__':

    # Clean up
    clean_test_env()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    ds = initialize_system(ds_config)

    # Step 1: Test agent creation.
    num_users = 2
    for i in range(num_users):
        res = ds.create_user(f"user{i}", "string", )
        print(res)

    res = ds.call_api("user0", "list_all_agents")
    print(res)

    # Test get all functions.
    res = ds.call_api("user0", "get_all_functions")
    print(res)

    # Test get function info.
    res = ds.call_api("user0", "get_function_info", "print_first_row")
    print(res)

    # Step 2: Test register and upload DEs.
    # We use the 6 csv files in integration_new/test_files/titanic_p
    for i in range(6):
        cur_f = f"integration_new/test_files/titanic_p/f{i}.csv"
        f = open(cur_f, "rb")
        plaintext_bytes = f.read()
        f.close()
        u_id = i // 3
        register_res = ds.call_api(f"user{u_id}", "register_de", f"f{i}.csv", "file", f"f{i}.csv", True, )
        print(register_res)
        ds.call_api(f"user{u_id}", "upload_de", register_res["de_id"], plaintext_bytes, )

    # Test remove DEs.
    res = ds.call_api("user1", "remove_de_from_storage", 5)
    print(res)
    res = ds.call_api("user1", "remove_de_from_db", 5)
    print(res)
    res = ds.call_api("user1", "remove_de_from_db", 6)
    print(res)

    # Test list discoverable DEs with source agents.
    res = ds.call_api("user0", "list_discoverable_des_with_src")
    print(f"Listing discoverable data elements with their source agents: {res}")

    # Test proposing contracts
    dest_agents = [1, 2]
    data_elements = [1, 4]
    res = ds.call_api("user0", "propose_contract", dest_agents, data_elements, "print_first_row", 4)
    print(res)

    dest_agents = [1, 2]
    data_elements = [2, 4]
    res = ds.call_api("user0", "propose_contract", dest_agents, data_elements, "print_first_row", 2)
    print(res)

    # Test show contracts
    res = ds.call_api("user1", "show_contract", 1)
    print(res)
    res = ds.call_api("user0", "show_all_contracts_as_dest")
    print(res)
    res = ds.call_api("user1", "show_all_contracts_as_src")
    print(res)

    # Step 4: Agents approving the share.
    res = ds.call_api("user0", "approve_contract", 1)
    print(res)
    res = ds.call_api("user1", "approve_contract", 1)
    print(res)

    # Step 5: user calls execute contract
    print_first_row_res = ds.call_api("user0", "execute_contract", 1)
    print("Result of printing first row is:", print_first_row_res)

    # Last step: shut down the Data Station
    ds.shut_down()
