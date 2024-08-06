from main import initialize_system
from common.general_utils import clean_test_env


if __name__ == '__main__':

    # Clean up
    clean_test_env()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    ds = initialize_system(ds_config)

    # Step 1: Agent creation.
    num_agents = 2
    for i in range(num_agents):
        res = ds.create_agent(f"user{i+1}", "password", )
        print(res)

    # Step 2: Log in agent.
    user1_token = ds.login_agent("user1", "password")["data"]
    user2_token = ds.login_agent("user2", "password")["data"]

    # Step 2: Register DEs.
    # We use the csv files in integration_new/test_files/titanic_p
    token_dict = {0: user1_token, 1: user2_token}
    for i in range(num_agents):
        cur_token = token_dict[i]
        cur_f = f"integration_new/test_files/titanic_p/f{i}.csv"
        f = open(cur_f, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))

    # Agents propose and approve contracts.
    dest_agents = [2]
    data_elements = [1]
    print(ds.call_api(user2_token, "propose_contract", dest_agents, data_elements, "show_schema", [1]))
    print(ds.call_api(user1_token, "approve_contract", 1))

    # Agents execute the contract
    show_schema_res = ds.call_api(user2_token, "show_schema", [1])
    print("Schema is:", show_schema_res)

    # Last step: shut down the Data Station
    ds.shut_down()
