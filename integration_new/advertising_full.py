from main import initialize_system
from common.general_utils import clean_test_env
from sklearn import tree
import matplotlib.pyplot as plt

if __name__ == '__main__':

    # Clean up
    clean_test_env()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    ds = initialize_system(ds_config)

    # Step 1: Agent creation.
    ds.create_user("advertiser", "string")
    ds.create_user("facebook", "string")
    ds.create_user("youtube", "string")

    # Step 2: Register and upload DEs.
    agents = ["facebook", "youtube"]
    for agent in agents:
        agent_de = f"integration_new/test_files/advertising_p/{agent}.csv"
        f = open(agent_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        res = ds.call_api(agent, "register_and_upload_de", f"{agent}.csv", plaintext_bytes)
        print(res)

    res = ds.list_all_des_with_src("advertiser")
    print(res)

    # First contract: materialize the output from the set linkage + join query
    dest_agents = []
    data_elements = [1, 2]
    query = "select de2.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad " \
            "from de1 inner join de2 " \
            "on de1.first_name = de2.first_name and de1.last_name = de2.last_name"
    res = ds.call_api("advertiser", "propose_SQL_query", dest_agents, data_elements, query)
    print(res)

    ds.call_api("facebook", "approve_SQL_query", 1)
    ds.call_api("youtube", "approve_SQL_query", 1)

    res = ds.call_api("advertiser", "execute_SQL_query", 1)
    print(res)

    # Second contract: propose train_model contract with intermediate DE
    data_elements = [3]
    model_name = "logistic_regression"
    label_name = "clicked_on_ad"
    res = ds.call_api("advertiser", "train_ML_model", data_elements, model_name, label_name)
    print(res.coef_)

    data_elements = [3]
    model_name = "decision_tree"
    label_name = "clicked_on_ad"
    res = ds.call_api("advertiser", "train_ML_model", data_elements, model_name, label_name)
    # tree.plot_tree(res)
    # plt.show()

    # Third contract: test to see if facebook has a large population who will respond to the ad
    dest_agents = [1]
    data_elements = [1]
    query = "SELECT COUNT(*) AS total_records, " \
            "SUM(CASE WHEN male = 1 AND married = 0 THEN 1 ELSE 0 END) AS male_1_married_0_records, " \
            "(SUM(CASE WHEN male = 1 AND married = 0 THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS proportion " \
            "FROM de1 ;"
    ds.call_api("advertiser", "propose_SQL_query", dest_agents, data_elements, query)
    ds.call_api("facebook", "approve_SQL_query", 4)
    res = ds.call_api("advertiser", "execute_SQL_query", 4)
    print(res)

    # Last step: shut down the Data Station
    ds.shut_down()
