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
    ds.create_agent("advertiser", "string")
    ds.create_agent("facebook", "string")
    ds.create_agent("youtube", "string")

    advertiser_token = ds.login_agent("advertiser", "string")["data"]
    facebook_token = ds.login_agent("facebook", "string")["data"]
    youtube_token = ds.login_agent("youtube", "string")["data"]

    # Step 2: Register and upload DEs.
    agents = ["facebook", "youtube"]
    for agent in agents:
        if agent == "facebook":
            cur_token = facebook_token
        else:
            cur_token = youtube_token
        agent_de = f"integration_new/test_files/advertising_p/{agent}.csv"
        f = open(agent_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        print(ds.call_api(cur_token, "upload_data_in_csv", plaintext_bytes))

    print(ds.call_api(advertiser_token, "list_all_des_with_src"))

    # Contract 1: Training the joint model.
    dest_agents = [1]
    data_elements = [1, 2]
    f = "train_model_over_joined_data"
    model_name = "logistic_regression"
    label_name = "clicked_on_ad"
    query = "select youtube.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad " \
            "from facebook inner join youtube " \
            "on facebook.first_name = youtube.first_name and facebook.last_name = youtube.last_name"
    print(ds.call_api(advertiser_token, "propose_contract",
                      dest_agents, data_elements, f, model_name, label_name, query))
    print(ds.call_api(facebook_token, "approve_contract", 1))
    print(ds.call_api(youtube_token, "approve_contract", 1))

    res = ds.call_api(advertiser_token, "train_model_over_joined_data", model_name, label_name, query)
    print(res.coef_)

    # # Some test on intermediate DEs
    # res = ds.call_api(advertiser_token, "train_model_over_joined_data", "decision_tree", "clicked_on_ad")
    # tree.plot_tree(res)
    # plt.show()

    # Query to see if facebook has a large population who will respond to the ad
    dest_agents = [1]
    data_elements = [1]
    query = "SELECT COUNT(*) AS total_records, " \
            "SUM(CASE WHEN male = 1 AND married = 0 THEN 1 ELSE 0 END) AS male_1_married_0_records, " \
            "(SUM(CASE WHEN male = 1 AND married = 0 THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS proportion " \
            "FROM facebook;"
    print(ds.call_api(advertiser_token, "propose_contract", dest_agents, data_elements, "run_query", query))
    print(ds.call_api(facebook_token, "approve_contract", 2))
    print(ds.call_api(advertiser_token, "run_query", query))

    # Last step: shut down the Data Station
    ds.shut_down()
