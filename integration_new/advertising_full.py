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
    facebook_de = f"integration_new/test_files/advertising_p/facebook.csv"
    f = open(facebook_de, "rb")
    plaintext_bytes = f.read()
    f.close()
    register_res = ds.call_api(f"facebook", "register_de", f"facebook.csv", True, )
    print(register_res)
    ds.call_api(f"facebook", "upload_de", register_res["de_id"], plaintext_bytes, )

    youtube_de = f"integration_new/test_files/advertising_p/youtube.csv"
    f = open(youtube_de, "rb")
    plaintext_bytes = f.read()
    f.close()
    register_res = ds.call_api(f"youtube", "register_de", f"youtube.csv", True, )
    print(register_res)
    ds.call_api(f"youtube", "upload_de", register_res["de_id"], plaintext_bytes, )

    # Proposing and approving first contract
    dest_agents = [1]
    data_elements = [1, 2]
    query = "select de2.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad " \
            "from de1 inner join de2 " \
            "on de1.first_name = de2.first_name and de1.last_name = de2.last_name"
    label = "clicked_on_ad"
    ds.call_api("advertiser", "propose_contract", dest_agents, data_elements, "logistic_wrapper",
                query, label)
    ds.call_api("facebook", "approve_contract", 1)
    ds.call_api("youtube", "approve_contract", 1)

    # Executing first contract: a logistic regression model. Then look at the coefficients.
    res = ds.call_api("advertiser", "execute_contract", 1)
    # print(res.coef_)

    # Proposing and approving second contract
    dest_agents = [1]
    data_elements = [1, 2]
    query = "select de2.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad " \
            "from de1 inner join de2 " \
            "on de1.first_name = de2.first_name and de1.last_name = de2.last_name"
    label = "clicked_on_ad"
    ds.call_api("advertiser", "propose_contract", dest_agents, data_elements, "decision_tree_wrapper",
                query, label)
    ds.call_api("facebook", "approve_contract", 2)
    ds.call_api("youtube", "approve_contract", 2)

    # Executing second contract: a decision model. Then look at the splits.
    res = ds.call_api("advertiser", "execute_contract", 2)
    # tree.plot_tree(res)
    # plt.show()

    # Third contract: test to see if facebook has a good population who will respond to the ad
    dest_agents = [1]
    data_elements = [1]
    query = "SELECT COUNT(*) AS total_records, " \
            "SUM(CASE WHEN male = 1 AND married = 0 THEN 1 ELSE 0 END) AS male_1_married_0_records, " \
            "(SUM(CASE WHEN male = 1 AND married = 0 THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS proportion " \
            "FROM de1 ;"
    ds.call_api("advertiser", "propose_contract", dest_agents, data_elements, "run_SQL_query",
                query)
    ds.call_api("facebook", "approve_contract", 3)

    # Executing third contract: a decision model.
    res = ds.call_api("advertiser", "execute_contract", 3)
    print(res)

    # Last step: shut down the Data Station
    ds.shut_down()
