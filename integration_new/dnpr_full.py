from main import initialize_system
from common.general_utils import clean_test_env
import networkx as nx

if __name__ == '__main__':

    # Clean up
    clean_test_env()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    ds = initialize_system(ds_config)

    # Step 1: Agent creation.
    ds.create_user("dnpr", "string")
    ds.create_user("dadr", "string")

    # Step 2: Register and upload DEs.
    dnpr_de = f"integration_new/test_files/DNPR_p/DNPR.csv"
    f = open(dnpr_de, "rb")
    plaintext_bytes = f.read()
    f.close()
    register_res = ds.call_api(f"dnpr", "register_de", f"DNPR.csv", True, )
    print(register_res)
    ds.call_api(f"dnpr", "upload_de", register_res["de_id"], plaintext_bytes, )

    dadr_de = f"integration_new/test_files/DNPR_p/DADR.csv"
    f = open(dadr_de, "rb")
    plaintext_bytes = f.read()
    f.close()
    register_res = ds.call_api(f"dadr", "register_de", f"DADR.csv", True, )
    print(register_res)
    ds.call_api(f"dadr", "upload_de", register_res["de_id"], plaintext_bytes, )

    # Proposing and approving contracts
    dest_agents = [2]
    data_elements = [1, 2]
    additional_vars = ['F32']
    causal_dag = [('smoking_status', 'HbA1c'), ('F32', 'smoking_status'), ('F32', 'HbA1c')]
    treatment = "smoking_status"
    outcome = "HbA1c"
    res = ds.call_api("dadr", "propose_contract", dest_agents, data_elements, "calc_causal_dnpr",
                      additional_vars, causal_dag, treatment, outcome)
    print(res)

    ds.call_api("dnpr", "approve_contract", 1)
    ds.call_api("dadr", "approve_contract", 1)

    # Executing the contract
    res = ds.call_api("dadr", "execute_contract", 1)
    print(res)

    # Last step: shut down the Data Station
    ds.shut_down()
