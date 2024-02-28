from main import initialize_system
from common.general_utils import clean_test_env

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
    print(ds.call_api(f"dnpr", "register_and_upload_de", plaintext_bytes, ))

    dadr_de = f"integration_new/test_files/DNPR_p/DADR.csv"
    f = open(dadr_de, "rb")
    plaintext_bytes = f.read()
    f.close()
    print(ds.call_api(f"dadr", "register_and_upload_de", plaintext_bytes, ))

    # Step 3: Uploading CMPs
    dest_a_id = 0
    de_id = 1
    function = "calc_causal_dnpr"
    print(ds.call_api(f"dnpr", "upload_cmp", dest_a_id, de_id, function))

    # Step 4:
    additional_vars = ['F32']
    causal_dag = [('smoking_status', 'HbA1c'), ('F32', 'smoking_status'), ('F32', 'HbA1c')]
    treatment = "smoking_status"
    outcome = "HbA1c"
    res = ds.call_api(f"dadr", "run_causal_query", 2, additional_vars, causal_dag, treatment, outcome)
    print("Calculate causal effect is", res)

    ds.shut_down()
