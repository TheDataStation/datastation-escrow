import os
import shutil

from main import initialize_system
from common.general_utils import clean_test_env


if __name__ == '__main__':

    clean_test_env()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    ds = initialize_system(ds_config)

    # Step 1: Create new agents
    num_users = 2
    for i in range(num_users):
        res = ds.create_user(f"bank{i}", "string", )

    res = ds.call_api("bank0", "list_all_agents")
    print(res)

    ds.shut_down()
