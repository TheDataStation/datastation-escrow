import os
from common.general_utils import parse_config

if __name__ == '__main__':

    # Get current file name
    app_config = parse_config("app_connector_config.yaml")
    numbers_file_name = "numbers/" + app_config["connector_name"] + ".csv"

    # For each dependency graph, we reconstruct the csv file
    if os.path.exists(numbers_file_name):
        os.remove(numbers_file_name)

    os.system("python -m integration_tests.plot_one_full_trust integration_tests/test_config/workload_config_10.yaml")
    os.system("python -m integration_tests.plot_one_full_trust integration_tests/test_config/workload_config_100.yaml")
    os.system("python -m integration_tests.plot_one_full_trust integration_tests/test_config/workload_config_500.yaml")
    os.system("python -m integration_tests.plot_one_full_trust integration_tests/test_config/workload_config_1000.yaml")
    os.system("python -m integration_tests.plot_one_full_trust integration_tests/test_config/workload_config_5000.yaml")
