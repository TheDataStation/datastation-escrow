import os

import pandas as pd
import matplotlib.pyplot as plt
from common.utils import parse_config

if __name__ == '__main__':

    app_config = parse_config("app_connector_config.yaml")
    figure_name = "plots/" + app_config["connector_name"] + ".png"
    numbers_name = "numbers/" + app_config["connector_name"] + ".csv"

    overhead_df = pd.read_csv(numbers_name, header=None)
    num_runs = 5
    average_df = overhead_df.groupby(overhead_df.index // num_runs).sum()/5
    average_df['sum'] = average_df[list(average_df.columns)].sum(axis=1)

    print(average_df)

    lines = average_df.plot.line()
    lines.legend(labels=["get_uid",
                         "get_accessible_data",
                         "get_access_method",
                         "call_actual_api_with_process",
                         "write_log",
                         "total_time", ])

    # if not os.path.exists(figure_name):
    plt.savefig(figure_name)
