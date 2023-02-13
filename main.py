from ds import DataStation
from multiprocessing import Process, Event, Queue, Manager
from common import general_utils

def initialize_system(ds_config_path, app_config_path, need_to_recover = False):

    ds_config = general_utils.parse_config(ds_config_path)
    app_config = general_utils.parse_config(app_config_path)

    ds = DataStation(ds_config, app_config, need_to_recover)

    return ds