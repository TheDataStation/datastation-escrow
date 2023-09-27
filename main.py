from ds import DataStation
from multiprocessing import Process, Event, Queue, Manager
from common import general_utils
from escrowapi.escrow_api import EscrowAPI

def initialize_system(ds_config_path, need_to_recover=False):
    """
    Initializes an instance of DataStation.

    Parameters:
        ds_config_path: path to config yaml file
        need_to_recover: Boolean, are we starting Data Station in recovery mode

    Returns:
        newly instantiated DataStation class
    """

    ds_config = general_utils.parse_config(ds_config_path)
    ds = DataStation(ds_config, need_to_recover)
    EscrowAPI.set_comp(ds)

    return ds
