from clientapi.client_api import ClientAPI
from ds import DataStation
from multiprocessing import Process, Event, Queue, Manager

class api_call():
    api_name = None
    api_args = None

def initialize_system(ds_config, app_config, need_to_recover = False):

    shared_queue = Queue()
    ds = DataStation(ds_config, app_config, need_to_recover)

    # ds.start()

    client_api = ClientAPI(shared_queue)