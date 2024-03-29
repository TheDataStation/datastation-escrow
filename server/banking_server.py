import os
import argparse

from fastapi import FastAPI, File, UploadFile
import uvicorn

from main import initialize_system
from dsapplicationregistration.dsar_core import get_registered_api_endpoint
from common.general_utils import clean_test_env

app = FastAPI()

# register agent
@app.post("/register_agent")
def register_agent(username, password):
    return ds.create_user(username, password)


if __name__ == "__main__":
    """
    Initializes an api to communicate by HTTP with a client.
    """
    parser = argparse.ArgumentParser(
        prog='FastAPI Client for DS',
        description='A Client API for Data Station',
        epilog='Text at the bottom of help')
    parser.add_argument('-c', '--ds_config', default='data_station_config.yaml', type=str)
    parser.add_argument('-p', '--port', default=8080, type=int)
    parser.add_argument('-hs', '--host', default="localhost", type=str)

    clean_test_env()

    args = parser.parse_args()
    print(args)
    port = args.port
    host = args.host

    # Initializing an instance of DS, according to the config file
    global ds
    ds = initialize_system(args.ds_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # Get the list of api_endpoints defined in the EPM (not including @functions)
    # Create FastAPI endpoints for them
    api_endpoints = get_registered_api_endpoint()
    for api in api_endpoints:
        print(api.__name__)
        if api.__name__ == "upload_transaction_data":
            upload_transaction_data_def = api

            def upload_transaction_data(user_id: int, file: UploadFile = File(...)):
                de_in_bytes = file.file.read()
                return upload_transaction_data_def(user_id, de_in_bytes)
            app.add_api_route(f"/{api.__name__}", upload_transaction_data, methods=["POST"])

    uvicorn.run(app, host='0.0.0.0', port=8000)
