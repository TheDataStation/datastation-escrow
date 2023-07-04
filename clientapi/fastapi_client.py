import os
import shutil
import argparse
from fastapi import FastAPI
import uvicorn

from main import initialize_system
from userregister import user_register

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/method1')
def method1_endpoint(param1: str):
    result = param1
    return {'result': result}

if __name__ == "__main__":
    """
    Initializes an api to communicate by HTTP with a client.
    """
    parser = argparse.ArgumentParser(
        prog='FastAPI Client for DS',
        description='A Client API for Data Station',
        epilog='Text at the bottom of help')
    parser.add_argument('-c', '--ds_config', default='data_station_config.yaml', type=str)
    parser.add_argument('-a', '--app_config', default='app_connector_config.yaml', type=str)
    parser.add_argument('-p', '--port', default=8080, type=int)
    parser.add_argument('-hs', '--host', default="localhost", type=str)

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")
    folders = ['SM_storage', 'Staging_storage']
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    args = parser.parse_args()
    print(args)
    port = args.port
    host = args.host

    global ds
    ds = initialize_system(args.ds_config, args.app_config)

    # TODO: remove these and put them somewhere else
    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    uvicorn.run(app, host='0.0.0.0', port=8000)
