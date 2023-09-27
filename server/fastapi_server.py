import os
import shutil
import argparse

from fastapi import FastAPI, File, UploadFile
import uvicorn

from main import initialize_system
from dsapplicationregistration.dsar_core import get_registered_api_endpoint

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

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

    # Clean up
    if os.path.exists("data_station.db"):
        os.remove("data_station.db")
    folders = ['SM_storage']
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
    ds = initialize_system(args.ds_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # Let's get the list of function objects (the api_endpoints defined in the EPF)
    api_endpoints = get_registered_api_endpoint()
    for api in api_endpoints:
        print(api.__name__)
        if api.__name__ == "upload_de":
            upload_de_def = api

            def upload_file(user_id: int, data_id: int, file: UploadFile = File(...)):
                contents = file.file.read()
                return upload_de_def(user_id, data_id, contents)
            app.add_api_route(f"/{api.__name__}", upload_file, methods=["POST"])
        else:
            app.add_api_route(f"/{api.__name__}", api, methods=["POST"])

    uvicorn.run(app, host='0.0.0.0', port=8000)
