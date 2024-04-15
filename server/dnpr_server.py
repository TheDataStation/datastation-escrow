import os
import argparse

from fastapi import FastAPI, Depends, HTTPException, status, File, UploadFile
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import uvicorn

from main import initialize_system
from common.general_utils import clean_test_env

app = FastAPI()

# Create a new user.
@app.post("/create_agent",)
def create_agent(username: str, password: str):
    """
    Create a new agent.\n
    Parameters:\n
        username: username
        password: password
    Returns:\n
        ID of the newly created user if success.
    """
    return ds.create_agent(username, password)


# Login Related
# Specifying oauth2_scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@app.post("/token/", include_in_schema=False)
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    response = ds.login_agent(form_data.username,
                              form_data.password)
    if response["status"] == 1:
        # raise an exception to indicate login has failed
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    else:
        access_token = response["data"]
        return {"access_token": access_token, "token_type": "bearer"}


@app.post("/upload_data_in_csv")
async def upload_data_in_csv(token: str = Depends(oauth2_scheme), dataset: UploadFile = File(...)):
    """
    Upload a new dataset in CSV format. Uploaded dataset needs to contain the column "CPR".\n
    Parameters:\n
        dataset: the dataset to upload
    Returns:\n
        ID of the uploaded dataset if success.
    """
    de_in_bytes = dataset.file.read()
    return ds.call_api(token, "upload_data_in_csv", de_in_bytes)


@app.post("/approve_all_causal_queries")
async def approval_all_causal_queries(token: str = Depends(oauth2_scheme)):
    """
    For DNPR only: approve all incoming causal queries.\n
    """
    return ds.call_api(token, "approve_all_causal_queries")


@app.post("/run_causal_query")
async def run_causal_query(user_de_id: int,
                           additional_vars: list[str],
                           dag_spec: list[tuple[str, str]],
                           treatment: str,
                           outcome: str,
                           token: str = Depends(oauth2_scheme)):
    """
    For users looking to run causal queries with confounders from DNPR's data.\n
    Available confounders from DNPR's data:\n
        F32: Depression.
        J9: Influenza.
        K25: Gastric ulcer.
    Parameters:\n
        user_de_id: ID of user's data.
        additional_vars: the list of confounders to use from DNPR's data. These will be joined to the user's data. (e.g. ["F32"])
        dag_spec: specification of the causal DAG. This will be used to create a directed graph with networkx. (e.g.[("F32","smoking_status"), ("F32","HbA1c"), ("smoking_status", "HbA1c")])
        treatment: the treatment variable from user's data. (e.g. smoking_status)
        outcome: the outcome variable from users' data. (e.g. HbA1c)
    Returns:\n
        The calculated causal effect of treatment on outcome (a number).
    """
    return ds.call_api(token, "run_causal_query", user_de_id, additional_vars, dag_spec, treatment, outcome)


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

    sys_args = parser.parse_args()
    # print(args)
    port = sys_args.port
    host = sys_args.host

    # Initializing an instance of DS, according to the config file
    global ds
    ds = initialize_system(sys_args.ds_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    uvicorn.run(app, host='0.0.0.0', port=8000)
