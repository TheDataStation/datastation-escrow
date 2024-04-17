import os
import argparse

from fastapi import FastAPI, Depends, HTTPException, status, File, UploadFile
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import Optional, List, Any
import uvicorn
from fastapi.responses import JSONResponse

from main import initialize_system
from common.general_utils import clean_test_env

app = FastAPI()


# Create a new user.
@app.post("/create_agent", )
def create_agent(username: str, password: str):
    """
    Creates a new agent.\n
    Parameters:\n
        username: username\n
        password: password\n
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
    Upload a new dataset in CSV format.\n
    Parameters:\n
        dataset: the dataset to upload
    Returns:\n
        ID of the uploaded dataset if success.
    """
    de_in_bytes = dataset.file.read()
    return ds.call_api(token, "upload_data_in_csv", de_in_bytes)


@app.post("/list_all_datasets")
async def list_all_datasets(token: str = Depends(oauth2_scheme)):
    """
    List all uploaded datasets.\n
    Returns:\n
        A list of {data ID, owner ID, owner username}.
    """
    return ds.call_api(token, "list_all_des_with_src")


@app.post("/show_schema")
async def show_schema(de_ids: list[int],
                      token: str = Depends(oauth2_scheme)):
    """
    Display the schema of requested data.\n
    Parameters:\n
        de_ids: a list of data IDs whose schema is to be displayed. (e.g. [1, 2, 3])
    Returns:\n
        Schema of the data.
    """
    return ds.call_api(token, "show_schema", de_ids)


@app.post("/share_sample")
async def share_sample(de_id: int,
                       sample_size: int,
                       token: str = Depends(oauth2_scheme), ):
    """
    Return a sample of requested data.\n
    Parameters:\n
        de_id: data ID. (e.g. 1)
        sample_size: size of the sample.
    Returns:\n
        Sample of the data.
    """
    res = ds.call_api(token, "share_sample", de_id, sample_size)
    return JSONResponse(content=res.to_json())


@app.post("/check_column_compatibility")
async def check_column_compatibility(de_1: int,
                                     de_2: int,
                                     cols_1: list[str],
                                     cols_2: list[str],
                                     token: str = Depends(oauth2_scheme), ):
    """
    Check the compatibility of selected columns from 2 DEs.\n
    Numerical columns are compared based on the KS statistic. Textual columns are compared based on Jaccard similarity.\n
    Parameters:\n
        de_1: the first data ID. (e.g. 1)
        de_2: the second data ID. (e.g. 2)
        cols_1: a list of column names from the first data. (e.g. ["amt", "gender", "city", "state", "city_pop_thousands", "dob"])
        cols_2: a list of column names from the second data. (e.g. ["dollar_amount", "gender", "city", "state", "city_population", "customer_date_of_birth"])
    Returns:\n
        Comparison results.
    """
    res = ds.call_api(token, "check_column_compatibility", de_1, de_2, cols_1, cols_2)
    return res


@app.post("/train_model_with_conditions")
async def train_model_with_conditions(label_name: str,
                                      train_de_ids: list[int],
                                      test_de_ids: list[int],
                                      size_constraint: int,
                                      accuracy_constraint: float,
                                      token: str = Depends(oauth2_scheme), ):
    """
    Train a joint model over the bank's joint data with 2 conditions: 1) each bank contributes enough data, 2) only release result model if it's sufficiently accurate.\n
    Parameters:\n
        label_name: name of the dependent variable (e.g. is_fraud)
        train_de_ids: the list of training data IDs (e.g. [1, 3])
        test_de_ids: the list of test data IDs (e.g. [2, 4])
        size_constraint: the minimum number of train records each bank must provide (e.g. 2000)
        accuracy_constraint: the minimum accuracy the result model needs to achieve to be released. (e.g. 0.95)
    Returns:\n
        Parameters of the model.
    """
    res = ds.call_api(token, "train_model_with_conditions",
                      label_name, train_de_ids, test_de_ids, size_constraint, accuracy_constraint)
    model_parameters = {
        "intercept": res.intercept_.tolist(),
        "coefficients": res.coef_.tolist(),
        "classes": res.classes_.tolist(),
    }
    return model_parameters


@app.post("/propose_contract")
async def propose_contract(dest_agents: List[int],
                           des: List[int],
                           f: str,
                           token: str = Depends(oauth2_scheme),
                           args: Optional[List[Any]] = None
                           ):
    """
    Proposes a new contract.\n
    Parameters:\n
        dest_agents: list of agent IDs who can access the contract output. (e.g. [1])
        des: list of data IDs the contract needs to access. (e.g. [1,2])
        f: function to run in this contract. (e.g. train_model_with_conditions)
        args: arguments of this function, given in a list. (e.g. if f is train_model_with_conditions, args can be ["is_fraud", [1,3], [2,4], 2000, 0.95])
    Returns:\n
        ID of the newly proposed contract if success.
    """
    return ds.call_api(token, "propose_contract", dest_agents, des, f, *args)


@app.post("/approve_contract")
async def approve_contract(contract_id: int, token: str = Depends(oauth2_scheme)):
    """
    Approve contract.\n
    Parameters:\n
        contract_id: id of the contract to approve.
    """
    return ds.call_api(token, "approve_contract", contract_id)


@app.post("/show_contracts_pending_my_approval")
async def show_contracts_pending_my_approval(token: str = Depends(oauth2_scheme)):
    """
    Show all contracts pending the current user's approval.
    """
    return ds.call_api(token, "show_contracts_pending_my_approval")


@app.post("/show_my_contracts_pending_approval")
async def show_my_contracts_pending_approval(token: str = Depends(oauth2_scheme)):
    """
    Show all contracts pending approval that the current users will access.
    """
    return ds.call_api(token, "show_my_contracts_pending_approval")


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
