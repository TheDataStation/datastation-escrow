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
async def upload_data_in_csv(token: str = Depends(oauth2_scheme),
                             dataset: UploadFile = File(...)):
    """
    For Facebook and YouTube: Upload a new dataset in CSV format.
    Parameters:\n
        dataset: the dataset to upload
    Returns:\n
        ID of the uploaded dataset if success.
    """
    de_in_bytes = dataset.file.read()
    return ds.call_api(token, "upload_data_in_csv", de_in_bytes)


@app.post("/run_query")
async def run_query(query: str,
                    token: str = Depends(oauth2_scheme),):
    """
    For advertiser: run a SQL query over Facebook and YouTube's data.\n
    Note: In the query statement, Facebook and YouTube's tables should be referred to as "facebook" and "youtube", respectively.\n
    Some example queries:\n
        select * from facebook;
        select clicked_on_ad from facebook inner join youtube on facebook.first_name = youtube.first_name and facebook.last_name = youtube.last_name;
    Facebook schema:\n
        first_name; last_name; email; male; less_than_twenty_five; live_in_states; married; liked_games_page
    YouTube schema:\n
        first_name; last_name; email; male; clicked_on_ad
    Parameters:\n
        query: a SQL query string.
    Returns:\n
        Output of the query.
    """
    res = ds.call_api(token, "run_query", query)
    return JSONResponse(content=res.to_json())


@app.post("/train_model_over_joined_data")
async def train_model_over_joined_data(model_name: str,
                                       label_name: str,
                                       token: str = Depends(oauth2_scheme),
                                       query: Optional[str] = None):
    """
    For advertiser: train a model over Facebook and YouTube's joint data.\n
    Parameters:\n
        model_name: what model to train. Either "logistic_regression" or "decision_tree".
        label_name: name of the dependent variable. (for demo, use: "clicked_on_ad")
        query: Optional SQL query specifying how data from Facebook and YouTube should be combined.
        (for demo, use: select youtube.male, less_than_twenty_five, live_in_states, married, liked_games_page, clicked_on_ad from facebook inner join youtube on facebook.first_name = youtube.first_name and facebook.last_name = youtube.last_name")
        Note: query only needs to specified the first time. (The demo assume that there's only one way to combine the data).
    Returns:\n
        Parameters of the model.
    """
    if query is None:
        res = ds.call_api(token, "train_model_over_joined_data", model_name, label_name)
    else:
        res = ds.call_api(token, "train_model_over_joined_data", model_name, label_name, query)
    if model_name == "logistic_regression":
        model_parameters = {
            "intercept": res.intercept_.tolist(),
            "coefficients": res.coef_.tolist(),
            "classes": res.classes_.tolist(),
        }
    elif model_name == "decision_tree":
        model_parameters = {
            "feature_importances": res.feature_importances_.tolist(),
            "tree_depth": int(res.get_depth()),
            "n_leaves": int(res.get_n_leaves()),
            # "n_classes": res.n_classes_,
        }
    else:
        model_parameters = None
    return model_parameters


@app.post("/propose_contract")
async def propose_contract(dest_agents: List[int],
                           des: List[int],
                           f: str,
                           token: str = Depends(oauth2_scheme),
                           args: Optional[List[Any]] = None
                           ):
    """
    For advertiser: proposes a new contract.\n
    Parameters:\n
        dest_agents: list of agent IDs who can access the contract output. (e.g. [1])
        des: list of data IDs the contract needs to access. (e.g. [1,2])
        f: function to run in this contract. (e.g. run_query)
        args: arguments of this function, given in a list. (e.g. if f is run_query, args can be ["select * from facebook"])
    Returns:\n
        ID of the newly proposed contract if success.
    """
    return ds.call_api(token, "propose_contract", dest_agents, des, f, *args)


@app.post("/approve_contract")
async def approve_contract(contract_id: int, token: str = Depends(oauth2_scheme)):
    """
    For Facebook and YouTube: approve contract proposed by the advertiser.\n
    Parameters:\n
        contract_id: id of the contract to approve.
    """
    return ds.call_api(token, "approve_contract", contract_id)


@app.post("/show_all_contracts_as_src")
async def show_all_contracts_as_src(token: str = Depends(oauth2_scheme)):
    """
    For Facebook and YouTube: display all contract waiting for approval.
    """
    return ds.call_api(token, "show_all_contracts_as_src")


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