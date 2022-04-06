from fastapi import FastAPI

from clientapi.client_api import ClientAPI
# from pydantic_models.api import *
# from pydantic_models.api_dependency import *
# from pydantic_models.response import *
from common.pydantic_models.policy import Policy
from common.pydantic_models.user import User
# from pydantic_models.dataset import Dataset
from fastapi import UploadFile, File, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

import main

# Specifying oauth2_scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

client_api: ClientAPI = None

@app.get('/')
async def root():
    return {"message": "Welcome to Data Station"}

# On startup: doing some initializations from DBS
@app.on_event("startup")
async def startup_event():
    global client_api
    if client_api is None:
        client_api = main.initialize_system(main.parse_config("data_station_config.yaml"),
                                            main.parse_config("app_connector_config.yaml"),)

# Register a new user
@app.post("/users/")
async def create_user(user: User):
    return client_api.create_user(user)

# The following API allows user log-in.
@app.post("/token/")
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    response = client_api.login_user(form_data.username,
                                     form_data.password)
    if response == -1:
        # raise an exception to indicate login has failed
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    else:
        return response

# # Upload a new API
# @app.post("/api/")
# async def upload_api(api: API):
#     return client_api.upload_api(api)

# Look at all available APIs
@app.get("/api/")
async def get_all_apis(token: str = Depends(oauth2_scheme)):
    return client_api.get_all_apis(token)

# # Upload a new API Dependency
# @app.post("/api_depend/")
# async def upload_api_dependency(api_dependency: APIDependency):
#     return client_api.upload_api_dependency(api_dependency)

# Look at all available API dependencies
@app.get("/api_depend/")
async def get_all_api_dependencies(token: str = Depends(oauth2_scheme)):
    return client_api.get_all_api_dependencies(token)

# Upload a new dataset
@app.post("/dataset/")
async def upload_dataset(data_name: str,
                         data_type: str,
                         token: str = Depends(oauth2_scheme),
                         data: UploadFile = File(...),):
    # Load the file in bytes
    data_in_bytes = bytes(await data.read())
    return client_api.upload_dataset(data_name, data_in_bytes, data_type, token)

# Remove a dataset that's uploaded
@app.delete("/dataset/")
async def remove_dataset(data_name: str,
                         token: str = Depends(oauth2_scheme),):
    return client_api.remove_dataset(data_name, token)

# Upload a new policy
@app.post("/policy/")
async def upload_policy(policy: Policy,
                        token: str = Depends(oauth2_scheme),):
    return client_api.upload_policy(policy, token)

# Remove a policy
@app.delete("/policy/")
async def remove_policy(policy: Policy,
                        token: str = Depends(oauth2_scheme),):
    return client_api.remove_policy(policy, token)

# Look at all available policies
@app.get("/policy/")
async def get_all_policies():
    return client_api.get_all_policies()

# Let's now add ways for fastapi users to call preprocess, modeltrain, and predict

# preprocess
@app.post("/applications/preprocess")
async def preprocess():
    return client_api.call_api("preprocess")

# modeltrain
@app.post("/applications/modeltrain")
async def modeltrain():
    return client_api.call_api("modeltrain")

# predict
@app.post("/applications/predict")
async def predict(accuracy: int, num_times: int):
    return client_api.call_api("predict", accuracy, num_times)

# # API for Data Users: Preprocess
# @app.get("/simpleML/datapreprocess/")
# async def data_process_wrapper(token: str = Depends(oauth2_scheme)):
#     # First set an execution mode
#     exe_mode = "optimistic"
#     # Get caller uname
#     username = user_register.authenticate_user(token)
#     # Get caller ID
#     get_caller_response = database_api.get_user_by_user_name(User(user_name=username,))
#     if get_caller_response.status != 1:
#         return Response(status=get_caller_response.status, message=get_caller_response.msg)
#     user_id = get_caller_response.data[0].id
#     # Calling the GK
#     gatekeeper_response = gatekeeper.broker_access(user_id, "Preprocess", exe_mode)
#     # print(gatekeeper_response)
#     # Case one: output contains info from unauthorized datasets
#     if not gatekeeper_response["status"]:
#         return {"status": "Not enough permissions"}
#         # Depends on the execution mode, we may still have a result that needs to be put into staging storage.
#     # Case two: status == True, can return directly to users
#     else:
#         # In here we need to know the output format of the data: a tuple with np arrays
#         data_output = []
#         for ele in gatekeeper_response["data"]:
#             data_output.append(ele.tolist())
#         json_data_output = json.dumps(data_output)
#         return {"data": json_data_output}
#
# # API for Data Users: ModelTrain
# @app.get("/simpleML/modeltrain/")
# async def model_train_wrapper(token: str = Depends(oauth2_scheme)):
#     # First set an execution mode
#     exe_mode = "optimistic"
#     # Get caller uname
#     username = user_register.authenticate_user(token)
#     # Get caller ID
#     get_caller_response = database_api.get_user_by_user_name(User(user_name=username,))
#     if get_caller_response.status != 1:
#         return Response(status=get_caller_response.status, message=get_caller_response.msg)
#     user_id = get_caller_response.data[0].id
#     # Calling the GK
#     gatekeeper_response = gatekeeper.broker_access(user_id, "ModelTrain", exe_mode)
#     print(gatekeeper_response["data"].coef_)
#
#     # Case one: output contains info from unauthorized datasets
#     if not gatekeeper_response["status"]:
#         return {"status": "Not enough permissions"}
#         # Depends on the execution mode, we may still have a result that needs to be put into staging storage.
#     # Case two: status == True, can return directly to users
#     else:
#         # In here we need to know the output format of the data: a tuple with np arrays
#         data_output = gatekeeper_response["data"].coef_.tolist()
#         json_data_output = json.dumps(data_output)
#         return {"data": json_data_output}
#
# # API for Data Users: Predict
# @app.post("/simpleML/predict/")
# async def predict_wrapper(token: str = Depends(oauth2_scheme),
#                           data: UploadFile = File(...),):
#     # First set an execution mode
#     exe_mode = "pessimistic"
#     # Get caller uname
#     username = user_register.authenticate_user(token)
#     # Get caller ID
#     get_caller_response = database_api.get_user_by_user_name(User(user_name=username,))
#     if get_caller_response.status != 1:
#         return Response(status=get_caller_response.status, message=get_caller_response.msg)
#     user_id = get_caller_response.data[0].id
#     # Calling the GK
#     # Load the file in bytes
#     data_in_bytes = bytes(await data.read())
#     gatekeeper_response = gatekeeper.broker_access(user_id, "Predict", exe_mode, data_in_bytes)
#     print(gatekeeper_response)
#
#     # Case one: output contains info from unauthorized datasets
#     if not gatekeeper_response["status"]:
#         return {"status": "Not enough permissions"}
#         # Depends on the execution mode, we may still have a result that needs to be put into staging storage.
#     # Case two: status == True, can return directly to users
#     else:
#         # In here we need to know the output format of the data: a tuple with np arrays
#         data_output = gatekeeper_response["data"].tolist()
#         json_data_output = json.dumps(data_output)
#         return {"data": json_data_output}
