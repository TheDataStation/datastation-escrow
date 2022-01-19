from fastapi import FastAPI
import policy_broker
from models.api import *
from models.api_dependency import *
from models.response import *
from models.policy import *
from models.user import *
from models.dataset import *
from fastapi import UploadFile, File, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
import json
from dbservice import database_api

import user_register
import data_register
import gatekeeper

# Adding global variables to support access token generation (for authentication)
SECRET_KEY = "736bf9552516f9fa304078c9022cea2400a6808f02c02cdcbd4882b94e2cb260"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Specifying oauth2_scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

@app.get('/')
async def root():
    return {"message": "Welcome to v1"}

# On startup: doing some initializations from DBS
@app.on_event("startup")
async def startup_event():
    pass

# The following API allows user log-in.
@app.post("/token/")
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    response = user_register.login_user(form_data.username,
                                        form_data.password)
    if response.status == 0:
        return {"access_token": response.token, "token_type": "bearer"}
    else:
        # if password cannot correctly be verified, we raise an exception to indicate login has failed
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

# # Upload a new API
# @app.post("/api/")
# async def upload_api(api: API):
#     response = database_api.create_api(api)
#     if response.status == 1:
#         policy_broker.add_new_api(response.data[0].api_name)
#     return Response(status=response.status, message=response.msg)

# Look at all available APIs
@app.get("/api/")
async def get_all_apis(token: str = Depends(oauth2_scheme)):

    # Perform authentication
    user_register.authenticate_user(token)

    # Call policy_broker directly
    return policy_broker.get_all_apis()

# # Upload a new API Dependency
# @app.post("/api_depend/")
# async def upload_api_dependency(api_dependency: APIDependency):
#     response = database_api.create_api_dependency(api_dependency)
#     if response.status != -1:
#         cur_tuple = (response.data[0].from_api, response.data[0].to_api)
#         policy_broker.add_new_api_depend(cur_tuple)
#     return Response(status=response.status, message=response.msg)

# Look at all available API dependencies
@app.get("/api_depend/")
async def get_all_api_dependencies(token: str = Depends(oauth2_scheme)):

    # Perform authentication
    user_register.authenticate_user(token)

    # Call policy_broker directly
    return policy_broker.get_all_dependencies()

# Upload a new policy
@app.post("/policy/")
async def upload_policy(policy: Policy):
    response = database_api.create_policy(policy)
    return Response(status=response.status, message=response.msg)

# Look at all available policies
@app.get("/policy/")
async def get_all_policies():
    return policy_broker.get_all_policies()

# Register a new user
@app.post("/users/")
async def create_user(user: User):
    response = user_register.create_user(user)
    return Response(status=response.status, message=response.message)

# Upload a new dataset
@app.post("/dataset/")
async def upload_dataset(data_name: str,
                         data: UploadFile = File(...),):
    # Load the file in bytes
    data_in_bytes = bytes(await data.read())

    response = data_register.upload_data(data_name, data_in_bytes)
    if response.status != 0:
        return Response(status=response.status, message=response.message)

    return response

# Remove a dataset that's uploaded
@app.delete("/dataset/")
async def remove_dataset(data_name: str,):
    response = data_register.remove_data(data_name)
    return Response(status=response.status, message=response.message)

# API for Data Users: Preprocess
@app.get("/simpleML/datapreprocess/")
async def data_process_wrapper(token: str = Depends(oauth2_scheme)):
    # First set an execution mode
    exe_mode = "optimistic"
    # Get caller uname
    username = user_register.authenticate_user(token)
    # Get caller ID
    get_caller_response = database_api.get_user_by_user_name(User(user_name=username,))
    if get_caller_response.status != 1:
        return Response(status=get_caller_response.status, message=get_caller_response.msg)
    user_id = get_caller_response.data[0].id
    # Calling the GK
    gatekeeper_response = gatekeeper.broker_access(user_id, "Preprocess", exe_mode)
    # print(gatekeeper_response)
    # Case one: output contains info from unauthorized datasets
    if not gatekeeper_response["status"]:
        return {"status": "Not enough permissions"}
        # Depends on the execution mode, we may still have a result that needs to be put into staging storage.
    # Case two: status == True, can return directly to users
    else:
        # In here we need to know the output format of the data: a tuple with np arrays
        data_output = []
        for ele in gatekeeper_response["data"]:
            data_output.append(ele.tolist())
        json_data_output = json.dumps(data_output)
        return {"data": json_data_output}

# API for Data Users: ModelTrain
@app.get("/simpleML/modeltrain/")
async def model_train_wrapper(token: str = Depends(oauth2_scheme)):
    # First set an execution mode
    exe_mode = "optimistic"
    # Get caller uname
    username = user_register.authenticate_user(token)
    # Get caller ID
    get_caller_response = database_api.get_user_by_user_name(User(user_name=username,))
    if get_caller_response.status != 1:
        return Response(status=get_caller_response.status, message=get_caller_response.msg)
    user_id = get_caller_response.data[0].id
    # Calling the GK
    gatekeeper_response = gatekeeper.broker_access(user_id, "ModelTrain", exe_mode)
    print(gatekeeper_response["data"].coef_)

    # Case one: output contains info from unauthorized datasets
    if not gatekeeper_response["status"]:
        return {"status": "Not enough permissions"}
        # Depends on the execution mode, we may still have a result that needs to be put into staging storage.
    # Case two: status == True, can return directly to users
    else:
        # In here we need to know the output format of the data: a tuple with np arrays
        data_output = gatekeeper_response["data"].coef_.tolist()
        json_data_output = json.dumps(data_output)
        return {"data": json_data_output}

# API for Data Users: Predict
@app.post("/simpleML/predict/")
async def predict_wrapper(token: str = Depends(oauth2_scheme),
                          data: UploadFile = File(...),):
    # First set an execution mode
    exe_mode = "pessimistic"
    # Get caller uname
    username = user_register.authenticate_user(token)
    # Get caller ID
    get_caller_response = database_api.get_user_by_user_name(User(user_name=username,))
    if get_caller_response.status != 1:
        return Response(status=get_caller_response.status, message=get_caller_response.msg)
    user_id = get_caller_response.data[0].id
    # Calling the GK
    # Load the file in bytes
    data_in_bytes = bytes(await data.read())
    gatekeeper_response = gatekeeper.broker_access(user_id, "Predict", exe_mode, data_in_bytes)
    print(gatekeeper_response)

    # Case one: output contains info from unauthorized datasets
    if not gatekeeper_response["status"]:
        return {"status": "Not enough permissions"}
        # Depends on the execution mode, we may still have a result that needs to be put into staging storage.
    # Case two: status == True, can return directly to users
    else:
        # In here we need to know the output format of the data: a tuple with np arrays
        data_output = gatekeeper_response["data"].tolist()
        json_data_output = json.dumps(data_output)
        return {"data": json_data_output}
