import grpc
from fastapi import FastAPI
import policyWithDependency
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

import database_pb2
import database_pb2_grpc

import userRegister
import dataRegister
import gatekeeper

# Adding global variables to support access token generation (for authentication)
SECRET_KEY = "736bf9552516f9fa304078c9022cea2400a6808f02c02cdcbd4882b94e2cb260"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Specifying oauth2_scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

database_service_channel = grpc.insecure_channel('localhost:50051')
database_service_stub = database_pb2_grpc.DatabaseStub(database_service_channel)

app = FastAPI()

@app.get('/')
async def root():
    return {"message": "Welcome to v1"}

# On startup: doing some initializations from DBS
@app.on_event("startup")
async def startup_event():
    policyWithDependency.initialize()

# The following API allows user log-in.
@app.post("/token/")
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    response = userRegister.LoginUser(form_data.username,
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
#     response = database_service_stub.CreateAPI(
#                 database_pb2.API(api_name=api.api_name))
#     policyWithDependency.add_new_api(response.data[0].api_name)
#     return Response(status=response.status, message=response.msg)

# Look at all available APIs
@app.get("/api/")
async def get_all_apis(token: str = Depends(oauth2_scheme)):

    # Perform authentication
    userRegister.authenticate_user(token)

    # Call policyWithDependency directly
    return policyWithDependency.get_all_apis()

# # Upload a new API Dependency
# @app.post("/api_depend/")
# async def upload_api_dependency(api_dependency: APIDependency):
#     response = database_service_stub.CreateAPIDependency(
#                 database_pb2.APIDependency(from_api=api_dependency.from_api,
#                                            to_api=api_dependency.to_api,))
#     if response.status != -1:
#         cur_tuple = (response.data[0].from_api, response.data[0].to_api)
#         policyWithDependency.add_new_api_depend(cur_tuple)
#     return Response(status=response.status, message=response.msg)

# Look at all available API dependencies
@app.get("/api_depend/")
async def get_all_api_dependencies(token: str = Depends(oauth2_scheme)):

    # Perform authentication
    userRegister.authenticate_user(token)

    # Call policyWithDependency directly
    return policyWithDependency.get_all_dependencies()

# Upload a new policy
@app.post("/policy/")
async def upload_policy(policy: Policy):
    response = database_service_stub.CreatePolicy(
                database_pb2.Policy(user_id=policy.user_id,
                                    api=policy.api,
                                    data_id=policy.data_id,))
    if response.status != -1:
        cur_tuple = (response.data[0].user_id,
                     response.data[0].api,
                     response.data[0].data_id)
        policyWithDependency.add_new_policy(cur_tuple)
    return Response(status=response.status, message=response.msg)

# Look at all available policies
@app.get("/policy/")
async def get_all_policies():
    return policyWithDependency.get_all_policies()

# Look at the dependency graph
@app.get("/dependency_graph/")
async def get_dependency_graph():
    return policyWithDependency.get_dependency_graph()

# Register a new user
@app.post("/users/")
async def create_user(user: User):
    response = userRegister.CreateUser(user)
    return Response(status=response.status, message=response.message)

# Upload a new dataset
@app.post("/dataset/")
async def upload_dataset(data_name: str,
                         data: UploadFile = File(...),):
    # Load the file in bytes
    data_in_bytes = bytes(await data.read())

    response = dataRegister.UploadData(data_name, data_in_bytes)
    if response.status != 0:
        return Response(status=response.status, message=response.message)

    return response

# Remove a dataset that's uploaded
@app.delete("/dataset/")
async def remove_dataset(data_name: str,):
    response = dataRegister.RemoveData(data_name)
    return Response(status=response.status, message=response.message)

# API for Data Users: Preprocess
@app.get("/simpleML/datapreprocess/")
async def data_process_wrapper(token: str = Depends(oauth2_scheme)):
    # Get caller uname
    username = userRegister.authenticate_user(token)
    # Get caller ID
    get_user_response = database_service_stub.GetUserByUserName(database_pb2.User(user_name=username))
    if get_user_response.status != 1:
        return Response(status=get_user_response.status, message=get_user_response.msg)
    user_id = get_user_response.data[0].id
    # Calling the GK
    gatekeeper_response = gatekeeper.brokerAccess_opt(user_id, "Preprocess")
    # print(gatekeeper_response)
    # Case one: output contains info from unauthorized datasets
    if not gatekeeper_response["status"]:
        return {"status": "Not enough permissions"}
    # Case two: status == True, can return directly to users
    else:
        # In here we need to know the output format of the data
        data_output = []
        for ele in gatekeeper_response["data"]:
            data_output.append(ele.tolist())
        json_data_output = json.dumps(data_output)
        return {"data": json_data_output}
