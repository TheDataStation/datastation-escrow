import grpc
import database_pb2
import database_pb2_grpc
from models.response import *
import bcrypt
from typing import Optional
from datetime import datetime, timedelta
from jose import jwt, JWTError
from fastapi import HTTPException, status

# Adding global variables to support access token generation (for authentication)
SECRET_KEY = "736bf9552516f9fa304078c9022cea2400a6808f02c02cdcbd4882b94e2cb260"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

database_service_channel = grpc.insecure_channel('localhost:50051')
database_service_stub = database_pb2_grpc.DatabaseStub(database_service_channel)

# The following function handles the creation of access tokens (for LoginUser)
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def CreateUser(request):
    # check if there is an existing user
    existed_user = database_service_stub.GetUserByUserName(database_pb2.User(user_name=request.user_name))
    if existed_user.status == 1:
        return Response(status=1, message="username already exists")
    # no existing username, create new user
    hashed = bcrypt.hashpw(request.password.encode(), bcrypt.gensalt())
    new_user = database_pb2.User(user_name=request.user_name,
                                 password=hashed.decode(),)
    resp = database_service_stub.CreateUser(new_user)
    if resp.status == -1:
        return Response(status=1, message="internal database error")

    return Response(status=0, message="success")

def LoginUser(username, password):
    # check if there is an existing user
    existed_user = database_service_stub.GetUserByUserName(database_pb2.User(user_name=username))
    # If the user doesn't exist, something is wrong
    if existed_user == -1:
        return TokenResponse(status=1, token="username is wrong")
    user_data = existed_user.data[0]
    # user = get_first_user(existed_user.data, password=True)
    if bcrypt.checkpw(password.encode(), user_data.password.encode()):
        # In here the password matches, we store the content for the token in the message
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": user_data.user_name}, expires_delta=access_token_expires
        )
        return TokenResponse(status=0, token=str(access_token))
    return TokenResponse(status=1, token="password does not match")

def authenticate_user(token):
    # Credential Checking
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username
