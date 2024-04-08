import bcrypt
from typing import Optional
from datetime import datetime, timedelta
from jose import jwt, JWTError
from fastapi import HTTPException, status
from dbservice import database_api

# Adding global variables to support access token generation (for authentication)
SECRET_KEY = "736bf9552516f9fa304078c9022cea2400a6808f02c02cdcbd4882b94e2cb260"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 365


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


def create_agent(user_id,
                 user_name,
                 password,
                 write_ahead_log,
                 key_manager, ):
    # print(user_id)
    # check if there is an existing user
    user_resp = database_api.get_user_by_user_name(user_name)
    if user_resp["status"] == 0:
        return {"status": 1, "message": "username already exists"}

    # no existing username, create new user
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

    # no_trust mode: record ADD_USER to wal
    if write_ahead_log:
        wal_entry = f"database_api.create_user({user_id}, {user_name}, {hashed.decode()})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    user_resp = database_api.create_user(user_id, user_name, hashed.decode())
    if user_resp == 1:
        return user_resp
    return {"status": user_resp["status"], "message": user_resp["message"], "user_id": user_resp["data"].id}


def login_agent(username, password):
    # check if there is an existing user
    user_resp = database_api.get_user_by_user_name(username)
    if user_resp["status"] == 1:
        return user_resp
    user_data = user_resp["data"]
    if bcrypt.checkpw(password.encode(), user_data.password.encode()):
        # In here the password matches, we store the content for the token in the message
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"a_id": user_data.id}, expires_delta=access_token_expires
        )
        return {"status": 0, "message": "login success", "data": str(access_token)}
    return {"status": 1, "message": "password does not match"}


def authenticate_agent(token: str):
    # Credential Checking
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        a_id: str = payload.get("a_id")
        if a_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return a_id


def list_all_agents():
    database_service_response = database_api.get_all_users()
    res = []
    for agent in database_service_response["data"]:
        cur_agent_id_name = {"agent_id": agent.id, "agent_name": agent.user_name}
        res.append(cur_agent_id_name)
    return res
