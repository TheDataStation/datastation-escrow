from pydantic import BaseModel
from typing import List, Optional

class Response(BaseModel):
    status: int
    msg: str

class User(BaseModel):
    id: int
    user_name: str
    password: Optional[str]

    class Config:
        orm_mode = True

class Dataset(BaseModel):
    id: int
    name: str
    description: Optional[str]
    upload: Optional[bool]
    url: Optional[str]
    owner_id: Optional[int]

    class Config:
        orm_mode = True

class API(BaseModel):
    api_name: str

    class Config:
        orm_mode = True

class APIDependency(BaseModel):
    from_api: str
    to_api: str

    class Config:
        orm_mode = True

class Policy(BaseModel):
    user_id: int
    api: str
    data_id: int

    class Config:
        orm_mode = True

class UserResponse(Response):
    data: List[User]

class DatasetResponse(Response):
    data: List[Dataset]

class APIResponse(Response):
    data: List[API]

class APIDependencyResponse(Response):
    data: List[APIDependency]

class PolicyResponse(Response):
    data: List[Policy]
