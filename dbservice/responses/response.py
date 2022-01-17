from pydantic import BaseModel
from typing import List, Optional

class Response(BaseModel):
    status: int
    msg: str

class User(BaseModel):
    id: int
    user_name: str
    password: str

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

class UserResponse(Response):
    data: List[User]

class DatasetResponse(Response):
    data: List[Dataset]
