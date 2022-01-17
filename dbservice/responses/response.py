from pydantic import BaseModel
from typing import List

class User(BaseModel):
    id: int
    user_name: str
    password: str

    class Config:
        orm_mode = True

class Response(BaseModel):
    status: int
    msg: str

class UserResponse(Response):
    data: List[User]
