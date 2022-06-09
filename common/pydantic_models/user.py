from pydantic import BaseModel
from typing import Optional

class User(BaseModel):
    id: Optional[int]
    user_name: Optional[str]
    password: Optional[str]

    class Config:
        orm_mode = True

class UserRegister(BaseModel):
    id: int
    user_name: str
    password: str

    class Config:
        orm_mode = True

