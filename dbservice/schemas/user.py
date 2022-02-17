from pydantic import BaseModel
from typing import List

from .dataset import Dataset


class User(BaseModel):
    id: int
    user_name: str
    datasets: List[Dataset] = []

    class Config:
        orm_mode = True


class UserRegister(BaseModel):
    id: int
    user_name: str
    password: str


class UserLogin(BaseModel):
    user_name: str
    password: str
