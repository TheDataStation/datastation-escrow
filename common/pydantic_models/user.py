from pydantic import BaseModel
from typing import Optional


class User(BaseModel):
    id: Optional[int]
    user_name: Optional[str]
    password: Optional[str]

