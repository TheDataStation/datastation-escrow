from pydantic import BaseModel
from typing import Optional


class User(BaseModel):
    user_name: str
    password: Optional[str]

