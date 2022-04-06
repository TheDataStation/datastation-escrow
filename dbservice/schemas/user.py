from pydantic import BaseModel


class UserRegister(BaseModel):
    id: int
    user_name: str
    password: str
