from pydantic import BaseModel


class Policy(BaseModel):
    user_id: int
    api: str
    data_id: int

    class Config:
        orm_mode = True


class PolicyCreate(BaseModel):
    user_id: int
    api: str
    data_id: int

    class Config:
        orm_mode = True
