from pydantic import BaseModel


class PolicyCreate(BaseModel):
    user_id: int
    api: str
    data_id: int

    class Config:
        orm_mode = True
