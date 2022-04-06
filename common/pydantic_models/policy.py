from pydantic import BaseModel


class Policy(BaseModel):
    user_id: int
    api: str
    data_id: int
