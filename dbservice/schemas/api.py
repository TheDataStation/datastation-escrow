from pydantic import BaseModel

class APICreate(BaseModel):
    api_name: str

    class Config:
        orm_mode = True

