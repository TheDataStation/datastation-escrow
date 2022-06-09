from pydantic import BaseModel

class API(BaseModel):
    api_name: str

    class Config:
        orm_mode = True

class APICreate(BaseModel):
    api_name: str

    class Config:
        orm_mode = True
