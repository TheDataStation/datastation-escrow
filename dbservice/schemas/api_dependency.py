from pydantic import BaseModel

class APIDependencyCreate(BaseModel):
    from_api: str
    to_api: str

    class Config:
        orm_mode = True
