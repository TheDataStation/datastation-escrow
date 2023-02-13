from pydantic import BaseModel

class Share(BaseModel):
    id: int

    class Config:
        orm_mode = True

class ShareCreate(BaseModel):
    id: int

    class Config:
        orm_mode = True
