from pydantic import BaseModel

class Share(BaseModel):
    id: int
    template: str
    param: str

    class Config:
        orm_mode = True
