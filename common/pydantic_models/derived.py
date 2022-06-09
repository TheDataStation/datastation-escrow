from pydantic import BaseModel

class Derived(BaseModel):
    id: int
    caller_id: int
    api: str

    class Config:
        orm_mode = True

class DerivedCreate(BaseModel):
    id: int
    caller_id: int  # pk user.id
    api: str  # pk apis.api_name

    class Config:
        orm_mode = True
