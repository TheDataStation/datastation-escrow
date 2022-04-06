from pydantic import BaseModel

class DerivedCreate(BaseModel):
    id: int
    caller_id: int  # pk user.id
    api: str  # pk apis.api_name

    class Config:
        orm_mode = True
