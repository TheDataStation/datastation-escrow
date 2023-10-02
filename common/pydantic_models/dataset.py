from pydantic import BaseModel
from typing import Optional

class Dataset(BaseModel):
    id: Optional[int]
    name: Optional[str]
    type: Optional[str]
    description: Optional[str]
    access_param: Optional[str]
    owner_id: Optional[int]
    optimistic: Optional[bool]

    class Config:
        orm_mode = True
