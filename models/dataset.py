from pydantic import BaseModel
from typing import Optional


class Dataset(BaseModel):
    id: Optional[int]
    name: Optional[str]
    description: Optional[str]
    upload: Optional[bool]
    url: Optional[str]
    owner_id: Optional[int]
