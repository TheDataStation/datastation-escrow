from pydantic import BaseModel


class ProvenanceCreate(BaseModel):
    child_id: int
    parent_id: int

    class Config:
        orm_mode = True
