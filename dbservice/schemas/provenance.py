from pydantic import BaseModel


class Provenance(BaseModel):
    child_id: int
    parent_id: int

    class Config:
        orm_mode = True


class ProvenanceCreate(BaseModel):
    child_id: int
    parent_id: int
