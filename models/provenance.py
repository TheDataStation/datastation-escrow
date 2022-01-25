from pydantic import BaseModel


class Provenance(BaseModel):
    child_id: int
    parent_id: int
