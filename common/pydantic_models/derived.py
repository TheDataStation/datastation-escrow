from pydantic import BaseModel


class Derived(BaseModel):
    id: int
    caller_id: int
    api: str
