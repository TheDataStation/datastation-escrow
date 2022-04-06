from pydantic import BaseModel


class DatasetCreate(BaseModel):
    id: int
    owner_id: int  # pk user.id
    name: str
    description: str
    type: str
    access_type: str
    optimistic: bool
    original_data_size: int
