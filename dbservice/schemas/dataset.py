from pydantic import BaseModel


class Dataset(BaseModel):
    id: int
    owner_id: int  # pk user.id
    name: str
    description: str
    type: str
    access_type: str

    class Config:
        orm_mode = True


class DatasetCreate(BaseModel):
    id: int
    owner_id: int  # pk user.id
    name: str
    description: str
    type: str
    access_type: str
