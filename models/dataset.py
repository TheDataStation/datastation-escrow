from pydantic import BaseModel


class Dataset(BaseModel):
    data_name: str
    data: bytes
