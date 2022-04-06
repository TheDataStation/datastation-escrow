from pydantic import BaseModel


class APIDependency(BaseModel):
    from_api: str
    to_api: str
