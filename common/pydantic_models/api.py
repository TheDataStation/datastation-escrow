from pydantic import BaseModel


class API(BaseModel):
    api_name: str
