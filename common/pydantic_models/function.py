from pydantic import BaseModel

class Function(BaseModel):
    function_name: str

    class Config:
        orm_mode = True
