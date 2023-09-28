from pydantic import BaseModel

class Function(BaseModel):
    function_name: str

    class Config:
        orm_mode = True

class FunctionCreate(BaseModel):
    function_name: str

    class Config:
        orm_mode = True
