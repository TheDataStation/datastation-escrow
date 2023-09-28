from pydantic import BaseModel

class FunctionDependency(BaseModel):
    from_f: str
    to_f: str

    class Config:
        orm_mode = True

class FunctionDependencyCreate(BaseModel):
    from_f: str
    to_f: str

    class Config:
        orm_mode = True
