from pydantic import BaseModel

# A general response type
class Response(BaseModel):
    status: int
    message: str
