from pydantic import BaseModel

# A general response type
class Response(BaseModel):
    status: int
    message: str

class UploadDataResponse(Response):
    data_id: int

class GetAPIResponse(Response):
    data: list[str]

# Response type for user login
class TokenResponse(BaseModel):
    status: int
    token: str

