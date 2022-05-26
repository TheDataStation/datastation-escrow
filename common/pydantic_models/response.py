from pydantic import BaseModel

# A general response type
from typing import List


class Response(BaseModel):
    status: int
    message: str

class CreateUserResponse(Response):
    user_id: int

class UploadUserResponse(Response):
    user_id: int

class UploadDataResponse(Response):
    data_id: int

class RemoveDataResponse(Response):
    data_id: int
    type: str

class StoreDataResponse(Response):
    access_type: str

class RetrieveDataResponse(Response):
    data: bytes

class GetAPIResponse(Response):
    data: List[str]

# Response type for user login
class TokenResponse(BaseModel):
    status: int
    token: str
