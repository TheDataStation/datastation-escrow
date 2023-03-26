from pydantic import BaseModel
from typing import Any

# A general response type
from typing import List


class Response(BaseModel):
    status: int
    message: str

# Response type for API execution
class APIExecResponse(Response):
    result: Any

class UploadUserResponse(Response):
    user_id: int

class UploadDataResponse(Response):
    de_id: int

class UploadShareResponse(Response):
    share_id: int

class RemoveDataResponse(Response):
    data_id: int
    type: str

class RetrieveDataResponse(Response):
    data: bytes

class GetAPIResponse(Response):
    data: List[str]

# Response type for user login
class TokenResponse(BaseModel):
    status: int
    token: str
