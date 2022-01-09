from pydantic import BaseModel


class Response(BaseModel):
    status: int
    message: str

class UploadDataResponse(Response):
    data_id: int

class GetAPIResponse(Response):
    data: list[str]

class TokenResponse(BaseModel):
    status: int
    token: str
