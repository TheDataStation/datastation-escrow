from pydantic import BaseModel
from common.pydantic_models.share import Share
from typing import List

class Response(BaseModel):
    status: int
    msg: str

class ShareResponse(Response):
    data: List[Share]
