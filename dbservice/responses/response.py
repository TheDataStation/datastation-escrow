from pydantic import BaseModel
from common.pydantic_models.user import User
from common.pydantic_models.dataset import Dataset
from common.pydantic_models.api import API
from common.pydantic_models.api_dependency import APIDependency
from common.pydantic_models.policy import Policy
from common.pydantic_models.staged import Staged
from common.pydantic_models.provenance import Provenance
from common.pydantic_models.share import Share
from typing import List

class Response(BaseModel):
    status: int
    msg: str

class UserResponse(Response):
    data: List[User]

class DatasetResponse(Response):
    data: List[Dataset]

class APIResponse(Response):
    data: List[API]

class GetAPIResponse(Response):
    data: List[str]

class APIDependencyResponse(Response):
    data: List[APIDependency]

class PolicyResponse(Response):
    data: List[Policy]

class StagedResponse(Response):
    data: List[Staged]

class ProvenanceResponse(Response):
    data: List[Provenance]

class ShareResponse(Response):
    data: List[Share]
