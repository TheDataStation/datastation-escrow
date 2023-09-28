from pydantic import BaseModel
from common.pydantic_models.user import User
from common.pydantic_models.dataset import Dataset
from common.pydantic_models.function import Function
from common.pydantic_models.function_dependency import FunctionDependency
from common.pydantic_models.policy import Policy
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

class FunctionResponse(Response):
    data: List[Function]

class GetFunctionResponse(Response):
    data: List[str]

class FunctionDependencyResponse(Response):
    data: List[FunctionDependency]

class PolicyResponse(Response):
    data: List[Policy]

class ProvenanceResponse(Response):
    data: List[Provenance]

class ShareResponse(Response):
    data: List[Share]
