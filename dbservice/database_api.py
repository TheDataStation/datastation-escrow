from .database import SessionLocal, engine, Base

from .crud import user_repo, dataset_repo, api_repo, api_dependency_repo, policy_repo, derived_repo, provenance_repo
from .responses import response

Base.metadata.create_all(engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_user(request):
    user = user_repo.create_user(next(get_db()), request)
    if user:
        return response.UserResponse(status=1, msg="success", data=[user])
    else:
        return response.UserResponse(status=-1, msg="internal database error", data=[])

def get_user_by_user_name(request):
    user = user_repo.get_user_by_user_name(next(get_db()), request.user_name)
    if user:
        return response.UserResponse(status=1, msg="success", data=[user])
    else:
        return response.UserResponse(status=-1, msg="internal database error", data=[])

def create_dataset(request):
    dataset = dataset_repo.create_dataset(next(get_db()), request)
    if dataset:
        return response.DatasetResponse(status=1, msg="success", data=[dataset])
    else:
        return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def get_dataset_by_name(request):
    dataset = dataset_repo.get_dataset_by_name(next(get_db()), request.name)
    if dataset:
        return response.DatasetResponse(status=1, msg="success", data=[dataset])
    else:
        return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def remove_dataset_by_name(request):
    res = dataset_repo.remove_dataset_by_name(next(get_db()), request.name)
    if res == "success":
        return response.DatasetResponse(status=1, msg="success", data=[])
    else:
        return response.DatasetResponse(status=-1, msg="fail", data=[])
