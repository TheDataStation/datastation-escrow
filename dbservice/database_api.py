from .database import engine
from .database import Base, DATABASE_URL
from sqlalchemy import create_engine, exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event
from sqlalchemy.pool import NullPool

from .crud import user_repo, dataset_repo, api_repo, api_dependency_repo, policy_repo, derived_repo, provenance_repo
from .responses import response
from contextlib import contextmanager

# global engine

def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute('pragma foreign_keys=ON')

@contextmanager
def get_db():
    # db = SessionLocal()
    # DATABASE_URL = "postgresql://zhiruzhu:@localhost:5432/data_station"

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    event.listen(engine, 'connect', _fk_pragma_on_connect)

    # Base = declarative_base()
    Base.metadata.create_all(engine)

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_user(request):
    with get_db() as session:
        user = user_repo.create_user(session, request)
        if user:
            return response.UserResponse(status=1, msg="success", data=[user])
        else:
            return response.UserResponse(status=-1, msg="internal database error", data=[])

def get_user_by_user_name(request):
    with get_db() as session:
        user = user_repo.get_user_by_user_name(session, request.user_name)
        if user:
            return response.UserResponse(status=1, msg="success", data=[user])
        else:
            return response.UserResponse(status=-1, msg="internal database error", data=[])

def get_user_with_max_id():
    with get_db() as session:
        user = user_repo.get_user_with_max_id(session)
        if user:
            return response.UserResponse(status=1, msg="success", data=[user])
        else:
            return response.UserResponse(status=-1, msg="internal database error", data=[])

def get_all_users():
    with get_db() as session:
        users = user_repo.get_all_users(session)
        if len(users):
            return response.UserResponse(status=1, msg="success", data=users)
        else:
            return response.UserResponse(status=-1, msg="no existing users", data=[])

def create_dataset(request):
    with get_db() as session:
        dataset = dataset_repo.create_dataset(session, request)
        if dataset:
            return response.DatasetResponse(status=1, msg="success", data=[dataset])
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def get_dataset_by_name(request):
    with get_db() as session:
        dataset = dataset_repo.get_dataset_by_name(session, request.name)
        if dataset:
            return response.DatasetResponse(status=1, msg="success", data=[dataset])
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def get_dataset_by_id(request):
    with get_db() as session:
        dataset = dataset_repo.get_dataset_by_id(session, request)
        if dataset:
            return response.DatasetResponse(status=1, msg="success", data=[dataset])
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def get_dataset_by_access_type(request):
    with get_db() as session:
        dataset = dataset_repo.get_dataset_by_access_type(session, request)
        if dataset:
            return response.DatasetResponse(status=1, msg="success", data=[dataset])
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def remove_dataset_by_name(request):
    with get_db() as session:
        res = dataset_repo.remove_dataset_by_name(session, request.name)
        if res == "success":
            return response.DatasetResponse(status=1, msg="success", data=[])
        else:
            return response.DatasetResponse(status=-1, msg="fail", data=[])

def get_dataset_owner(request):
    with get_db() as session:
        owner = dataset_repo.get_dataset_owner(session, request.id)
        if owner:
            return response.UserResponse(status=1, msg="success", data=[owner])
        else:
            return response.UserResponse(status=-1, msg="fail", data=[])

def get_all_datasets():
    with get_db() as session:
        datasets = dataset_repo.get_all_datasets(session)
        if len(datasets):
            return response.DatasetResponse(status=1, msg="success", data=datasets)
        else:
            return response.DatasetResponse(status=-1, msg="no existing datasets", data=[])

def get_all_optimistic_datasets():
    with get_db() as session:
        datasets = dataset_repo.get_all_optimistic_datasets(session)
        if len(datasets):
            return response.DatasetResponse(status=1, msg="success", data=datasets)
        else:
            return response.DatasetResponse(status=-1, msg="no optimistic datasets", data=[])

def get_data_with_max_id():
    with get_db() as session:
        dataset = dataset_repo.get_data_with_max_id(session)
        if dataset:
            return response.DatasetResponse(status=1, msg="success", data=[dataset])
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def create_api(request):
    with get_db() as session:
        api = api_repo.create_api(session, request)
        if api:
            return response.APIResponse(status=1, msg="success", data=[api])
        else:
            return response.APIResponse(status=-1, msg="fail", data=[])

def get_all_apis():
    with get_db() as session:
        apis = api_repo.get_all_apis(session)
        if len(apis):
            return response.GetAPIResponse(status=1, msg="success", data=apis)
        else:
            return response.GetAPIResponse(status=-1, msg="no existing apis", data=[])

def create_api_dependency(request):
    with get_db() as session:
        api_dependency = api_dependency_repo.create_api_dependency(session, request)
        if api_dependency:
            return response.APIDependencyResponse(status=1, msg="success", data=[api_dependency])
        else:
            return response.APIDependencyResponse(status=-1, msg="fail", data=[])

def get_all_api_dependencies():
    with get_db() as session:
        api_dependencies = api_dependency_repo.get_all_dependencies(session)
        if len(api_dependencies):
            return response.APIDependencyResponse(status=1, msg="success", data=api_dependencies)
        else:
            return response.APIDependencyResponse(status=-1, msg="no existing dependencies", data=[])

def create_policy(request):
    with get_db() as session:
        policy = policy_repo.create_policy(session, request)
        if policy:
            return response.PolicyResponse(status=1, msg="success", data=[policy])
        else:
            return response.PolicyResponse(status=-1, msg="fail", data=[])

def remove_policy(request):
    with get_db() as session:
        res = policy_repo.remove_policy(session, request)
        if res == "success":
            return response.PolicyResponse(status=1, msg="success", data=[])
        else:
            return response.PolicyResponse(status=-1, msg="fail", data=[])

def get_all_policies():
    with get_db() as session:
        policies = policy_repo.get_all_policies(session)
        if len(policies):
            return response.PolicyResponse(status=1, msg="success", data=policies)
        else:
            return response.PolicyResponse(status=-1, msg="no existing policies", data=[])

def get_policy_for_user(user_id):
    with get_db() as session:
        policies = policy_repo.get_policy_for_user(session, user_id)
        if len(policies):
            return response.PolicyResponse(status=1, msg="success", data=policies)
        else:
            return response.PolicyResponse(status=-1, msg="no existing policies", data=[])

def create_derived(request):
    with get_db() as session:
        derived = derived_repo.create_derived(session, request)
        if derived:
            return response.DerivedResponse(status=1, msg="success", data=[derived])
        else:
            return response.DerivedResponse(status=-1, msg="fail", data=[])

def create_provenance(request):
    with get_db() as session:
        provenance = provenance_repo.create_provenance(session, request)
        if provenance:
            return response.ProvenanceResponse(status=1, msg="success", data=[provenance])
        else:
            return response.ProvenanceResponse(status=-1, msg="fail", data=[])
