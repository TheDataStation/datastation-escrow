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


def get_all_users():
    users = user_repo.get_all_users(next(get_db()))
    if len(users):
        return response.UserResponse(status=1, msg="success", data=users)
    else:
        return response.UserResponse(status=-1, msg="no existing users", data=[])

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

def get_dataset_by_id(request):
    dataset = dataset_repo.get_dataset_by_id(next(get_db()), request)
    if dataset:
        return response.DatasetResponse(status=1, msg="success", data=[dataset])
    else:
        return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def get_dataset_by_access_type(request):
    dataset = dataset_repo.get_dataset_by_access_type(next(get_db()), request)
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

def get_dataset_owner(request):
    owner = dataset_repo.get_dataset_owner(next(get_db()), request.id)
    if owner:
        return response.UserResponse(status=1, msg="success", data=[owner])
    else:
        return response.UserResponse(status=-1, msg="fail", data=[])

def get_all_datasets():
    datasets = dataset_repo.get_all_datasets(next(get_db()))
    if len(datasets):
        return response.DatasetResponse(status=1, msg="success", data=datasets)
    else:
        return response.DatasetResponse(status=-1, msg="no existing datasets", data=[])

def get_all_optimistic_datasets():
    datasets = dataset_repo.get_all_optimistic_datasets(next(get_db()))
    if len(datasets):
        return response.DatasetResponse(status=1, msg="success", data=datasets)
    else:
        return response.DatasetResponse(status=-1, msg="no optimistic datasets", data=[])

def create_api(request):
    api = api_repo.create_api(next(get_db()), request)
    if api:
        return response.APIResponse(status=1, msg="success", data=[api])
    else:
        return response.APIResponse(status=-1, msg="fail", data=[])

def get_all_apis():
    apis = api_repo.get_all_apis(next(get_db()))
    if len(apis):
        return response.GetAPIResponse(status=1, msg="success", data=apis)
    else:
        return response.GetAPIResponse(status=-1, msg="no existing apis", data=[])

def create_api_dependency(request):
    api_dependency = api_dependency_repo.create_api_dependency(next(get_db()), request)
    if api_dependency:
        return response.APIDependencyResponse(status=1, msg="success", data=[api_dependency])
    else:
        return response.APIDependencyResponse(status=-1, msg="fail", data=[])

def get_all_api_dependencies():
    api_dependencies = api_dependency_repo.get_all_dependencies(next(get_db()))
    if len(api_dependencies):
        return response.APIDependencyResponse(status=1, msg="success", data=api_dependencies)
    else:
        return response.APIDependencyResponse(status=-1, msg="no existing dependencies", data=[])

def create_policy(request):
    policy = policy_repo.create_policy(next(get_db()), request)
    if policy:
        return response.PolicyResponse(status=1, msg="success", data=[policy])
    else:
        return response.PolicyResponse(status=-1, msg="fail", data=[])

def remove_policy(request):
    res = policy_repo.remove_policy(next(get_db()), request)
    if res == "success":
        return response.PolicyResponse(status=1, msg="success", data=[])
    else:
        return response.PolicyResponse(status=-1, msg="fail", data=[])

def get_all_policies():
    policies = policy_repo.get_all_policies(next(get_db()))
    if len(policies):
        return response.PolicyResponse(status=1, msg="success", data=policies)
    else:
        return response.PolicyResponse(status=-1, msg="no existing policies", data=[])

def get_policy_for_user(user_id):
    policies = policy_repo.get_policy_for_user(next(get_db()), user_id)
    if len(policies):
        return response.PolicyResponse(status=1, msg="success", data=policies)
    else:
        return response.PolicyResponse(status=-1, msg="no existing policies", data=[])

def create_derived(request):
    derived = derived_repo.create_derived(next(get_db()), request)
    if derived:
        return response.DerivedResponse(status=1, msg="success", data=[derived])
    else:
        return response.DerivedResponse(status=-1, msg="fail", data=[])

def create_provenance(request):
    provenance = provenance_repo.create_provenance(next(get_db()), request)
    if provenance:
        return response.ProvenanceResponse(status=1, msg="success", data=[provenance])
    else:
        return response.ProvenanceResponse(status=-1, msg="fail", data=[])
