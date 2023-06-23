from .database import engine, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event

from .crud import (user_repo,
                   dataset_repo,
                   api_repo,
                   api_dependency_repo,
                   policy_repo,
                   staged_repo,
                   provenance_repo,
                   share_repo, )
from .responses import response
from contextlib import contextmanager
from dbservice.checkpoint.check_point import check_point

# global engine

def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute('pragma foreign_keys=ON')

@contextmanager
def get_db():

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

def recover_users(users):
    with get_db() as session:
        res = user_repo.recover_users(session, users)
        if res is not None:
            return 0

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

def get_data_by_id(data_id: int):
    with get_db() as session:
        dataset = dataset_repo.get_data_by_id(session, data_id)
        if dataset:
            return response.DatasetResponse(status=1, msg="success", data=[dataset])
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def get_dataset_by_access_type(request):
    with get_db() as session:
        dataset = dataset_repo.get_dataset_by_access_param(session, request)
        if dataset:
            return response.DatasetResponse(status=1, msg="success", data=[dataset])
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def list_discoverable_des():
    with get_db() as session:
        discoverable_des = dataset_repo.list_discoverable_des(session)
        return discoverable_des

def get_datasets_by_paths(request):
    with get_db() as session:
        datasets = dataset_repo.get_datasets_by_paths(session, request)
        if len(datasets) > 0:
            return response.DatasetResponse(status=1, msg="success", data=datasets)
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def get_datasets_by_ids(request):
    with get_db() as session:
        datasets = dataset_repo.get_datasets_by_ids(session, request)
        if len(datasets) > 0:
            return response.DatasetResponse(status=1, msg="success", data=datasets)
        else:
            return response.DatasetResponse(status=-1, msg="internal database error", data=[])

def remove_dataset_by_name(request):
    with get_db() as session:
        res = dataset_repo.remove_dataset_by_name(session, request.name)
        if res == "success":
            return response.DatasetResponse(status=1, msg="success", data=[])
        else:
            return response.DatasetResponse(status=-1, msg="fail", data=[])

def get_de_owner(request):
    with get_db() as session:
        return dataset_repo.get_de_owner(session, request)

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

def recover_datas(datas):
    with get_db() as session:
        res = dataset_repo.recover_datas(session, datas)
        if res is not None:
            return 0

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

def ack_data_in_share(data_id, share_id):
    with get_db() as session:
        res = policy_repo.ack_data_in_share(session, data_id, share_id)
        if res is not None:
            return 0

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

def get_ack_policy_user_share(user_id, share_id):
    with get_db() as session:
        policies = policy_repo.get_ack_policy_user_share(session, user_id, share_id)
        if len(policies):
            return response.PolicyResponse(status=1, msg="success", data=policies)
        else:
            return response.PolicyResponse(status=-1, msg="no existing policies", data=[])

def bulk_upload_policies(policies):
    with get_db() as session:
        res = policy_repo.bulk_upload_policies(session, policies)
        if res is not None:
            return 0

def create_staged(request):
    with get_db() as session:
        staged = staged_repo.create_staged(session, request)
        if staged:
            return response.StagedResponse(status=1, msg="success", data=[staged])
        else:
            return response.StagedResponse(status=-1, msg="fail", data=[])

def get_api_for_staged_id(staged_id):
    with get_db() as session:
        api_name = staged_repo.get_api_for_staged_id(session, staged_id)
        if api_name:
            return api_name.api

def get_all_staged():
    with get_db() as session:
        staged = staged_repo.get_all_staged(session)
        if len(staged):
            return response.StagedResponse(status=1, msg="success", data=staged)
        else:
            return response.StagedResponse(status=-1, msg="no existing staged DEs", data=[])

def get_staging_with_max_id():
    with get_db() as session:
        staged = staged_repo.get_staged_with_max_id(session)
        if staged:
            return response.StagedResponse(status=1, msg="success", data=[staged])
        else:
            return response.StagedResponse(status=-1, msg="internal database error", data=[])

def recover_staged(staged):
    with get_db() as session:
        res = staged_repo.recover_staged(session, staged)
        if res is not None:
            return 0

def create_provenance(request):
    with get_db() as session:
        provenance = provenance_repo.create_provenance(session, request)
        if provenance:
            return response.ProvenanceResponse(status=1, msg="success", data=[provenance])
        else:
            return response.ProvenanceResponse(status=-1, msg="fail", data=[])

def get_all_provenances():
    with get_db() as session:
        provenances = provenance_repo.get_all_provenances(session)
        if len(provenances):
            return response.ProvenanceResponse(status=1, msg="success", data=provenances)
        else:
            return response.ProvenanceResponse(status=-1, msg="no existing provenances", data=[])

def get_parent_id_for_staged_id(staged_id):
    with get_db() as session:
        parent_ids = provenance_repo.get_parent_id_for_staged_id(session, staged_id)
        if len(parent_ids):
            return parent_ids

def recover_provenance(provenances):
    with get_db() as session:
        res = provenance_repo.recover_provenance(session, provenances)
        if res is not None:
            return 0

def bulk_create_provenance(child_id, provenances):
    with get_db() as session:
        res = provenance_repo.bulk_create_provenance(session, child_id, provenances)
        if res is not None:
            return 0
        else:
            return 1

def create_share(share_id, share_template, share_param):
    with get_db() as session:
        share = share_repo.create_share(session, share_id, share_template, share_param)
        if share:
            return response.ShareResponse(status=1, msg="success", data=[share])
        else:
            return response.ShareResponse(status=-1, msg="fail", data=[])

def get_share_with_max_id():
    with get_db() as session:
        share = share_repo.get_share_with_max_id(session)
        if share:
            return response.ShareResponse(status=1, msg="success", data=[share])
        else:
            return response.ShareResponse(status=-1, msg="internal database error", data=[])

def create_share_dest(share_id, dest_agent_id):
    with get_db() as session:
        share_dest = share_repo.create_share_dest(session, share_id, dest_agent_id)
        if share_dest:
            return response.Response(status=1, msg="success")
        else:
            return response.Response(status=-1, msg="Create Share Dest Agent failed")

def create_share_de(share_id, de_id):
    with get_db() as session:
        share_de = share_repo.create_share_de(session, share_id, de_id)
        if share_de:
            return response.Response(status=1, msg="success")
        else:
            return response.Response(status=-1, msg="Create Share Data Element failed")

def create_share_policy(share_id, approval_agent_id, status):
    with get_db() as session:
        share_policy = share_repo.create_share_policy(session, share_id, approval_agent_id, status)
        if share_policy:
            return response.Response(status=1, msg="success")
        else:
            return response.Response(status=-1, msg="Create Share Policy failed")

def get_approval_for_share(share_id):
    with get_db() as session:
        return share_repo.get_approval_for_share(session, share_id)

def get_dest_for_share(share_id):
    with get_db() as session:
        return share_repo.get_dest_for_share(session, share_id)

def get_de_for_share(share_id):
    with get_db() as session:
        return share_repo.get_de_for_share(session, share_id)

def get_share(share_id):
    with get_db() as session:
        return share_repo.get_share(session, share_id)

def set_checkpoint_table_paths(table_paths):
    check_point.set_table_paths(table_paths)

def check_point_all_tables(key_manager):
    check_point.check_point_all_tables(key_manager)

def recover_db_from_snapshots(key_manager):
    check_point.recover_db_from_snapshots(key_manager)

def clear_checkpoint_table_paths():
    check_point.clear()
