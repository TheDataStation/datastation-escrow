from .database import engine, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event

from .crud import (user_repo,
                   dataelement_repo,
                   function_repo,
                   function_dependency_repo,
                   contract_repo, )
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

def create_user(user_id, user_name, password):
    with get_db() as session:
        user = user_repo.create_user(session, user_id, user_name, password)
        if user:
            return {"status": 0, "message": "success", "data": user}
        else:
            return {"status": 1, "message": "database error: create user failed"}

def get_user_by_user_name(user_name):
    with get_db() as session:
        user = user_repo.get_user_by_user_name(session, user_name)
        if user:
            return {"status": 0, "message": "success", "data": user}
        else:
            return {"status": 1, "message": "database error: get user by username failed"}

def get_user_with_max_id():
    with get_db() as session:
        user = user_repo.get_user_with_max_id(session)
        if user:
            return {"status": 0, "message": "success", "data": user}
        else:
            return {"status": 1, "message": "database error: get user with max id failed"}

def get_all_users():
    with get_db() as session:
        users = user_repo.get_all_users(session)
        if len(users):
            return {"status": 0, "message": "success", "data": users}
        else:
            return {"status": 1, "message": "no existing users"}

def recover_users(users):
    with get_db() as session:
        res = user_repo.recover_users(session, users)
        if res:
            return 0

def create_de(de_id, de_name, user_id, de_type, access_param, optimistic):
    with get_db() as session:
        de = dataelement_repo.create_de(session, de_id, de_name, user_id, de_type, access_param, optimistic)
        if de:
            return {"status": 0, "message": "success", "data": de}
        else:
            return {"status": 1, "message": "database error: create de failed"}

def get_de_by_name(de_name):
    with get_db() as session:
        de = dataelement_repo.get_de_by_name(session, de_name)
        if de:
            return {"status": 0, "message": "success", "data": de}
        else:
            return {"status": 1, "message": "database error: get DE by name failed"}

def get_de_by_id(de_id: int):
    with get_db() as session:
        de = dataelement_repo.get_de_by_id(session, de_id)
        if de:
            return {"status": 0, "message": "success", "data": de}
        else:
            return {"status": 1, "message": "database error: get DE by id failed"}

def get_de_by_access_type(request):
    with get_db() as session:
        de = dataelement_repo.get_de_by_access_param(session, request)
        if de:
            return {"status": 0, "message": "success", "data": de}
        else:
            return {"status": 1, "message": "database error: get DE by access type failed"}

def list_discoverable_des():
    with get_db() as session:
        discoverable_des = dataelement_repo.list_discoverable_des(session)
        return discoverable_des

def get_des_by_ids(request):
    with get_db() as session:
        des = dataelement_repo.get_des_by_ids(session, request)
        if len(des) > 0:
            return {"status": 0, "message": "success", "data": des}
        else:
            return {"status": 1, "message": "database error: no DE found for give IDs"}

def remove_de_by_name(de_name):
    with get_db() as session:
        res = dataelement_repo.remove_de_by_name(session, de_name)
        if res == "success":
            return {"status": 0, "message": "success"}
        else:
            return {"status": 1, "message": "database error: remove DE by name failed"}

def get_de_owner_id(request):
    with get_db() as session:
        res = dataelement_repo.get_de_owner_id(session, request)
        if res:
            return {"status": 0, "message": "success", "data": res}
        else:
            return {"status": 1, "message": "database error: get DE owner id failed"}

def get_all_des():
    with get_db() as session:
        des = dataelement_repo.get_all_des(session)
        if len(des):
            return {"status": 0, "message": "success", "data": des}
        else:
            return {"status": 1, "message": "database error: no existing DEs"}

def get_de_with_max_id():
    with get_db() as session:
        de = dataelement_repo.get_de_with_max_id(session)
        if de:
            return {"status": 0, "message": "success", "data": de}
        else:
            return {"status": 1, "message": "database error: get DE with max ID failed"}

def recover_des(des):
    with get_db() as session:
        res = dataelement_repo.recover_des(session, des)
        if res is not None:
            return 0

def create_function(function_name):
    with get_db() as session:
        function = function_repo.create_function(session, function_name)
        if function:
            return {"status": 0, "message": "success", "data": function}
        else:
            return {"status": 1, "message": "database error: register function in DB failed"}

def get_all_functions():
    with get_db() as session:
        functions = function_repo.get_all_functions(session)
        if len(functions):
            return {"status": 0, "message": "success", "data": functions}
        else:
            return {"status": 1, "message": "database error: no registered functions found"}

def create_function_dependency(from_f, to_f):
    with get_db() as session:
        function_dependency = function_dependency_repo.create_function_dependency(session, from_f, to_f)
        if function_dependency:
            return {"status": 0, "message": "success", "data": function_dependency}
        else:
            return {"status": 1, "message": "database error: create function dependency failed"}

def get_all_function_dependencies():
    with get_db() as session:
        function_dependencies = function_dependency_repo.get_all_dependencies(session)
        if len(function_dependencies):
            return {"status": 0, "message": "success", "data": function_dependencies}
        else:
            return {"status": 1, "message": "database error: no function dependencies found"}

def create_contract(contract_id, contract_function, contract_function_param):
    with get_db() as session:
        contract = contract_repo.create_contract(session, contract_id, contract_function, contract_function_param)
        if contract:
            return {"status": 0, "message": "success", "data": contract}
        else:
            return {"status": 1, "message": "database error: create contract failed"}

def get_contract_with_max_id():
    with get_db() as session:
        contract = contract_repo.get_contract_with_max_id(session)
        if contract:
            return {"status": 1, "message": "success", "data": contract}
        else:
            return {"status": 1, "message": "database error: get contract with max ID failed"}

def create_share_dest(share_id, dest_agent_id):
    with get_db() as session:
        share_dest = contract_repo.create_share_dest(session, share_id, dest_agent_id)
        if share_dest:
            return response.Response(status=1, msg="success")
        else:
            return response.Response(status=-1, msg="Create Contract Dest Agent failed")

def create_share_de(share_id, de_id):
    with get_db() as session:
        share_de = contract_repo.create_share_de(session, share_id, de_id)
        if share_de:
            return response.Response(status=1, msg="success")
        else:
            return response.Response(status=-1, msg="Create Contract Data Element failed")

def create_share_policy(share_id, approval_agent_id, status):
    with get_db() as session:
        share_policy = contract_repo.create_share_policy(session, share_id, approval_agent_id, status)
        if share_policy:
            return response.Response(status=1, msg="success")
        else:
            return response.Response(status=-1, msg="Create Contract Policy failed")

def get_approval_for_share(share_id):
    with get_db() as session:
        return contract_repo.get_approval_for_share(session, share_id)

def get_status_for_share(share_id):
    with get_db() as session:
        return contract_repo.get_status_for_share(session, share_id)

def get_dest_for_share(share_id):
    with get_db() as session:
        return contract_repo.get_dest_for_share(session, share_id)

def get_de_for_share(share_id):
    with get_db() as session:
        return contract_repo.get_de_for_share(session, share_id)

def get_share(share_id):
    with get_db() as session:
        return contract_repo.get_share(session, share_id)

def approve_share(a_id, share_id):
    with get_db() as session:
        return contract_repo.approve_share(session, a_id, share_id)

def set_checkpoint_table_paths(table_paths):
    check_point.set_table_paths(table_paths)

def check_point_all_tables(key_manager):
    check_point.check_point_all_tables(key_manager)

def recover_db_from_snapshots(key_manager):
    check_point.recover_db_from_snapshots(key_manager)

def clear_checkpoint_table_paths():
    check_point.clear()
