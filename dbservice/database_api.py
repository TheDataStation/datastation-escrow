from .database import engine, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event

from .crud import (user_repo,
                   dataelement_repo,
                   function_repo,
                   function_dependency_repo,
                   contract_repo, )
from contextlib import contextmanager
from dbservice.checkpoint.check_point import check_point

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from .schemas.contract import Contract
from .schemas.contract_dest import ContractDest
from .schemas.contract_de import ContractDE
from .schemas.contract_status import ContractStatus

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

def create_de(de_id, de_name, user_id, de_type, access_param):
    with get_db() as session:
        de = dataelement_repo.create_de(session, de_id, de_name, user_id, de_type, access_param)
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

def get_des_by_ids(request):
    with get_db() as session:
        des = dataelement_repo.get_des_by_ids(session, request)
        if len(des) > 0:
            return {"status": 0, "message": "success", "data": des}
        else:
            return {"status": 1, "message": "database error: no DE found for give DE IDs"}

def remove_de_by_name(de_name):
    with get_db() as session:
        res = dataelement_repo.remove_de_by_name(session, de_name)
        if res == "success":
            return {"status": 0, "message": "success"}
        else:
            return {"status": 1, "message": "database error: remove DE by name failed"}

def remove_de_by_id(de_id):
    with get_db() as session:
        res = dataelement_repo.remove_de_by_id(session, de_id)
        if res == "success":
            return {"status": 0, "message": "success"}
        else:
            return {"status": 1, "message": "database error: remove DE by ID failed"}

def get_de_owner_id(de_id):
    with get_db() as session:
        res = dataelement_repo.get_de_owner_id(session, de_id)
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

def create_contract_dest(contract_id, dest_agent_id):
    with get_db() as session:
        contract_dest = contract_repo.create_contract_dest(session, contract_id, dest_agent_id)
        if contract_dest:
            return {"status": 0, "message": "success"}
        else:
            return {"status": 1, "message": "database error: create contract dest agents failed"}

def create_contract_de(contract_id, de_id):
    with get_db() as session:
        contract_de = contract_repo.create_contract_de(session, contract_id, de_id)
        if contract_de:
            return {"status": 0, "message": "success"}
        else:
            return {"status": 1, "message": "database error: create contract DE failed"}

def create_contract_status(contract_id, approval_agent_id, status):
    with get_db() as session:
        contract_status = contract_repo.create_contract_status(session, contract_id, approval_agent_id, status)
        if contract_status:
            return {"status": 0, "message": "success"}
        else:
            return {"status": 1, "message": "database error: create contract status failed"}

def get_src_for_contract(contract_id):
    with get_db() as session:
        return contract_repo.get_src_for_contract(session, contract_id)

def get_status_for_contract(contract_id):
    with get_db() as session:
        return contract_repo.get_status_for_contract(session, contract_id)

def get_dest_for_contract(contract_id):
    with get_db() as session:
        return contract_repo.get_dest_for_contract(session, contract_id)

def get_de_for_contract(contract_id):
    with get_db() as session:
        return contract_repo.get_de_for_contract(session, contract_id)

def get_contract(contract_id):
    with get_db() as session:
        contract = contract_repo.get_contract(session, contract_id)
        if contract:
            return {"status": 0, "message": "success", "data": contract}
        else:
            return {"status": 1, "message": "database error: get contract failed"}

def get_all_contracts_for_dest(dest_agent_id):
    with get_db() as session:
        contract_ids = contract_repo.get_all_contracts_for_dest(session, dest_agent_id)
        if contract_ids:
            return {"status": 0, "message": "success", "data": contract_ids}
        else:
            return {"status": 1, "message": "No contracts found for destination agent."}

def get_all_contracts_for_src(src_agent_id):
    with get_db() as session:
        contract_ids = contract_repo.get_all_contracts_for_src(session, src_agent_id)
        if contract_ids:
            return {"status": 0, "message": "success", "data": contract_ids}
        else:
            return {"status": 1, "message": "No contracts found for source agent."}


def approve_contract(a_id, contract_id):
    with get_db() as db:
        try:
            db.query(ContractStatus).filter(ContractStatus.a_id == a_id,
                                            ContractStatus.c_id == contract_id) \
                .update({'status': 1})
            db.commit()
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: approve contract failed"}
        return {"status": 0, "message": "success"}


def reject_contract(a_id, contract_id):
    with get_db() as db:
        try:
            db.query(ContractStatus).filter(ContractStatus.a_id == a_id,
                                            ContractStatus.c_id == contract_id) \
                .update({'status': -1})
            db.commit()
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: reject contract failed"}
        return {"status": 0, "message": "success"}


def set_checkpoint_table_paths(table_paths):
    check_point.set_table_paths(table_paths)

def check_point_all_tables(key_manager):
    check_point.check_point_all_tables(key_manager)

def recover_db_from_snapshots(key_manager):
    check_point.recover_db_from_snapshots(key_manager)

def clear_checkpoint_table_paths():
    check_point.clear()
