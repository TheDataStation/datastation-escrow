from .database import engine, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event

from contextlib import contextmanager
from dbservice.checkpoint.check_point import check_point

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from .schemas.user import User
from .schemas.dataelement import DataElement
from .schemas.function import Function
from .schemas.function_dependency import FunctionDependency
from .schemas.contract import Contract
from .schemas.contract_dest import ContractDest
from .schemas.contract_de import ContractDE
from .schemas.contract_status import ContractStatus
from .schemas.policy import Policy
from .schemas.cmp import CMP
from .schemas.derived_de import DerivedDE


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
    db_user = User(id=user_id,
                   user_name=user_name,
                   password=password, )
    with get_db() as db:
        try:
            db.add(db_user)
            db.commit()
            db.refresh(db_user)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create user failed"}
        return {"status": 0, "message": "success", "data": db_user}


def get_user_by_user_name(user_name):
    with get_db() as db:
        res = db.query(User).filter(User.user_name == user_name)
        user = res.first()
        if user:
            return {"status": 0, "message": "success", "data": user}
        else:
            return {"status": 1, "message": "database error: get user by username failed"}


def get_user_with_max_id():
    with get_db() as db:
        max_id = db.query(func.max(User.id)).scalar_subquery()
        user = db.query(User).filter(User.id == max_id).first()
        if user:
            return {"status": 0, "message": "success", "data": user}
        else:
            return {"status": 1, "message": "database error: get user with max id failed"}


def get_all_users():
    with get_db() as db:
        users = db.query(User).all()
        if len(users):
            return {"status": 0, "message": "success", "data": users}
        else:
            return {"status": 1, "message": "no existing users"}


def recover_users(users):
    with get_db() as db:
        users_to_add = []
        for user in users:
            cur_user = User(id=user.id, user_name=user.user_name, password=user.password)
            users_to_add.append(cur_user)
        try:
            db.add_all(users_to_add)
            db.commit()
        except SQLAlchemyError as e:
            db.rollback()
            return None
        return 0


def create_de(de_id, user_id, store_type, derived):
    with get_db() as db:
        db_de = DataElement(id=de_id,
                            owner_id=user_id,
                            store_type=store_type,
                            derived=derived)

        try:
            db.add(db_de)
            db.commit()
            db.refresh(db_de)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create de failed"}
        return {"status": 0, "message": "success", "data": db_de}


def get_de_by_id(de_id: int):
    with get_db() as db:
        de = db.query(DataElement).filter(DataElement.id == de_id).first()
        if de:
            return {"status": 0, "message": "success", "data": de}
        else:
            return {"status": 1, "message": "database error: get DE by id failed"}


def get_des_by_ids(de_ids):
    with get_db() as db:
        des = db.query(DataElement).filter(DataElement.id.in_(tuple(de_ids))).all()
        if len(des) > 0:
            return {"status": 0, "message": "success", "data": des}
        else:
            return {"status": 1, "message": "database error: no DE found for give DE IDs"}


def remove_de_by_id(de_id):
    with get_db() as db:
        try:
            db.query(DataElement).filter(DataElement.id == de_id).delete()
            db.commit()
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: remove DE by ID failed"}
        return {"status": 0, "message": "success"}


def get_de_owner_id(de_id):
    with get_db() as db:
        de = db.query(DataElement).filter(DataElement.id == de_id).first()
        if de:
            owner_id = db.query(User.id).filter(User.id == de.owner_id).first()[0]
            if owner_id:
                return {"status": 0, "message": "success", "data": owner_id}
        return {"status": 1, "message": "database error: get DE owner id failed"}


def get_all_des():
    with get_db() as db:
        des = db.query(DataElement).all()
        if len(des):
            return {"status": 0, "message": "success", "data": des}
        else:
            return {"status": 1, "message": "database error: no existing DEs"}


def get_de_with_max_id():
    with get_db() as db:
        max_id = db.query(func.max(DataElement.id)).scalar_subquery()
        de = db.query(DataElement).filter(DataElement.id == max_id).first()
        if de:
            return {"status": 0, "message": "success", "data": de}
        else:
            return {"status": 1, "message": "database error: get DE with max ID failed"}


def recover_des(des):
    with get_db() as db:
        des_to_add = []
        for de in des:
            cur_de = DataElement(id=de.id,
                                 owner_id=de.owner_id, )
            des_to_add.append(cur_de)
        try:
            db.add_all(des_to_add)
            db.commit()
        except SQLAlchemyError as e:
            db.rollback()
            return None
        return 0


def create_function(function_name):
    with get_db() as db:
        db_function = Function(function_name=function_name, )
        try:
            db.add(db_function)
            db.commit()
            db.refresh(db_function)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: register function in DB failed"}
        return {"status": 0, "message": "success", "data": db_function}


def get_all_functions():
    with get_db() as db:
        functions = db.query(Function).all()
        function_name_array = []
        for function in functions:
            function_name_array.append(function.function_name)
        if len(function_name_array):
            return {"status": 0, "message": "success", "data": function_name_array}
        else:
            return {"status": 1, "message": "database error: no registered functions found"}


def create_function_dependency(from_f, to_f):
    with get_db() as db:
        db_f_depend = FunctionDependency(from_f=from_f, to_f=to_f, )
        try:
            db.add(db_f_depend)
            db.commit()
            db.refresh(db_f_depend)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create function dependency failed"}
        return {"status": 0, "message": "success", "data": db_f_depend}


def get_all_function_dependencies():
    with get_db() as db:
        f_depends = db.query(FunctionDependency).all()
        if len(f_depends):
            return {"status": 0, "message": "success", "data": f_depends}
        else:
            return {"status": 1, "message": "database error: no function dependencies found"}


def create_contract(contract_id, contract_function, contract_function_param):
    with get_db() as db:
        db_contract = Contract(id=contract_id,
                               function=contract_function,
                               function_param=contract_function_param, )
        try:
            db.add(db_contract)
            db.commit()
            db.refresh(db_contract)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create contract failed"}
        return {"status": 0, "message": "success", "data": db_contract}


def create_contract_dest(contract_id, dest_agent_id):
    with get_db() as db:
        db_contract_dest = ContractDest(c_id=contract_id, a_id=dest_agent_id)
        try:
            db.add(db_contract_dest)
            db.commit()
            db.refresh(db_contract_dest)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create contract dest agents failed"}
        return {"status": 0, "message": "success"}


def create_contract_de(contract_id, de_id):
    with get_db() as db:
        db_contract_de = ContractDE(c_id=contract_id, de_id=de_id)
        try:
            db.add(db_contract_de)
            db.commit()
            db.refresh(db_contract_de)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create contract DE failed"}
        return {"status": 0, "message": "success"}


def create_contract_status(contract_id, approval_agent_id, status):
    with get_db() as db:
        db_contract_status = ContractStatus(c_id=contract_id, a_id=approval_agent_id, status=status)
        try:
            db.add(db_contract_status)
            db.commit()
            db.refresh(db_contract_status)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create contract status failed"}
        return {"status": 0, "message": "success"}


def get_contract_with_max_id():
    with get_db() as db:
        max_id = db.query(func.max(Contract.id)).scalar_subquery()
        contract = db.query(Contract).filter(Contract.id == max_id).first()
        if contract:
            return {"status": 0, "message": "success", "data": contract}
        else:
            return {"status": 1, "message": "database error: get contract with max ID failed"}


def create_cmp(src_a_id, dest_a_id, de_id, function):
    with get_db() as db:
        db_cmp = CMP(src_a_id=src_a_id,
                     dest_a_id=dest_a_id,
                     de_id=de_id,
                     function=function, )
        try:
            db.add(db_cmp)
            db.commit()
            db.refresh(db_cmp)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create contract management policy failed"}
        return {"status": 0, "message": "success"}


def get_cmp_for_src_and_f(src_a_id, function):
    with get_db() as db:
        return db.query(CMP).filter(CMP.src_a_id == src_a_id and CMP.function == function).all()


def get_src_for_contract(contract_id):
    with get_db() as db:
        return db.query(ContractStatus.a_id).filter(ContractStatus.c_id == contract_id).all()


def get_status_for_contract(contract_id):
    with get_db() as db:
        return db.query(ContractStatus.status).filter(ContractStatus.c_id == contract_id).all()


def get_dest_for_contract(contract_id):
    with get_db() as db:
        return db.query(ContractDest.a_id).filter(ContractDest.c_id == contract_id).all()


def get_de_for_contract(contract_id):
    with get_db() as db:
        return db.query(ContractDE.de_id).filter(ContractDE.c_id == contract_id).all()


def get_contract(contract_id):
    with get_db() as db:
        contract = db.query(Contract).filter(Contract.id == contract_id).first()
        if contract:
            return {"status": 0, "message": "success", "data": contract}
        else:
            return {"status": 1, "message": "database error: get contract failed"}


def get_all_contracts_for_dest(dest_agent_id):
    with get_db() as db:
        contract_ids = db.query(ContractDest.c_id).filter(ContractDest.a_id == dest_agent_id).all()
        if contract_ids:
            return {"status": 0, "message": "success", "data": contract_ids}
        else:
            return {"status": 1, "message": "No contracts found for destination agent."}


def get_all_contracts_for_src(src_agent_id):
    with get_db() as db:
        contract_ids = db.query(ContractStatus.c_id).filter(ContractStatus.a_id == src_agent_id).all()
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


def create_policy(a_id, de_ids, function, function_param):
    with get_db() as db:
        db_policy = Policy(a_id=a_id,
                           de_ids=de_ids,
                           function=function,
                           function_param=function_param)
        try:
            db.add(db_policy)
            db.commit()
            db.refresh(db_policy)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create policy failed"}
        return {"status": 0, "message": "success", "data": db_policy}


def check_policy_exists(a_id, de_ids, function, function_param):
    with get_db() as db:
        res = db.query(Policy).filter(Policy.a_id == a_id,
                                      Policy.de_ids == de_ids,
                                      Policy.function == function,
                                      Policy.function_param == function_param).all()
        if res:
            return True
        else:
            return False


def create_derived_de(de_id, src_de_id):
    with get_db() as db:
        db_derived_de = DerivedDE(de_id=de_id, src_de_id=src_de_id)
        try:
            db.add(db_derived_de)
            db.commit()
            db.refresh(db_derived_de)
        except SQLAlchemyError as e:
            db.rollback()
            return {"status": 1, "message": "database error: create derived DE failed"}
        return {"status": 0, "message": "success"}


def get_source_des_for_derived_de(de_id):
    with get_db() as db:
        return db.query(DerivedDE.src_de_id).filter(DerivedDE.de_id == de_id).all()


def set_checkpoint_table_paths(table_paths):
    check_point.set_table_paths(table_paths)


def check_point_all_tables(key_manager):
    check_point.check_point_all_tables(key_manager)


def recover_db_from_snapshots(key_manager):
    check_point.recover_db_from_snapshots(key_manager)


def clear_checkpoint_table_paths():
    check_point.clear()
