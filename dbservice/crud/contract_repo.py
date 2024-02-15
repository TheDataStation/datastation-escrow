from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..schemas.contract import Contract
from ..schemas.contract_dest import ContractDest
from ..schemas.contract_de import ContractDE
from ..schemas.contract_status import ContractStatus


def create_contract(db: Session, contract_id, contract_function, contract_function_param):
    db_contract = Contract(id=contract_id,
                           function=contract_function,
                           function_param=contract_function_param, )
    try:
        db.add(db_contract)
        db.commit()
        db.refresh(db_contract)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_contract


def create_contract_dest(db: Session, c_id, a_id):
    db_contract_dest = ContractDest(c_id=c_id, a_id=a_id)
    try:
        db.add(db_contract_dest)
        db.commit()
        db.refresh(db_contract_dest)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_contract_dest


def create_contract_de(db: Session, c_id, de_id):
    db_contract_de = ContractDE(c_id=c_id, de_id=de_id)
    try:
        db.add(db_contract_de)
        db.commit()
        db.refresh(db_contract_de)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_contract_de


def create_contract_status(db: Session, c_id, a_id, status):
    db_contract_status = ContractStatus(c_id=c_id, a_id=a_id, status=status)
    try:
        db.add(db_contract_status)
        db.commit()
        db.refresh(db_contract_status)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_contract_status


def get_contract_with_max_id(db: Session):
    """
    Get the contract with the max ID.
    """
    max_id = db.query(func.max(Contract.id)).scalar_subquery()
    contract = db.query(Contract).filter(Contract.id == max_id).first()
    if contract:
        return contract
    else:
        return None


def get_src_for_contract(db: Session, contract_id):
    approval_agents = db.query(ContractStatus.a_id).filter(ContractStatus.c_id == contract_id).all()
    return approval_agents


def get_status_for_contract(db: Session, contract_id):
    contract_status = db.query(ContractStatus.status).filter(ContractStatus.c_id == contract_id).all()
    return contract_status


def get_de_for_contract(db: Session, contract_id):
    de_in_contract = db.query(ContractDE.de_id).filter(ContractDE.c_id == contract_id).all()
    return de_in_contract


def get_dest_for_contract(db: Session, contract_id):
    dest_agents = db.query(ContractDest.a_id).filter(ContractDest.c_id == contract_id).all()
    return dest_agents


def get_contract(db: Session, contract_id):
    contract = db.query(Contract).filter(Contract.id == contract_id).first()
    return contract


def get_all_contracts_for_dest(db: Session, dest_agent_id):
    contract_ids = db.query(ContractDest.c_id).filter(ContractDest.a_id == dest_agent_id).all()
    return contract_ids


def get_all_contracts_for_src(db: Session, src_agent_id):
    contract_ids = db.query(ContractStatus.c_id).filter(ContractStatus.a_id == src_agent_id).all()
    return contract_ids


def approve_contract(db: Session, a_id, contract_id):
    try:
        db.query(ContractStatus).filter(ContractStatus.a_id == a_id,
                                        ContractStatus.c_id == contract_id)\
                                .update({'status': 1})
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "update success"


def reject_contract(db: Session, a_id, contract_id):
    try:
        db.query(ContractStatus).filter(ContractStatus.a_id == a_id,
                                        ContractStatus.c_id == contract_id)\
                                .update({'status': -1})
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "update success"
