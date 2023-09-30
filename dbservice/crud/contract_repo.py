from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..schemas.contract import Contract
from ..schemas.share_dest import ContractDest
from ..schemas.share_de import ShareDE
from ..schemas.share_policy import SharePolicy


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


def create_share_de(db: Session, s_id, de_id):
    db_share_de = ShareDE(s_id=s_id, de_id=de_id)
    try:
        db.add(db_share_de)
        db.commit()
        db.refresh(db_share_de)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_share_de


def create_share_policy(db: Session, s_id, a_id, status):
    db_share_policy = SharePolicy(s_id=s_id, a_id=a_id, status=status)
    try:
        db.add(db_share_policy)
        db.commit()
        db.refresh(db_share_policy)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_share_policy


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


def get_approval_for_share(db: Session, share_id):
    approval_agents = db.query(SharePolicy.a_id).filter(SharePolicy.s_id == share_id).all()
    return approval_agents


def get_status_for_share(db: Session, share_id):
    share_status = db.query(SharePolicy.status).filter(SharePolicy.s_id == share_id).all()
    return share_status


def get_de_for_share(db: Session, share_id):
    de_in_share = db.query(ShareDE.de_id).filter(ShareDE.s_id == share_id).all()
    return de_in_share


def get_dest_for_contract(db: Session, c_id):
    dest_agents = db.query(ContractDest.a_id).filter(ContractDest.c_id == c_id).all()
    return dest_agents


def get_share(db: Session, share_id):
    share = db.query(Contract).filter(Contract.id == share_id).first()
    return share


def approve_share(db: Session, a_id, share_id):
    try:
        db.query(SharePolicy).filter(SharePolicy.a_id == a_id, SharePolicy.s_id == share_id).update({'status': 1})
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "update success"
