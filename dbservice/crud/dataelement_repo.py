from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..schemas.dataelement import DataElement
from ..schemas.user import User


def get_all_des(db: Session):
    des = db.query(DataElement).all()
    return des


def list_discoverable_des(db: Session):
    all_discoverable_de = db.query(DataElement.id).filter(DataElement.discoverable == True).all()
    return all_discoverable_de


def get_de_by_id(db: Session, de_id: int):
    de = db.query(DataElement).filter(DataElement.id == de_id).first()
    if de:
        return de
    else:
        return None


def get_de_by_name(db: Session, name: str):
    de = db.query(DataElement).filter(DataElement.name == name).first()
    if de:
        return de
    else:
        return None


def get_de_by_access_param(db: Session, access_param: str):
    de = db.query(DataElement).filter(DataElement.access_param == access_param).first()
    if de:
        return de
    else:
        return None


def get_des_by_ids(db: Session, ids: list):
    des = db.query(DataElement).filter(DataElement.id.in_(tuple(ids))).all()
    return des


def remove_de_by_name(db: Session, de_name):
    try:
        db.query(DataElement).filter(DataElement.name == de_name).delete()
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "success"


def remove_de_by_id(db: Session, de_id):
    try:
        db.query(DataElement).filter(DataElement.id == de_id).delete()
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "success"


def create_de(db: Session, de_id, de_name, user_id, de_type, access_param, discoverable):
    db_de = DataElement(id=de_id,
                        owner_id=user_id,
                        name=de_name,
                        type=de_type,
                        access_param=access_param,
                        discoverable=discoverable)

    try:
        db.add(db_de)
        db.commit()
        db.refresh(db_de)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_de


# The following function returns the owner, given a DE ID.
def get_de_owner_id(db: Session, de_id: int):
    de = db.query(DataElement).filter(DataElement.id == de_id).first()
    if de:
        owner_id = db.query(User.id).filter(User.id == de.owner_id).first()[0]
        if owner_id:
            return owner_id
    return None


# The following function returns the DE with the max ID
def get_de_with_max_id(db: Session):
    max_id = db.query(func.max(DataElement.id)).scalar_subquery()
    de = db.query(DataElement).filter(DataElement.id == max_id).first()
    if de:
        return de
    else:
        return None


# The following function recovers the data table from a list of Data
def recover_des(db: Session, datas):
    datas_to_add = []
    for data in datas:
        cur_data = DataElement(id=data.id,
                               owner_id=data.owner_id,
                               name=data.name,
                               type=data.type,
                               access_param=data.access_param,
                               description=data.description,
                               discoverable=data.discoverable, )
        datas_to_add.append(cur_data)
    try:
        db.add_all(datas_to_add)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return "success"
