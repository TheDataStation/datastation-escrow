from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..models.api import API
from ..schemas.api import APICreate

def create_api(db: Session, api: APICreate):
    db_api = API(api_name=api.api_name,)
    try:
        db.add(db_api)
        db.commit()
        db.refresh(db_api)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_api

def get_all_apis(db: Session):
    apis = db.query(API).all()
    api_name_array = []
    for api in apis:
        api_name_array.append(api.api_name)
    # Note: we do not return the special element "dir_accessible"
    if "dir_accessible" in api_name_array:
        api_name_array.remove("dir_accessible")
    return api_name_array