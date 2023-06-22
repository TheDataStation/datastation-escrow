from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..schemas.share import Share
from ..schemas.share_dest import ShareDest
from ..schemas.share_de import ShareDE

# The following function returns the share with the max ID
def get_share_with_max_id(db: Session):
    max_id = db.query(func.max(Share.id)).scalar_subquery()
    share = db.query(Share).filter(Share.id == max_id).first()
    if share:
        return share
    else:
        return None

def create_share(db: Session, share_id, share_template, share_param):
    db_share = Share(id=share_id, template=share_template, param=share_param)
    try:
        db.add(db_share)
        db.commit()
        db.refresh(db_share)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_share

def create_share_dest(db: Session, s_id, u_id):
    db_share_dest = ShareDest(s_id=s_id, u_id=u_id)
    try:
        db.add(db_share_dest)
        db.commit()
        db.refresh(db_share_dest)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_share_dest

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
