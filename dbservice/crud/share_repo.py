from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..schemas.share import Share
from ..schemas.share_dest import ShareDest
from ..schemas.share_de import ShareDE
from ..schemas.share_policy import SharePolicy

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

def create_share_dest(db: Session, s_id, a_id):
    db_share_dest = ShareDest(s_id=s_id, a_id=a_id)
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

def get_share_with_max_id(db: Session):
    """
    Get the share with the max ID.
    """
    max_id = db.query(func.max(Share.id)).scalar_subquery()
    share = db.query(Share).filter(Share.id == max_id).first()
    if share:
        return share
    else:
        return None

def get_approval_for_share(db: Session, share_id):
    approval_agents = db.query(SharePolicy.a_id).filter(SharePolicy.s_id == share_id).all()
    return approval_agents

def get_de_for_share(db: Session, share_id):
    de_in_share = db.query(ShareDE.de_id).filter(ShareDE.s_id == share_id).all()
    return de_in_share

def get_dest_for_share(db: Session, share_id):
    dest_agents = db.query(ShareDest.a_id).filter(ShareDest.s_id == share_id).all()
    return dest_agents

def get_share(db: Session, share_id):
    share = db.query(Share).filter(Share.id == share_id).first()
    return share

def approve_share(db: Session, a_id, share_id):
    try:
        db.query(SharePolicy).filter(SharePolicy.a_id == a_id, SharePolicy.s_id == share_id).update({'status': 1})
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "update success"
