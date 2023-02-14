from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..schemas.share import Share
from common.pydantic_models.share import ShareCreate

# The following function returns the share with the max ID
def get_share_with_max_id(db: Session):
    max_id = db.query(func.max(Share.id)).scalar_subquery()
    share = db.query(Share).filter(Share.id == max_id).first()
    if share:
        return share
    else:
        return None

def create_share(db: Session, share: ShareCreate):
    db_share = Share(id=share.id)
    try:
        db.add(db_share)
        db.commit()
        db.refresh(db_share)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_share
