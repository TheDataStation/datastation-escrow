from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..schemas.share_dest import ShareDest

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
