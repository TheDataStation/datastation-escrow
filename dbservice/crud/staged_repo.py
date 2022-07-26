from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..schemas.staged import Staged
from common.pydantic_models.staged import StagedCreate


def create_staged(db: Session, staged: StagedCreate):
    db_staged = Staged(id=staged.id,
                       caller_id=staged.caller_id,
                       api=staged.api,)

    try:
        db.add(db_staged)
        db.commit()
        db.refresh(db_staged)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_staged

def get_all_staged(db: Session):
    staged = db.query(Staged).all()
    return staged
