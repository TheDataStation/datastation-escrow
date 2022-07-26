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

# The following function recovers the staged table from a list of Staged
def recover_staged(db: Session, list_of_staged):
    staged_to_add = []
    for staged in list_of_staged:
        cur_staged = Staged(id=staged.id, caller_id=staged.caller_id, api=staged.api)
        staged_to_add.append(cur_staged)
    try:
        db.add_all(staged_to_add)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return "success"
