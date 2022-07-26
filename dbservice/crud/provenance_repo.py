from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..schemas.provenance import Provenance

from common.pydantic_models.provenance import ProvenanceCreate


def create_provenance(db: Session, provenance: ProvenanceCreate):
    db_provenance = Provenance(child_id=provenance.child_id,
                               parent_id=provenance.parent_id,)
    try:
        db.add(db_provenance)
        db.commit()
        db.refresh(db_provenance)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_provenance

def bulk_create_provenance(db: Session, child_id, provenances):
    provenances_to_add = []
    for parent_id in provenances:
        cur_provenance = Provenance(child_id=child_id,
                                    parent_id=parent_id,)
        provenances_to_add.append(cur_provenance)
    try:
        db.add_all(provenances_to_add)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "success"
