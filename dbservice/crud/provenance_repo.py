from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..models.provenance import Provenance

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
