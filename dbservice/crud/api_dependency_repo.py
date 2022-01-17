from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..models.api_dependency import APIDependency
from ..schemas.api_dependency import APIDependencyCreate

def create_api_dependency(db: Session, api_depend: APIDependencyCreate):
    db_api_depend = APIDependency(from_api=api_depend.from_api,
                                  to_api=api_depend.to_api,)
    try:
        db.add(db_api_depend)
        db.commit()
        db.refresh(db_api_depend)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_api_depend

def get_all_dependencies(db: Session):
    api_depends = db.query(APIDependency).all()
    return api_depends
