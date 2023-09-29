from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..schemas.function_dependency import FunctionDependency

def create_function_dependency(db: Session, from_f, to_f):
    db_f_depend = FunctionDependency(from_f=from_f, to_f=to_f,)
    try:
        db.add(db_f_depend)
        db.commit()
        db.refresh(db_f_depend)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_f_depend

def get_all_dependencies(db: Session):
    f_depends = db.query(FunctionDependency).all()
    return f_depends
