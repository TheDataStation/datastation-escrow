from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..schemas.function import Function

def create_function(db: Session, function_name):
    db_function = Function(function_name=function_name,)
    try:
        db.add(db_function)
        db.commit()
        db.refresh(db_function)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_function

def get_all_functions(db: Session):
    functions = db.query(Function).all()
    function_name_array = []
    for function in functions:
        function_name_array.append(function.function_name)
    return function_name_array
