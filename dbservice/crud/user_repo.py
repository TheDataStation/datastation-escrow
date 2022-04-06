from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..models.user import User
from common.pydantic_models.user import UserRegister


def get_user(db: Session, user_id: int):
    user = db.query(User).filter(User.id == user_id).first()
    if user:
        return user
    return None


def get_user_by_user_name(db: Session, user_name: str):
    res = db.query(User).filter(User.user_name == user_name)
    # print(res)
    # print(user_name)
    # print(res.first())
    user = res.first()
    if user:
        return user
    else:
        return None

# The following function returns the user with the max ID
def get_user_with_max_id(db: Session):
    max_id = db.query(func.max(User.id)).scalar_subquery()
    user = db.query(User).filter(User.id == max_id).first()
    if user:
        return user
    else:
        return None


def get_all_users(db: Session):
    # users = db.query(User).limit(limit).all()
    users = db.query(User).all()
    return users


def create_user(db: Session, user: UserRegister):
    db_user = User(id=user.id,
                   user_name=user.user_name,
                   password=user.password,)
    try:
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return db_user

# The following function recovers the user table from a list of User
def recover_users(db: Session, users):
    users_to_add = []
    for user in users:
        cur_user = User(id=user.id, user_name=user.user_name, password=user.password)
        users_to_add.append(cur_user)
    try:
        db.add_all(users_to_add)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return "success"
