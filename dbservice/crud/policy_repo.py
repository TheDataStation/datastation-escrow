from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..models.policy import Policy
from ..schemas.policy import PolicyCreate

def create_policy(db: Session, policy: PolicyCreate):
    db_policy = Policy(user_id=policy.user_id,
                       api=policy.api,
                       data_id=policy.data_id,)
    try:
        db.add(db_policy)
        db.commit()
        db.refresh(db_policy)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_policy

def remove_policy(db: Session, policy: PolicyCreate):
    try:
        db.query(Policy).filter(Policy.user_id == policy.user_id,
                                Policy.api == policy.api,
                                Policy.data_id == policy.data_id).delete()
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "success"

def get_all_policies(db: Session):
    policies = db.query(Policy).all()
    return policies

def get_policy_for_user(db: Session, user_id: str):
    policies = db.query(Policy).filter(Policy.user_id == user_id).all()
    return policies
