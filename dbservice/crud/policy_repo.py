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

def get_all_policies(db: Session):
    policies = db.query(Policy).all()
    return policies
