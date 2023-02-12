from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..schemas.policy import Policy
from common.pydantic_models.policy import PolicyCreate

def create_policy(db: Session, policy: PolicyCreate):
    db_policy = Policy(user_id=policy.user_id,
                       api=policy.api,
                       data_id=policy.data_id,
                       share_id=policy.share_id,
                       status=policy.status,)
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
                                Policy.data_id == policy.data_id,
                                Policy.share_id == policy.share_id,
                                Policy.status == policy.status).delete()
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "success"

def get_all_policies(db: Session):
    policies = db.query(Policy).all()
    return policies

def get_policy_for_user(db: Session, user_id: str, share_id: int):
    policies = db.query(Policy).filter(Policy.user_id == user_id,
                                       Policy.share_id == share_id,
                                       Policy.status == 1).all()
    return policies

# The following function recovers the policy table from a list of Policy
def bulk_upload_policies(db: Session, policies):
    # print("Bulk upload policies")
    # print(policies)
    policies_to_add = []
    for policy in policies:
        cur_policy = Policy(user_id=policy.user_id,
                            api=policy.api,
                            data_id=policy.data_id,
                            share_id=policy.share_id,
                            status=policy.status,
                            )
        policies_to_add.append(cur_policy)
    try:
        db.add_all(policies_to_add)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return "success"
