from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..schemas.policy import Policy

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
