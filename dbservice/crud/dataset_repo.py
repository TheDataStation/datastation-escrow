from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..models.dataset import Dataset
from ..models.user import User

from ..schemas.dataset import DatasetCreate


def get_all_datasets(db: Session):
    all_data = db.query(Dataset).all()
    return all_data

def get_dataset_by_id(db: Session, dataset_id: int):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if dataset:
        return dataset
    else:
        return None


def get_dataset_by_name(db: Session, name: str):
    dataset = db.query(Dataset).filter(Dataset.name == name).first()
    if dataset:
        return dataset
    else:
        return None


def remove_dataset_by_name(db: Session, name: str):
    try:
        db.query(Dataset).filter(Dataset.name == name).delete()
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return "success"


def create_dataset(db: Session, dataset: DatasetCreate):
    db_dataset = Dataset(id=dataset.id,
                         owner_id=dataset.owner_id,
                         name=dataset.name,
                         type=dataset.type,
                         access_type=dataset.access_type,
                         description=dataset.description,)

    try:
        db.add(db_dataset)
        db.commit()
        db.refresh(db_dataset)
    except SQLAlchemyError as e:
        db.rollback()
        return None
    return db_dataset


# The following function returns the owner, given a dataset ID.
def get_dataset_owner(db: Session, dataset_id: int):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if dataset:
        user = db.query(User).filter(User.id == dataset.owner_id).first()
        if user:
            return user
    return None

