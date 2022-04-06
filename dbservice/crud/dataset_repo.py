from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from ..models.dataset import Dataset
from ..models.user import User
from common.pydantic_models.dataset import DatasetCreate


def get_all_datasets(db: Session):
    all_data = db.query(Dataset).all()
    return all_data

# The following function returns all datasets with optimistic == True
def get_all_optimistic_datasets(db: Session):
    all_optimistic_data = db.query(Dataset).filter(Dataset.optimistic == True).all()
    return all_optimistic_data

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

# zz: get id by access_type
def get_dataset_by_access_type(db: Session, access_type: str):
    dataset = db.query(Dataset).filter(Dataset.access_type == access_type).first()
    if dataset:
        return dataset
    else:
        return None

def get_datasets_by_paths(db: Session, paths):
    # session.query(MyUserClass).filter(MyUserClass.id.in_((123,456))).all()
    datasets = db.query(Dataset).filter(Dataset.access_type.in_(tuple(paths))).all()
    return datasets

def get_datasets_by_ids(db: Session, ids: list):
    datasets = db.query(Dataset).filter(Dataset.id.in_(tuple(ids))).all()
    return datasets

def remove_dataset_by_name(db: Session, name):
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
                         description=dataset.description,
                         optimistic=dataset.optimistic,
                         original_data_size=dataset.original_data_size)

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

# The following function returns the dataset with the max ID
def get_data_with_max_id(db: Session):
    max_id = db.query(func.max(Dataset.id)).scalar_subquery()
    dataset = db.query(Dataset).filter(Dataset.id == max_id).first()
    if dataset:
        return dataset
    else:
        return None

# The following function recovers the data table from a list of Data
def recover_datas(db: Session, datas):
    datas_to_add = []
    for data in datas:
        cur_data = Dataset(id=data.id,
                           owner_id=data.owner_id,
                           name=data.name,
                           type=data.type,
                           access_type=data.access_type,
                           description=data.description,
                           optimistic=data.optimistic,)
        datas_to_add.append(cur_data)
    try:
        db.add_all(datas_to_add)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return None

    return "success"
