from sqlalchemy import Column, ForeignKey, Integer, String, Boolean
from sqlalchemy.orm import relationship

from ..database import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    description = Column(String)
    type = Column(String)
    access_type = Column(String)
    optimistic = Column(Boolean)
    owner_id = Column(Integer)
    original_data_size = Column(Integer)
