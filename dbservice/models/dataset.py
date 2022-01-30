from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship

from ..database import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    description = Column(String)
    type = Column(String)
    access_type = Column(String)
    # owner_id = Column(Integer, ForeignKey("users.id"))
    owner_id = Column(Integer)

    # owner = relationship("User", back_populates="datasets")
