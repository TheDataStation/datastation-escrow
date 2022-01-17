from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship

from ..database import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    description = Column(String)
    upload = Column(Boolean)
    url = Column(String)
    # owner_id = Column(Integer, ForeignKey("users.id"))
    owner_id = Column(Integer)

    # owner = relationship("User", back_populates="datasets")
