from sqlalchemy import Column, ForeignKey, Integer, String, Boolean
from sqlalchemy.orm import relationship

from ..database import Base


class DataElement(Base):
    __tablename__ = "DataElements"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    type = Column(String)
    access_param = Column(String)
    optimistic = Column(Boolean)
    owner_id = Column(Integer)
