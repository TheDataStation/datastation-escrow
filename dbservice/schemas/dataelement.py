from sqlalchemy import Column, ForeignKey, Integer, String, Boolean
from sqlalchemy.orm import relationship

from ..database import Base


class DataElement(Base):
    __tablename__ = "DataElements"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer)
    contract_id = Column(Integer)
