from sqlalchemy import Column, Integer

from ..database import Base


class ContractDest(Base):
    __tablename__ = "ContractDests"

    c_id = Column(Integer, primary_key=True, index=True)
    a_id = Column(Integer, primary_key=True)
