from sqlalchemy import Column, Integer

from ..database import Base


class ContractStatus(Base):
    __tablename__ = "ContractStatus"

    c_id = Column(Integer, primary_key=True, index=True)
    a_id = Column(Integer, primary_key=True)
    status = Column(Integer)
