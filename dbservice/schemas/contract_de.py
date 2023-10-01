from sqlalchemy import Column, Integer

from ..database import Base


class ContractDE(Base):
    __tablename__ = "ContractDEs"

    c_id = Column(Integer, primary_key=True, index=True)
    de_id = Column(Integer, primary_key=True)
