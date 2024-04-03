from sqlalchemy import Column, Integer

from ..database import Base


class DerivedDE(Base):
    __tablename__ = "DerivedDEs"

    de_id = Column(Integer, primary_key=True, index=True)
    src_de_id = Column(Integer, primary_key=True)
