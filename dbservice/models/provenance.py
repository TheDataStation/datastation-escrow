from sqlalchemy import Column, Integer, ForeignKey

from ..database import Base


class Provenance(Base):
    __tablename__ = "provenance"

    child_id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, primary_key=True)
