from sqlalchemy import Column, String, Integer, ForeignKey

from ..database import Base


class Policy(Base):
    __tablename__ = "Policy"

    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    api = Column(String, ForeignKey("APIs.api_name"), primary_key=True)
    data_id = Column(Integer, ForeignKey("datasets.id", ondelete='CASCADE'), primary_key=True)
    share_id = Column(Integer, primary_key=True)
    status = Column(Integer)
