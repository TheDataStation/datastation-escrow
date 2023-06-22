from sqlalchemy import Column, Integer

from ..database import Base


class ShareDE(Base):
    __tablename__ = "sharede"

    s_id = Column(Integer, primary_key=True, index=True)
    de_id = Column(Integer, primary_key=True)
