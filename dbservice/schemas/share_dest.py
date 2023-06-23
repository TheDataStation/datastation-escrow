from sqlalchemy import Column, Integer

from ..database import Base


class ShareDest(Base):
    __tablename__ = "sharedest"

    s_id = Column(Integer, primary_key=True, index=True)
    a_id = Column(Integer, primary_key=True)