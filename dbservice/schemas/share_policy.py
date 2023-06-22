from sqlalchemy import Column, Integer

from ..database import Base


class SharePolicy(Base):
    __tablename__ = "sharepolicy"

    s_id = Column(Integer, primary_key=True, index=True)
    a_id = Column(Integer, primary_key=True)
    status = Column(Integer)
