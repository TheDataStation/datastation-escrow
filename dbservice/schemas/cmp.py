from sqlalchemy import Column, Integer, String

from ..database import Base


class CMP(Base):
    __tablename__ = "CMPs"

    src_a_id = Column(Integer, primary_key=True, index=True)
    dest_a_id = Column(Integer, primary_key=True)
    de_id = Column(Integer, primary_key=True)
    function = Column(String, primary_key=True)
