from sqlalchemy import Column, Integer, String

from ..database import Base


class Share(Base):
    __tablename__ = "shares"

    id = Column(Integer, primary_key=True, index=True)
    template = Column(String)
    param = Column(String)
