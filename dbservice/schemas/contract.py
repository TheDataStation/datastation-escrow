from sqlalchemy import Column, Integer, String

from ..database import Base


class Contract(Base):
    __tablename__ = "Contracts"

    id = Column(Integer, primary_key=True, index=True)
    function = Column(String)
    function_param = Column(String)
