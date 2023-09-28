from sqlalchemy import Column, String, Integer, ForeignKey

from ..database import Base


class Staged(Base):
    __tablename__ = "Derived"

    id = Column(Integer, primary_key=True)
    caller_id = Column(Integer, ForeignKey("users.id"))
    api = Column(String, ForeignKey("Functions.function_name"))
