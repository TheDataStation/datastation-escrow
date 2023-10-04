from sqlalchemy import Column, String
from ..database import Base


class Function(Base):
    __tablename__ = "Functions"

    function_name = Column(String, primary_key=True, index=True)
