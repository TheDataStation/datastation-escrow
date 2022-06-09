from sqlalchemy import Column, String
from ..database import Base


class API(Base):
    __tablename__ = "APIs"

    api_name = Column(String, primary_key=True, index=True)
