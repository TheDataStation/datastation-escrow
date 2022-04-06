from sqlalchemy import Column, Integer, String

from ..database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    user_name = Column(String, unique=True, index=True)
    password = Column(String)

    # datasets = relationship("Dataset", back_populates="owner")
