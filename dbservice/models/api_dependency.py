from sqlalchemy import Column, String, ForeignKey

from ..database import Base


class APIDependency(Base):
    __tablename__ = "APIDependency"

    from_api = Column(String, ForeignKey("APIs.api_name"), primary_key=True,)
    to_api = Column(String, ForeignKey("APIs.api_name"), primary_key=True)
