from sqlalchemy import Column, String, ForeignKey

from ..database import Base


class FunctionDependency(Base):
    __tablename__ = "FunctionDependencies"

    from_f = Column(String, ForeignKey("Functions.function_name"), primary_key=True,)
    to_f = Column(String, ForeignKey("Functions.function_name"), primary_key=True)
