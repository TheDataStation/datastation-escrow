from sqlalchemy import Column, Integer, String

from ..database import Base


class Policy(Base):
    __tablename__ = "Policies"

    a_id = Column(Integer, primary_key=True, index=True)
    de_ids = Column(String, primary_key=True)  # We do string comparion here
    function = Column(String, primary_key=True)
    function_param = Column(String, primary_key=True)
