from sqlalchemy import Column, Integer, Date
from .base import Base


class Date(Base):
    __tablename__ = 'Dates'

    id = Column(Integer, primary_key=True)
    dated = Column(Date, unique=True, sqlite_on_conflict_unique='IGNORE')

    def __init__(self, dated):
        self.dated = dated

    def __repr__(self):
        return f"<Dates date={self.dated}>"
