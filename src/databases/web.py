from sqlalchemy import Column, Integer, Text
from .base import Base


class Web(Base):
    __tablename__ = 'Webs'

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, sqlite_on_conflict_unique='IGNORE')

    def __init__(self, url):
        self.url = url

    def __repr__(self):
        return f"<Webs url={self.url}>"
