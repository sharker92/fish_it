from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


engine = create_engine("sqlite:///test.db", echo=False)
Base = declarative_base()
metadata = Base.metadata
_Session = sessionmaker(bind=engine)


# use session_factory() to get a new Session
def session_factory():
    metadata.create_all(engine)
    return _Session()


def reset_db():
    metadata.drop_all(engine)
    print("Tables deleted")
    metadata.create_all(engine)
    print("Tables created")
