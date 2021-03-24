from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


engine = create_engine("sqlite:///crawl.db", echo=False)
engine_test = create_engine("sqlite:///test.db", echo=False)
Base = declarative_base()
metadata = Base.metadata
_Session = sessionmaker(bind=engine)
_Session_test = sessionmaker(bind=engine_test)


# use session_factory() to get a new Session
def session_factory():
    metadata.create_all(engine)
    return _Session()


def reset_db():
    metadata.drop_all(engine)
    print("Tables deleted")
    metadata.create_all(engine)
    print("Tables created")


def session_factory_test():
    metadata.create_all(engine_test)
    return _Session_test()


def delete_db_test():
    metadata.drop_all(engine_test)
    print("Tables deleted")
