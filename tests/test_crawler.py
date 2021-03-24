import pytest
from sqlalchemy.orm.exc import NoResultFound
from src.crawler.crawler import add_url_to_db
from src.databases.web import Web
from src.databases.base import session_factory_test, delete_db_test
# export PYTHONPATH="${PYTHONPATH}:~/OneDrive/03Programming/01_fish_it"


@pytest.fixture
def create_session():
    # global session
    session = session_factory_test()
    print("ONE")
    yield session
    session.close()
    delete_db_test()
    print("THREE")


@pytest.mark.parametrize('url',
                         [
                             ('https://alsuper.com/'),
                             ('https://www.walmart.com.mx'),
                             ('https://www.liverpool.com.mx/'),
                             ('not_an_url'),
                             ('not an url.com'),
                         ]
                         )
def test_add_url_to_db(url, create_session):
    session = create_session
    add_url_to_db(url)
    if (url.endswith('/')):
        url = url[:-1]
    try:
        web = None
        web = session.query(Web).filter_by(url=url).one()
    except NoResultFound:
        assert(not web)
    else:
        assert(web.url == url)
