import pytest
from sqlalchemy.orm.exc import NoResultFound
from datetime import date
from src.crawler.crawler import add_url_to_db, input_today_at_db, get_new_url
from src.databases.web import Web
from src.databases.page import Page
from src.databases.date import Date
from src.databases.base import session_factory, delete_db
# export PYTHONPATH="${PYTHONPATH}:~/OneDrive/03Programming/01_fish_it"


@pytest.fixture
def create_session():
    print("CREATE")
    session = session_factory()
    yield session
    session.close()
    delete_db()
    print("DELETE")


@pytest.fixture(params=[
    'https://alsuper.com /',
    'https://www.walmart.com.mx',
    'https://www.liverpool.com.mx/',
])
def fill_database(request):
    request.param
    print("TWO")
    print(request.param)
    session.add(Page(url="url", interest=0, error=0))
    # session.add(Web(url=url))


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
        assert(url == web.url)


def test_input_today_at_db(create_session):
    session = create_session
    today = date.today()
    return_today = input_today_at_db()
    today_db = session.query(Date).filter_by(dated=today).one()
    assert(today == today_db.dated and today == return_today)


def test_get_new_url(create_session, fill_database):
    print("three")
    session = create_session
    session.add(Date(dated='2020-05-11'))
    today = input_today_at_db()
    get_new_url(today)
    assert(False)
