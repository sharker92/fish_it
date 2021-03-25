import pytest
from sqlalchemy.orm.exc import NoResultFound
from datetime import date
from src.crawler.crawler import add_url_to_db, input_today_at_db, get_new_url
from src.databases.web import Web
from src.databases.page import Page
from src.databases.date import Date
from src.databases.base import session_factory, delete_db
# export PYTHONPATH="${PYTHONPATH}:~/OneDrive/03Programming/01_fish_it"


@pytest.fixture(scope="class")
def create_session(request):
    print("CREATE")
    session = session_factory()
    request.cls.session = session
    yield
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
    request.cls.session.add(Page(url="url", interest=0, error=0))
    # session.add(Web(url=url))


@pytest.mark.usefixtures("create_session")
class Test_Basic_DB:
    @pytest.mark.parametrize('url',
                             [
                                 ('https://alsuper.com/'),
                                 ('https://www.walmart.com.mx'),
                                 ('https://www.liverpool.com.mx/'),
                                 ('not_an_url'),
                                 ('not an url.com'),
                             ]
                             )
    def test_add_url_to_db(self, url):
        # session = create_session
        add_url_to_db(url)
        if (url.endswith('/')):
            url = url[:-1]
        try:
            web = None
            web = self.session.query(Web).filter_by(url=url).one()
        except NoResultFound:
            assert(not web)
        else:
            assert(url == web.url)

    def test_input_today_at_db(self):
        today = date.today()
        return_today = input_today_at_db()
        today_db = self.session.query(Date).filter_by(dated=today).one()
        assert(today == today_db.dated and today == return_today)


@pytest.mark.usefixtures("create_session", "fill_database")
class Test_Pre_Loaded_DB:
    def test_get_new_url(self):
        print("three")
        self.session.add(Date(dated='2020-05-11'))
        today = input_today_at_db()
        get_new_url(today)
        assert(False)
