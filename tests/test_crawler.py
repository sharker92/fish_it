import pytest
from sqlalchemy.orm.exc import NoResultFound
from datetime import date
from src.crawler.crawler import (
    add_url_to_db, input_today_at_db, get_crawlable_urls,
    get_html, extract_html_content)
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


@pytest.fixture(scope="class")
def fill_database(request):

    session = request.cls.session
    old_date_orm = Date(dated=date.fromisoformat('2020-05-11'))
    session.add(old_date_orm)
    today_date = input_today_at_db()
    request.cls.today_date = today_date
    today_date_orm = session.query(Date).filter_by(dated=today_date).one()
    data = list()
    pass_url = 'pass_'
    dont_pass_url = 'wont_'
    end_url = ".com"
    for d in range(3):
        for i in range(3):
            for e in range(2):
                url = 'https://www.'
                if d == 0:
                    dated = today_date_orm
                    url += dont_pass_url
                elif d == 1:
                    dated = old_date_orm
                    url += pass_url
                elif d == 2:
                    dated = None
                    url += pass_url
                if i == 0:
                    interest = 0
                    url += dont_pass_url
                elif i == 1:
                    interest = 1
                    url += pass_url
                elif i == 2:
                    interest = None
                    url += pass_url
                if e == 0:
                    error = 0
                    url += dont_pass_url
                elif e == 1:
                    error = None
                    url += pass_url

                url += str(d) + str(i) + str(e) + end_url
                datum = {
                    'url': url,
                    'dated': dated,
                    'interest': interest,
                    'error': error,
                }
                data.append(datum)

    for datum in data:
        page = Page(
            url=datum['url'],
            interest=datum['interest'],
            error=datum['error'],
            date_r=datum['dated']
        )
        session.add(page)
    session.commit()

    test_url = ['https://alsuper.com/',
                'https://www.walmart.com.mx', 'https://www.liverpool.com.mx/']

    for url in test_url:
        page = Page(url=url,)
        session.add(page)
    session.commit()


@ pytest.mark.usefixtures("create_session")
class Test_Basic_DB:
    @ pytest.mark.parametrize('url',
                              [
                                  ('https://alsuper.com/'),
                                  ('https://www.walmart.com.mx'),
                                  ('https://www.liverpool.com.mx/'),
                                  ('not_an_url'),
                                  ('not an url.com'),
                              ]
                              )
    def test_add_url_to_db(self, url):
        add_url_to_db(url)
        if (url.endswith('/')):
            url = url[:-1]
        try:
            web_orm = None
            web_orm = self.session.query(Web).filter_by(url=url).one()
        except NoResultFound:
            assert not web_orm, 'Web not found.'
        try:
            page_orm = None
            page_orm = self.session.query(Page).filter_by(url=url).one()
        except NoResultFound:
            assert not page_orm, 'Page not found.'
        else:
            assert (url == web_orm.url and
                    url == page_orm.url and
                    web_orm.id == page_orm.web_id), 'Problems adding url to db.'

    def test_input_today_at_db(self):
        today_date = date.today()
        rtrn_today_date = input_today_at_db()
        today_date_orm = self.session.query(Date).filter_by(
            dated=today_date).one()
        assert (today_date == today_date_orm.dated and
                today_date == rtrn_today_date), 'Mismatch at today dates.'


@ pytest.mark.usefixtures("create_session", "fill_database")
class Test_Pre_Loaded_DB:
    def test_get_crawlable_urls(self):
        crawlable_urls = get_crawlable_urls(self.today_date)
        for crwlbl_url in crawlable_urls:
            if 'wont' in crwlbl_url.url:
                # escribir en assert y cambiar nombres de variables
                assert False, 'There is an invalid url.'
        assert len(crawlable_urls) == 4, 'Found more url than the expected.'

    @ pytest.mark.parametrize('url',
                              [
                                  ('https://alsuper.com'),
                                  ('https://www.wont_wont_wont_000.com'),
                              ]
                              )
    def test_get_html(_, url):
        response = get_html(url)
        if response:
            assert(response.status_code == 200)
        else:
            assert(not response)

    @ pytest.mark.parametrize('url,test_data',
                              [
                                  ('https://alsuper.com/', None),
                                  ('https://www.walmart.com.mx',
                                   (404, 'text/html; charset=utf-8')),
                                  ('https://www.liverpool.com.mx/',
                                   (200, 'image/png; charset=utf-8')),
                              ]
                              )
    def test_extract_html_content(self, url, test_data):
        response = get_html(url)
        if test_data:
            response.status_code = test_data[0]
            response.headers["content-type"] = test_data[1]
        pg_inf = extract_html_content(url, response, self.today_date)
        if pg_inf:
            assert(pg_inf["status_code"] == 200)
            assert(pg_inf["content_type"] == 'text/html')
            assert(pg_inf["html"])
        else:
            assert(not pg_inf)
