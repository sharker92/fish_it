import requests
from requests_html import HTML
from urllib.parse import urlparse
from urllib.parse import urljoin
from datetime import date, timedelta
import time
import logging
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
from sqlalchemy.sql.expression import func
from src.databases.page import Page
from src.databases.web import Web
from src.databases.date import Date
from src.databases.base import session_factory, reset_db
# https://alsuper.com/
# https://www.walmart.com.mx/
# https://www.liverpool.com.mx/
# https://www.soriana.com/

logger = logging.getLogger(__name__)
# format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# logging.basicConfig(filename='{}.log'
#                     .format(date.today()),
#                     filemode='w',
#                     format=format)


def add_url_to_db(url):
    """Validate website into valid URL"""
    session = session_factory()
    try:
        # pass if pressed enter and has at least http
        validate = URLValidator()
        validate(url)
    except ValidationError:
        print("{} not a valid url.".format(url))
        print('Please enter a valid url or click enter to continue. \n')
    else:
        if (url.endswith('/')):
            url = url[:-1]
        print("Adding to databases: ", url)
        Web(url=url)
        session.add(Web(url=url))
        session.commit()
        web = session.query(Web).filter_by(url=url).one()
        session.add(Page(url=url, web_id=web.id))
        session.commit()
        print("\nActual Webs: ")
        qs = session.query(Web).all()
        print(qs, "\n")
    session.close()


def input_today_at_db():
    session = session_factory()
    today = date.today()
    print("Today is: ", today)
    session.add(Date(dated=today))
    session.commit()
    session.close()
    return today


def get_new_url(today):
    session = session_factory()
    try:
        qs = session.query(Page).filter(
            (Page.date_r.has(Date.dated < today)) | (Page.date_id.is_(None)),
            (Page.interest.is_(None)) | (Page.interest > 0),
            (Page.error.is_(None))).order_by(func.random()).limit(1).all()[0]
    except IndexError:
        qs = None
        print('No unretrieved HTML pages found')
    session.close()
    return qs


def get_html(qs):
    session = session_factory()
    document = None
    try:
        header = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        headers = {
            'User-Agent': header}
        document = requests.get(qs.url, headers=headers)
    except Exception:
        logging.exception(
            'Unable to retrieve or parse page: {}\n\n'.format(qs))
        qs.error = -1
        session.commit()
    session.close()
    return document


def parse_html(qs, document):
    session = session_factory()
    pg_inf = {}
    pg_inf["status_code"] = document.status_code

    if pg_inf["status_code"] != 200:
        print("\nError on page: ", document.status_code)
        qs.error = pg_inf["status_code"]
        session.commit()
        result = "continue"

    pg_inf["content_type"] = document.headers["content-type"][:9]

    if pg_inf["content_type"] != 'text/html':
        print("Ignore non text/html page")
        qs.interest = 0
        qs.date_r = today
        session.commit()
        result = "continue"

    pg_inf["html"] = document.text
    # imprime longitud del html
    print('('+str(len(pg_inf["html"]))+')', end=' ')
    r_html = HTML(html=pg_inf["html"])


def crawl_pages():

    # Base = declarative_base()
    # metadata = Base.metadata

    # CLASSES

    # engine = create_engine("sqlite:///test.db", echo=False)
    # metadata.create_all(bind=engine)
    # Session = sessionmaker(engine)
    # session = Session()
    session = session_factory()

    # Input new pages or resert
    while True:
        start_url = input('Enter a new web url or enter to crawl: ')
        # reset data base
        if start_url:
            if start_url == "reset":
                reset_db()
            else:
                add_url_to_db(start_url)
            continue

        qs = session.query(Web).all()
        if not qs:
            print("Found No Webs to crawl. Please, try Again.\n")
        else:
            break

    # Crawler
    print("\nStarting crawl on:")
    qs = session.query(Web).all()
    print(qs, "\n")

    # webs = list()
    # for row in cur:
    #     webs.append(str(row[0]))
    # print(webs, "\n")

    pgs_cnt = 0
    today = input_today_at_db()
    start = time.time()
    while True:
        if today != date.today():
            today = input_today_at_db()
        qs = get_new_url(today)
        if not qs:
            break
        print("Fetching:")
        print(qs.id, qs.url, end=' ')
        document = get_html(qs)
        if not document:
            continue
        pg_inf = parse_html(qs, document)
        if not pg_inf:
            continue
# TERMINAR aquí
# REVISAR LO DE Las Sesiones
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            document = requests.get(qs.url, headers=headers)
            pg_inf = {}
            pg_inf["status_code"] = document.status_code

            if pg_inf["status_code"] != 200:
                print("\nError on page: ", document.status_code)
                qs.error = pg_inf["status_code"]
                session.commit()
                continue

            pg_inf["content_type"] = document.headers["content-type"][:9]

            if pg_inf["content_type"] != 'text/html':
                print("Ignore non text/html page")
                qs.interest = 0
                qs.date_r = today
                session.commit()
                continue

            pg_inf["html"] = document.text
            # imprime longitud del html
            print('('+str(len(pg_inf["html"]))+')', end=' ')
            r_html = HTML(html=pg_inf["html"])
        except KeyboardInterrupt:
            print('\n\nProgram interrupted by user...')
            break
        except Exception:
            logging.exception(
                'Unable to retrieve or parse page: {}\n'.format(qs))
            print()
            qs.error = -1
            session.commit()
            continue
        print("AQUI")
        input()
        cur.execute('SELECT html, interest FROM Pages WHERE url=?', (url,))
        old_html, interest = cur.fetchone()
        if old_html == pg_inf['html']:
            cur.execute(
                'UPDATE Pages SET date_id = (SELECT id FROM Dates WHERE date = ?) WHERE url=?', (today, url))
        else:
            cur.execute('UPDATE Pages SET html=?, date_id = (SELECT id FROM Dates WHERE date = ?) WHERE url=?',
                        (memoryview(bytes(pg_inf["html"], encoding='utf-8')), today, url))
            if interest == 2:
                cur.execute(
                    'UPDATE Pages SET interest=1 WHERE url=?', (today, url))
        conn.commit()

        # Retrieve all of the href links on anchor tags
        href_list = r_html.xpath('//a/@href')
        count = 0
        for href in href_list:
            if (href is None):
                continue
            # Resolve relative references like href="/contact"
            phref = urlparse(href)
            if (len(phref.scheme) < 1):
                href = urljoin(url, href)
            ipos = href.find('#')
            if (ipos > 1):
                href = href[:ipos]
            if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')):
                continue
            if (href.endswith('/')):
                href = href[:-1]
            if (len(href) < 1):
                continue
            # Check if the URL is in any of the webs (web delimiter)
            found = False
            for web in webs:
                if (href.startswith(web)):
                    cur.execute(
                        'UPDATE Pages SET web_id = (SELECT id From Webs WHERE Webs.url = ?) WHERE url = ?', (web, href))
                    found = True
                    break
            if not found:
                continue

            cur.execute(
                'INSERT OR IGNORE INTO Pages (url, html ) VALUES ( ?, NULL )', (href, ))
            count = count + 1
            conn.commit()

        if count % 50 == 0:
            conn.commit()
        if count % 100 == 0:
            time.sleep(1)
        print(count)
        pgs_cnt += 1

    cur.close()
    end = time.time()
    total_time = timedelta(seconds=int(end - start))
    final_msg = "Retrieved {} pages on {} seconds".format(pgs_cnt, total_time)
    print(final_msg)
