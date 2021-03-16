import requests
from requests_html import HTML
from urllib.parse import urlparse
from urllib.parse import urljoin
from datetime import date, timedelta
import time

from sqlalchemy import create_engine, Column, ForeignKey, Integer, Text, Date
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import func

Base = declarative_base()
metadata = Base.metadata


class Page(Base):
    __tablename__ = 'Pages'

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, sqlite_on_conflict_unique='IGNORE')
    html = Column(Text)
    web_id = Column(ForeignKey('Webs.id'))
    date_id = Column(ForeignKey('Dates.id'))
    interest = Column(Integer)
    error = Column(Integer)

    dater = relationship('Date')
    webr = relationship('Web')

    def __repr__(self):
        return f"<Pages url={self.url}>"


class Date(Base):
    __tablename__ = 'Dates'

    id = Column(Integer, primary_key=True)
    dated = Column(Date, unique=True, sqlite_on_conflict_unique='IGNORE')

    def __repr__(self):
        return f"<Dates date={self.dated}>"


class Web(Base):
    __tablename__ = 'Webs'

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, sqlite_on_conflict_unique='IGNORE')

    def __repr__(self):
        return f"<Webs url={self.url}>"


engine = create_engine("sqlite:///test.db", echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

# https://alsuper.com/
# https://www.walmart.com.mx/
# https://www.liverpool.com.mx/
# https://www.soriana.com/

# Input new pages
while True:
    start_url = input('Enter a new web url or enter to crawl: ')
    # reset data base
    if start_url == "reset":
        Base.metadata.drop_all(engine)
        print("Tables deleted")
        Base.metadata.create_all(engine)
        print("Tables created")
        continue
    # pass if pressed enter and has at least http
    if start_url != '' and not start_url.startswith('http'):
        print('Please enter a valid url or click enter to continue. \n')
        continue
    if start_url:
        if (start_url.endswith('/')):
            start_url = start_url[:-1]
        print("Adding to databases: ", start_url)
        Web(url=start_url)
        session.add(Web(url=start_url))
        session.commit()
        web = session.query(Web).filter_by(url=start_url).one()
        session.add(Page(url=start_url, web_id=web.id))
        session.commit()
        print("\nActual Webs: ")
        qs = session.query(Web).all()
        print(qs, "\n")
        continue

    qs = session.query(Web).all()
    if not qs:
        print("Found No Webs to crawl. Please, try Again.\n")
    else:
        break

# Crawler
print("\nStarting crawl on:")
# Get the current webs
qs = session.query(Web).all()
print(qs, "\n")

# webs = list()
# for row in cur:
#     webs.append(str(row[0]))
# print(webs, "\n")

pgs_cnt = 0
today = date.today()
print("Today: ", today)
session.add(Date(dated=today))
session.commit()
start = time.time()
while True:

    if today != date.today():
        today = date.today()
        print("Now today is: ", today)
        session.add(Date(dated=today))
        session.commit()
    try:
        qs = session.query(Page).filter((Page.dater.has(Date.dated < today)) | (
            Page.date_id == None), (Page.interest == None) | (Page.interest > 0), (Page.error == None)).order_by(func.random()).limit(1).all()[0]
        print(qs)  # TOBEDELETED
    except IndexError:
        print('No unretrieved HTML pages found')
        break
    print("Fetching:")
    print(qs.id, qs.url, end=' ')
    print("AQUI")
    input()
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        document = requests.get(url, headers=headers)
        pg_inf = {}
        pg_inf["status_code"] = document.status_code

        if pg_inf["status_code"] != 200:
            print("\nError on page: ", document.status_code)
            cur.execute('UPDATE Pages SET error=? WHERE url=?',
                        (pg_inf["status_code"], url))

        pg_inf["content_type"] = document.headers["content-type"][:9]

        if pg_inf["content_type"] != 'text/html':
            print("Ignore non text/html page")
            cur.execute(
                'UPDATE Pages SET interest=?, date_id = (SELECT id FROM Dates WHERE date = ?) WHERE url=?', (0, today, url))
            conn.commit()
            continue

        pg_inf["html"] = document.text
        # imprime longitud del html
        print('('+str(len(pg_inf["html"]))+')', end=' ')
        r_html = HTML(html=pg_inf["html"])

    except KeyboardInterrupt:
        print('\n\nProgram interrupted by user...')
        break
    except:
        print("Unable to retrieve or parse page")
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ))
        conn.commit()
        continue

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
