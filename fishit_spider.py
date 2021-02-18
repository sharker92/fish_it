import sqlite3
import requests
from requests_html import HTML
from urllib.parse import urlparse
from urllib.parse import urljoin
from datetime import date, timedelta
import time
# UPDATE Pages SET date_id = 12 WHERE date_id = 2


def deleteTables():
    cur.executescript('''
    DROP TABLE IF EXISTS Pages;
    DROP TABLE IF EXISTS Dates;
    DROP TABLE IF EXISTS Webs;
    ''')


def createTables():
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Pages (
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE,
        html TEXT DEFAULT NULL,
        web_id INTEGER DEFAULT NULL,
        date_id INTEGER DEFAULT NULL,
        interest INTEGER DEFAULT NULL,
        error INTEGER DEFAULT NULL,
        FOREIGN KEY(web_id) REFERENCES Webs(id),
        FOREIGN KEY(date_id) REFERENCES Dates(id)
        );

    CREATE TABLE IF NOT EXISTS Dates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Webs (
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE
    );
    ''')


conn = sqlite3.connect('./fishit_spider.sqlite')
cur = conn.cursor()
createTables()
# https://alsuper.com/
# https://www.walmart.com.mx/
# https://www.liverpool.com.mx/

# Input new pages
while True:
    start_url = input('Enter a new web url or enter to crawl: ')
    # reset data base
    if start_url == "reset":
        deleteTables()
        print("Tables deleted")
        createTables()
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
        cur.execute(
            'INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (start_url, ))
        cur.execute(
            'INSERT OR IGNORE INTO Pages (url) VALUES ( ? )', (start_url, ))
        cur.execute(
            'UPDATE Pages SET web_id = (SELECT id From Webs WHERE Webs.url = ?) WHERE url = ?', (start_url, start_url))

        conn.commit()
        print("\nActual Webs: ")
        cur.execute('''SELECT url FROM Webs''')
        print(cur.fetchall(), "\n")
        continue

    cur.execute('''SELECT url FROM Webs''')
    webs_row = cur.fetchone()
    if webs_row is None:
        print("Found No Webs to crawl. Please, try Again.\n")
    else:
        break

# Crawler
print("\nStarting crawl on:")
# Get the current webs
cur.execute('''SELECT url FROM Webs''')
webs = list()
for row in cur:
    webs.append(str(row[0]))
print(webs, "\n")

pgs_cnt = 0
today = date.today()
print("Today: ", today)
cur.execute('INSERT OR IGNORE INTO Dates (date) VALUES ( ? )', (today,))
conn.commit()
start = time.time()
while True:

    if today != date.today():
        today = date.today()
        print(today)
        cur.execute('INSERT OR IGNORE INTO Dates (date) VALUES ( ? )', (today,))
        conn.commit()

    cur.execute(
        'SELECT Pages.id, Pages.url FROM Pages WHERE (date_id is NULL or (SELECT date FROM Dates WHERE Dates.id = date_id) < ?) and (interest is NULL or interest > 0) and error is NULL ORDER BY RANDOM() LIMIT 1', (today,))
    try:
        fromid, url = cur.fetchone()
    except:
        print('No unretrieved HTML pages found')
        break
    print("Fetching:")
    print(fromid, url, end=' ')
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
