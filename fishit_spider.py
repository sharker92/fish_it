import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.parse import urljoin
from datetime import date
import time


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
        FOREIGN KEY(date_id) REFERENCES Dates(id),
        FOREIGN KEY(web_id) REFERENCES Webs(id)
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
            'UPDATE Pages SET web_id = Webs.id From Webs WHERE Pages.url = Webs.url')

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
# ME QUEDE EN REVISAR TODO EL CRAWL.
# TO DO Falta agregar web_id a Pages y referenciarlo con la tabla de webs
# Crawler
print("\nStarting crawl on:")
cur.execute('''SELECT url FROM Webs''')
print(cur.fetchall(), "\n")

many_pgs = 0
pgs_cnt = 0
while True:

    today = date.today()
    print(today)
    cur.execute('INSERT OR IGNORE INTO Dates (date) VALUES ( ? )', (today,))
    conn.commit()
    # cur.execute('SELECT id,date From Dates WHERE date == ?', (today,))
    # row = cur.fetchone()
    # print(row)
    # today_id = row[0]
    #current_date = datetime.strptime(row[1], "%Y-%m-%d").date()
    cur.execute('SELECT Pages.id, Pages.url FROM Pages WHERE (date_id is NULL or (SELECT date FROM Dates WHERE Dates.id = date_id < ?)) and (interest is NULL or 1) and error is NULL ORDER BY RANDOM() LIMIT 1', (today,))
###
    try:
        row = cur.fetchone()
        # print("Fetching: ", row)
        fromid = row[0]
        url = row[1]
    except:
        print('No unretrieved HTML pages found')
        # many_pgs = 0 creo ya no lo necesito
        break
    print("Fetching:")
    print(fromid, url, end=' ')
    try:
        document = requests.get(url)

        status_code = document.status_code

        if status_code != 200:
            print("Error on page: ", document.status_code)
            cur.execute('UPDATE Pages SET error=? WHERE url=?',
                        (status_code, url))

        content_type = document.headers["content-type"][:9]
        # print(document.headers["content-type"])
        if content_type != 'text/html':
            print("Ignore non text/html page")
            cur.execute('UPDATE Pages SET interest=? WHERE url=?', (0, url))
            conn.commit()
            continue

        html = document.text
        print('('+str(len(html))+')', end=' ')  # imprime longitud del html

        soup = BeautifulSoup(html, "html.parser")
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    except:
        print("Unable to retrieve or parse page")
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ))
        conn.commit()
        continue
    # cur.execute('INSERT OR IGNORE INTO Pages (url, html) VALUES ( ?, NULL)', (url, )) #necesario¿? #AQUI es posible que sí
    #   cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
    #   cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url ) )
    cur.execute('UPDATE Pages SET html=?, date_id = (SELECT id FROM Dates WHERE date = ?) WHERE url=?',
                (memoryview(bytes(html, encoding='utf-8')), today, url))
    conn.commit()

    input("pause")
    # Retrieve all of the anchor tags
    tags = soup.find_all('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        # print()
        # print(tag)
        # print("href: ", href)
        # input()

        if (href is None):
            continue
        # Resolve relative references like href="/contact"
        up = urlparse(href)
        if (len(up.scheme) < 1):
            href = urljoin(url, href)
        ipos = href.find('#')
        if (ipos > 1):
            href = href[:ipos]

        # print("#href: ", href)
        # input()

        if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')):
            continue
        if (href.endswith('/')):
            href = href[:-1]
        if (len(href) < 1):
            continue

        # Check if the URL is in any of the webs
        found = False
        for web in webs:
            if (href.startswith(web)):
                found = True
                break
        if not found:
            continue

        cur.execute(
            'INSERT OR IGNORE INTO Pages (url, html ) VALUES ( ?, NULL )', (href, ))
        count = count + 1
        conn.commit()

        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (href, ))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print('Could not retrieve id')
            continue
        # print fromid, toid
        # cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))

    if count % 50 == 0:
        conn.commit()
    if count % 100 == 0:
        time.sleep(1)
    print(count)

cur.close()


# que pasa cuando ya este trepado y necesite retrepar porque es otro día
# TODO Revisar los de las fechas, no necesita ser NULL el html
#cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL and (interest is NULL or interest is  1) ORDER BY RANDOM() LIMIT 1')


# location
# me esta metiendo el interest como 0 por defecto

# tipos de contenido
# application/pdf
#text/html; charset=utf-8
