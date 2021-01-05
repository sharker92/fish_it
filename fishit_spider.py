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
        date_id INTEGER DEFAULT NULL,
        interest INTEGER DEFAULT NULL,
        error INTEGER DEFAULT NULL,
        web_id INTEGER DEFAULT NULL,
        FOREIGN KEY(date_id) REFERENCES Dates(id)
        FOREIGN KEY(web_id) REFERENCES Webs(id)
        );

    CREATE TABLE IF NOT EXISTS Dates (
        id INTEGER PRIMARY KEY,
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
        print('Please enter a valid url or click enter to continue. ')
        continue
    if start_url:
        if (start_url.endswith('/')):
            start_url = start_url[:-1]
        web = start_url
        print("Adding to databases: ", web)
        cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web, ))
        cur.execute('INSERT OR IGNORE INTO Pages (url) VALUES ( ? )', (web, ))
        cur.execute(
            'UPDATE Pages SET web_id = Webs.id From Webs WHERE Pages.url = Webs.url')
        # TO DO Falta agregar web_id a Pages y referenciarlo con la tabla de webs
        # AQUI ME QUEDE
        # https://stackoverflow.com/questions/3845718/update-table-values-from-another-table-with-the-same-user-name/63079219#63079219
        conn.commit()
    else:
        # que pasa cuando ya este trepado y necesite retrepar porque es otro día
        today = date.today()
        cur.execute('INSERT OR IGNORE INTO Dates (date) VALUES ( ? )', (today,))
        conn.commit()
        # TODO Revisar los de las fechas, no necesita ser NULL el html
        cur.execute(
            'SELECT id,url FROM Pages WHERE html is NULL and error is NULL and (interest is NULL or interest is  1) ORDER BY RANDOM() LIMIT 1')

    row = cur.fetchone()
    if row is None:
        print("Found No Webs to crawl. Please, try Again.")
    else:
        break
    print(type(row))
    print(row)
# ME QUEDE EN REVISAR TODO EL CRAWL
# Crawl on current pages
print("Starting crawl on:")
cur.execute('''SELECT url FROM Webs''')
webs = list()
for row in cur:
    webs.append(str(row[0]))
print(webs)

many_pgs = 0
while True:
    # if (many_pgs < 1):
    #     sval = input('How many pages:')
    #     if (len(sval) < 1):
    #         break
    #     many_pgs = int(sval)
    # many_pgs -= 1
    ###
    today = date.today()
    cur.execute('INSERT OR IGNORE INTO Dates (date) VALUES ( ? )', (today,))
    conn.commit()
    cur.execute('SELECT id,date From Dates WHERE date == ?', (today,))
    row = cur.fetchone()
    print(row)
    today_id = row[0]

#current_date = datetime.strptime(row[1], "%Y-%m-%d").date()
    # Falta verificar si se ordenan los datos conforme los vamos metiendo. De ser así, el comparar las fechas por ID sería correcto
    # REVISAR HTML NO TIENE QUE SER NULL (LA FECHA MANDA)
    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and (interest is NULL or 1) and error is NULL and (date_id is NULL or date_id > ?)  ORDER BY RANDOM() LIMIT 1', (today_id,))

###
    try:
        row = cur.fetchone()
        # print("Fetching: ", row)
        fromid = row[0]
        url = row[1]
    except:
        print('No unretrieved HTML pages found')
        many_pgs = 0
        break
    print("Fetching:")
    print(fromid, url, end=' ')

    try:
        document = requests.get(url)

        html = document.text

        if document.status_code != 200:
            print("Error on page: ", document.status_code)
            cur.execute('UPDATE Pages SET error=? WHERE url=?',
                        (document.status_code, url))
        print(document.headers["content-type"])
        if 'text/html' != document.headers["content-type"][:9]:
            print("Ignore non text/html page")
            cur.execute('UPDATE Pages SET interest=?', (0, ))
            print("AAAAAAAAA")
            conn.commit()
            continue

        print('('+str(len(html))+')', end=' ')

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

    # cur.execute('INSERT OR IGNORE INTO Pages (url, html) VALUES ( ?, NULL)', (url, )) #necesario¿?
    cur.execute('UPDATE Pages SET html=?, date_id=? WHERE url=?',
                (memoryview(bytes(html, encoding='utf-8')), today_id, url))
    conn.commit()

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


# location
# me esta metiendo el interest como 0 por defecto

# tipos de contenido
# application/pdf
#text/html; charset=utf-8
