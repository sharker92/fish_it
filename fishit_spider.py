import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.parse import urljoin


def deleteTables():
    cur.executescript('''
    DROP TABLE IF EXISTS Pages;
    DROP TABLE IF EXISTS Webs;
    ''')


def createTables():
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Pages (
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE,
        html TEXT DEFAULT NULL,
        date TEXT DEFAULT NULL,
        interest INTEGER DEFAULT NULL,
        error INTEGER DEFAULT NULL
        );

    CREATE TABLE IF NOT EXISTS Webs (
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
    if start_url == "reset":
        deleteTables()
        print("Tables deleted")
        createTables()
        print("Tables created")
    if start_url != '' and not start_url.startswith('http'):
        print('Please enter a valid url or click enter to continue. ')
        continue
    if start_url:
        if (start_url.endswith('/')):
            start_url = start_url[:-1]
        web = start_url
        print("Adding to databases: ", web)
        if (len(web) > 1):
            cur.execute(
                'INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web, ))
            cur.execute(
                'INSERT OR IGNORE INTO Pages (url) VALUES ( ? )', (web, ))
            conn.commit()
    else:
        cur.execute(
            'SELECT id,url FROM Pages WHERE html is NULL and interest is NULL or 1 and error is NULL ORDER BY RANDOM() LIMIT 1')  # revisar si es así o mejor en webs
        row = cur.fetchone()
        if row is None:
            print("No Webs founded to crawl.\nPlease, try Again.")
        else:
            break
        print(type(row))
        print(row)

# Crawl on current pages
print("Starting crawl on:")
cur.execute('''SELECT url FROM Webs''')
webs = list()
for row in cur:
    webs.append(str(row[0]))
print(webs)
many_pgs = 0
while True:
    if (many_pgs < 1):
        sval = input('How many pages:')
        if (len(sval) < 1):
            break
        many_pgs = int(sval)
    many_pgs -= 1
    cur.execute(
        'SELECT id,url FROM Pages WHERE html is NULL and interest is NULL or 1 and error is NULL ORDER BY RANDOM() LIMIT 1')  # Date < Date Now
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

        if 'text/html' != document.headers["content-type"][:9]:
            print("Ignore non text/html page")
            cur.execute('UPDATE Pages SET interest=?', (0, ))
            # cur.execute('DELETE FROM Pages WHERE url=?', (url, )) CODIGO ORIGINAL
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
    cur.execute('UPDATE Pages SET html=? WHERE url=?',
                (memoryview(bytes(html, encoding='utf-8')), url))
    conn.commit()

    # Retrieve all of the anchor tags
    tags = soup.find_all('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        print()
        print(tag)
        print("href: ", href)
        input()

        if (href is None):
            continue
        # Resolve relative references like href="/contact"
        up = urlparse(href)
        if (len(up.scheme) < 1):
            href = urljoin(url, href)
        ipos = href.find('#')
        if (ipos > 1):
            href = href[:ipos]

        print("#href: ", href)
        input()

        if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')):
            continue
        if (href.endswith('/')):
            href = href[:-1]
        # print href
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

    print(count)

cur.close()

# todo save date when html is fetched
# <a class = "newCity" href = "switchlocation?id_location=6" > Cd. Chihuahua < /a >
# href:  switchlocation?id_location = 6
# <a class="newCity" href="switchlocation?id_location=85">Saltillo</a>
# href:  switchlocation?id_location=85
