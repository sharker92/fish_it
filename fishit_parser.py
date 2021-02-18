import sqlite3
from requests_html import HTML
from datetime import timedelta
import time


def createTables():
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Products (
        id INTEGER PRIMARY KEY,
        web_id INTEGER DEFAULT NULL,
        url_id INTEGER DEFAULT NULL,
        date_id INTEGER DEFAULT NULL,
        id_prod INTEGER DEFAULT NULL,
        prod_name TEXT DEFAULT NULL,
        prod_price INTEGER DEFAULT NULL,
        data_fav TEXT DEFAULT NULL,
        FOREIGN KEY(web_id) REFERENCES Webs(id),
        FOREIGN KEY(url_id) REFERENCES Urls(id),
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
    CREATE TABLE IF NOT EXISTS Urls (
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE
        );
    ''')


conn = sqlite3.connect('./fishit_products.sqlite')
cur = conn.cursor()
createTables()

crawled_database = './fishit_spider.sqlite'
cur.execute("ATTACH ? as Crawled", (crawled_database,))

cur.execute('INSERT or IGNORE INTO Dates SELECT * from Crawled.Dates')
cur.execute('INSERT or IGNORE INTO Webs SELECT * from Crawled.Webs')
conn.commit()
prod_cnt = 0
url_cnt = 0
start = time.time()
while True:
    # Interest -2 = Unable to find products.
    # Interest -1 = Found more <ul> than expected.
    # Interest 0 = Page with no prices, not interested.
    # Interest 1 = Interested, not scraped.
    # Interest 2 = Interested, scraped since last html update.
    # Error -1 = Unable to retrieve or parse page [CRAWLER].
    # Error -2 = Unable to retrieve or parse page [PARSER].
    cur.execute(
        'SELECT id, url, html, web_id, date_id FROM Crawled.Pages WHERE (interest is NULL or interest == 1) and html IS NOT NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        id, url, html, web_id, date_id = cur.fetchone()
    except:
        print('No unretrieved HTML pages found')
        break
    print("Fetching:")
    print(id, url, end=' ')

    # Parsing data
    try:
        r_html = HTML(html=html)
        ul = r_html.find(".row.list-unstyled")
    except KeyboardInterrupt:
        print('\n\nProgram interrupted by user...')
        break
    except:
        print("\nUnable to parse page\n")
        cur.execute('UPDATE Crawled.Pages SET error=-2 WHERE url=?', (url, ))
        conn.commit()
        continue

    if len(ul) == 0:
        cur.execute('UPDATE Crawled.Pages SET interest=0 WHERE url=?', (url,))
        conn.commit()
        print("\nPage without interest.")
        continue
    if len(ul) != 1:
        cur.execute('UPDATE Crawled.Pages SET interest=-1 WHERE url=?', (url,))
        conn.commit()
        print("\nFound more <ul> than expected")
        continue

    # Products extraction
    li_list = ul[0].find("li")
    print(f": Found {len(li_list)} items.")
    for li in li_list:
        prod_data = {}
        try:
            prod_data['id_prod'] = li.attrs["data-product-id"]
            prod_data['prod_name'] = li.find(".product-item--title p")[0].text
            prod_data['prod_price'] = li.find(".product-item--price b")[0].text
            prod_data['data_fav'] = li.attrs["data-favorite"]
        except:
            print("\nUnable to find products or error extracting one.\n")
            cur.execute(
                'UPDATE Crawled.Pages SET interest=-2 WHERE url=?', (url, ))
            conn.commit()
            break

        cur.execute(
            'INSERT OR IGNORE INTO Products (web_id, url_id, date_id, id_prod, prod_name, prod_price, data_fav) VALUES (?,(SELECT id FROM Urls WHERE url=?),?,?,?,?,?)', (web_id, url, date_id, prod_data['id_prod'], prod_data['prod_name'], prod_data['prod_price'], prod_data['data_fav']))
        prod_cnt += 1

    if len(prod_data) > 0:
        cur.execute('INSERT OR IGNORE INTO Urls (url) VALUES ( ? )', (url, ))
        # Update crawled database
        url_cnt += 1
        cur.execute('UPDATE Crawled.Pages SET interest=2 WHERE url=?', (url,))
        conn.commit()

cur.execute('DETACH DATABASE Crawled')
cur.close()
end = time.time()
total_time = timedelta(seconds=int(end - start))
final_msg = "Retrieved {} products from {} pages on {} seconds".format(
    prod_cnt, url_cnt, total_time)
print(final_msg)
