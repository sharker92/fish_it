import os
import pandas as pd
from requests_html import HTML
import sqlite3

BASE_DIR = os.path.dirname(__file__)


def createTables():
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Products (
        id INTEGER PRIMARY KEY,
        web_id INTEGER DEFAULT NULL,
        date_id INTEGER DEFAULT NULL,
        id_prod INTEGER DEFAULT NULL,
        prod_name TEXT,
        prod_price INTEGER DEFAULT NULL,
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


conn = sqlite3.connect('./fishit_products.sqlite')
cur = conn.cursor()
createTables()

crawled_database = './fishit_spider.sqlite'
cur.execute("ATTACH ? as Crawled", (crawled_database,))

cur.execute('INSERT or IGNORE INTO Dates SELECT * from Crawled.Dates')
cur.execute('INSERT or IGNORE INTO Webs SELECT * from Crawled.Webs')
conn.commit()

while True:
    # Interest 0 = Page with no prices, not interested.
    # Interest 1 = Interested, not scraped.
    # Interest 2 = Interested, scraped since last html update.
    # Error -1 = Unable to retrieve or parse page
    # Error -2 = found more <ul> than expected
    cur.execute(
        'SELECT id, url, html FROM Crawled.Pages WHERE (interest is NULL or 1) and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        id, url, html = cur.fetchone()
    except:
        print('No unretrieved HTML pages found')  # AQUI
        break
    print("Fetching:")
    print(id, url, end=' ')

    # TODO buscar precios, si no hay, marcarlo como pagina sin interés.
    # filtrar por ubicación (location)
    # Si la pagina que se trepo es igual de un día a otro, no es necesario trepar de nuevo [CRAWLER]

    r_html = HTML(html=html)
    ul = r_html.find(".row.list-unstyled")

    if len(ul) == 0:
        cur.execute('UPDATE Pages SET interest=0 WHERE url=?', (url,))
        conn.commit()
        print("Page without interest.")
        continue
    if len(ul) != 1:
        cur.execute('UPDATE Pages SET error=-2 WHERE url=?', (url,))
        conn.commit()
        print("Error, found more <ul> than expected")
        continue

    # print(ul[0].text)
    li_list = ul[0].find("li")

    print(f": Found {len(li_list)} items.")
    table_data = []
    for li in li_list:
        prod_data = {}
        # print(li)
        # print(li.full_text)
        prod_data['id_prod'] = li.attrs["data-product-id"]
        prod_data['prod_name'] = li.find(".product-item--title p")[0].text
        prod_data['prod_price'] = li.find(".product-item--price b")[0].text
        prod_data['data_fav'] = li.attrs["data-favorite"]
        # print(prod_data)
        table_data.append(prod_data)

    name = "alsuper"
    df = pd.DataFrame(table_data)
    path = os.path.join(BASE_DIR, 'data')
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join('data', f'{name}.csv')
    df.to_csv(filepath, index=False)
    break

cur.execute('DETACH DATABASE Crawled')
cur.close()
