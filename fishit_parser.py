import os
import pandas as pd
from requests_html import HTML
import sqlite3

BASE_DIR = os.path.dirname(__file__)


def createTables(cur):
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price TEXT DEFAULT NULL,
        date_id INTEGER DEFAULT NULL,
        FOREIGN KEY(date_id) REFERENCES Dates(id)
        );
    CREATE TABLE IF NOT EXISTS Dates (
        id INTEGER PRIMARY KEY,
        date TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS Webs (
        url TEXT UNIQUE
    );
    ''')


conn = sqlite3.connect('./fishit_spider.sqlite')
cur = conn.cursor()
new_conn = sqlite3.connect('./fishit_products.sqlite')
new_cur = new_conn.cursor()

createTables(new_conn)
while True:
    # Interest 1 = Interested, not scraped.
    # Interest 2 = Interested, scraped since last html update.
    cur.execute(
        'SELECT id, url, html FROM Pages WHERE (interest is NULL or 1) and error is NULL ORDER BY RANDOM() LIMIT 1')
    id, url, html = cur.fetchone()

    # TODO buscar precios, si no hay, marcarlo como pagina sin interés.
    # filtrar por ubicación (location)
    # <ul class="row list-unstyled products--list">
    # <li class="col-xs-6 col-sm-3 col-md-3 home--product-item" data-product-id="441494" data-favorite="0">
    # <div class="product-item--desc">
    # <h5 class="product-item--title">
    # <h4 class="product-item--price">
    r_html = HTML(html=html)
    ul = r_html.find(".row.list-unstyled")
    # ul = r_html.xpath("//ul[@class = 'products--list']")
    print(url)
    print(ul)

    if len(ul) != 1:
        print("Error, found more <ul> than expected")
        continue
    # print(ul[0].text)
    li_list = ul[0].find("li")

    print(f"Found {len(li_list)} items.")
    table_data = []
    for li in li_list:
        prod_data = {}
        # print(li)
        # print(li.full_text)
        prod_data['prod_id'] = li.attrs["data-product-id"]
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
    df.to_csv(filepath, index=True)
    break
