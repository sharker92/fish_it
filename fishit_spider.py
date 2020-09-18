import sqlite3
import requests
import datetime


def deleteTables():
    cur.executescript('''
    DROP TABLE IF EXISTS Pages;
    DROP TABLE IF EXISTS Webs;
    ''')


def createTables():
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT, date TEXT, error INTEGER);

    CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE);
    ''')


conn = sqlite3.connect('./fishit_spider.sqlite')
cur = conn.cursor()
createTables()
# https://alsuper.com/


while True:
    start_url = input('Enter new web url or enter to crawl: ')
    if start_url == "reset":
        deleteTables()
        print("Tables deleted")
        createTables()
        print("Tables created")
    elif start_url:
        if (start_url.endswith('/')):
            start_url = start_url[:-1]
        web = start_url
        print(web)
        if (len(web) > 1):
            cur.execute(
                'INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web, ))
            cur.execute(
                'INSERT OR IGNORE INTO Pages (url, html) VALUES ( ?, NULL)', (start_url, ))
            conn.commit()
    else:
        cur.execute(
            'SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
        row = cur.fetchone()
        if row is None:
            print("No Webs founded to crawl.\nPlease, try Again.")
        else:
            break
        print(type(row))
        print(row)
