import sqlite3


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


while True:
    start_url = input('Enter a new web url or enter to crawl: ')
    if start_url == "reset":
        deleteTables()
        print("Tables deleted")
        createTables()
        print("Tables created")
    elif start_url:
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
            'SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')  # revisar si es así o mejor en webs
        row = cur.fetchone()
        if row is None:
            print("No Webs founded to crawl.\nPlease, try Again.")
        else:
            break
        print(type(row))
        print(row)
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
