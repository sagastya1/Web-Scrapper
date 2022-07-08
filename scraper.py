import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

sql = input('Enter name of new Database to create or press enter to continue: ')
if(len(sql) < 1):
    sql = 'scrap.sqlite'
conn = sqlite3.connect(sql)
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

c.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')

c.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

# Check to see if we are already in progress...
c.execute('SELECT id,url FROM Pages ORDER BY RANDOM() LIMIT 1')
row = c.fetchone()
if row is not None :
    check = input('Working with existing database. Press enter to continue or \n enter n to exit: ')
    if(check == 'n'):
        exit()

starturl = 'https://en.m.wikipedia.org/wiki/Computer_science'
if ( starturl.endswith('/') ) :
    starturl = starturl[:-1]
web = starturl
if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :
    pos = starturl.rfind('/')
    web = starturl[:pos]

if ( len(web) > 1 ) :
    c.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', ( web, ) )
    c.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0)', ( starturl, ) )
    conn.commit()


# Get the current webs
c.execute('''SELECT url FROM Webs''')
webs = list()
for row in c :
    webs.append(str(row[0]))
print(webs)

limit = 0
while True:
    c.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY id LIMIT 1')
    try:
        row = c.fetchone()
        fromid = row[0]
        url = row[1]
    except:
        print('No unretrieved HTML pages found')
        many = 0
        break

    print(fromid, url, end=' ')

    c.execute('DELETE from Links WHERE from_id=?', (fromid, ) )
    try:
        document = urlopen(url, context=ctx)

        html = document.read()
        if document.getcode() != 200 :
            print("Error on page: ",document.getcode())
            c.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url) )
            continue

        if 'text/html' != document.info().get_content_type() :
            print("Ignore non text/html page")
            c.execute('DELETE FROM Pages WHERE url=?', ( url, ) )
            conn.commit()
            continue

        print('Characters retrieved: '+'('+str(len(html))+')', end=' ')

        soup = BeautifulSoup(html, "html.parser")
        limit = 0
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    except:
        print("Unable to retrieve or parse page")
        c.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
        conn.commit()
        limit += 1
        if(limit >= 5):
            print('Something went wrong check your connection!')
            exit()
        continue

    c.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0)', ( url, ) )
    c.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url ) )
    conn.commit()

    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        if ( href is None ) :
            continue
        up = urlparse(href)
        if ( len(up.scheme) < 1 ) :
            href = urljoin(url, href)
        ipos = href.find('#')
        if ( ipos > 1 ) : href = href[:ipos]
        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) :
            continue
        if ( href.endswith('/') ) : href = href[:-1]
        if ( len(href) < 1 ) :
            continue

        found = False
        if ( href.startswith('https://en.m.wikipedia.org/wiki') ) :
            found = True
        if not found :
            continue

        c.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0)', ( href, ) )
        count = count + 1
        conn.commit()

        c.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
        try:
            row = c.fetchone()
            toid = row[0]
        except:
            print('Could not retrieve id')
            continue

        c.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) )

    
    print("Links retrieved:",count)

c.close()
