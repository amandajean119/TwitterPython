from bs4 import BeautifulSoup
import urllib3
import os
import psycopg2
from dotenv import load_dotenv
import sys
import traceback
import ast

#with open('potential_followers.csv')
#user_id bigint primary key, screen_name varchar default null, bot bool default null, suspended bool default null, deleted bool default null
#ids = ['789458325448581120', '83905639876409873219']

def find_suspended_followers():
    http = urllib3.PoolManager()
    to_check = set()
    try:
        load_dotenv('creds/.env')
        user = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASSWORD')
        conn_string = "dbname='twitter' user=" + user + " password = " + password
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
    except (psycopg2.DatabaseError) as e:
        print 'Error %s' % e
        traceback.print_exc()
        sys.exit(1)
    cur.execute('SELECT followers FROM name_id  WHERE followers IS NOT NULL')
    records = cur.fetchall()
    for record in records:
        record = ast.literal_eval(record[0])
        for id in record:
            bot = None
            suspended = False
            deleted = False
            crawled = False
            if id not in to_check:
                to_check.add(id)
                cur.execute('SELECT * FROM followers  WHERE user_id = (%s)', (str(id),))
                if not cur.fetchone():
                    cur.execute('SELECT * FROM name_id  WHERE user_id = (%s)', (str(id),))
                    if cur.fetchone():
                        bot = True
                    r = http.request('GET', 'https://twitter.com/intent/user?user_id=' + str(id))
                    soup = BeautifulSoup(r.data, 'html.parser')
                    p = soup.find("div", class_="flex-module error-page clearfix")
                    if p:
                        if p.h1.text == 'Account suspended':
                            suspended = True
                    else:
                        p = soup.find("div", class_="body-content")
                        if p:
                            if p.h1.text[0:22] == u'Sorry, that page doesn':    #truncated to avoid non-ascii apostrophe
                                deleted = True
                        else:
                            print id
                    cur.execute('INSERT INTO followers (user_id, bot, suspended, deleted, crawled) VALUES ((%s), (%s), (%s), (%s), (%s))', (id, bot, suspended, deleted, crawled))
                    con.commit()

find_suspended_followers()