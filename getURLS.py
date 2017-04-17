import psycopg2
from dotenv import load_dotenv
import os
import traceback
import sys
import ast


def load_from_file (f):
    filename = open(f, 'r')
    contents = filename.read()
    filename.close()
    items = [name for name in contents.split('\n') if name]
    return items


def get_URLS():
    try:
        load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
        username = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASSWORD')
        conn_string = "dbname='twitter' user=" + username + " password = " + password
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
    except psycopg2.DatabaseError as e:
        print 'Error: ' + e
        traceback.print_exc()
        sys.exit(1)
    users = load_from_file('all_no_dups.txt')
    with open('all_screen_names.txt', 'w') as output:
        for user in users:
            cur.execute('SELECT user_info_json FROM followers WHERE user_id = %s;', (user,))
            f = cur.fetchone()
            if f:
                try:
                    user_info = ast.literal_eval(f[0])
                    screen_name = user_info['screen_name']
                    url = 'https://twitter.com/' + screen_name
                    output.write(url + '\n')
                except Exception as e:
                        print e
                        continue

get_URLS()