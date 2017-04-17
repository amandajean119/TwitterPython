import os
import pandas as pd
import psycopg2
import sys
from dotenv import load_dotenv
import traceback


def get_files(file_path):
    folders = os.walk(file_path)
    file_names = []
    for f in folders:
        files = [fs for fs in f[2] if 'clique' in fs]
        if files:
            for i in range(0, len(files)):
                file_names.append((f[0], files[i]))
    return file_names


def aggregate_names(file_names):
    try:
        load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
        username = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASSWORD')
        conn_string = "dbname='twitter' user=" + username + " password = " + password
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
    except psycopg2.DatabaseError as e:
        print 'Error %s' % e
        traceback.print_exc()
        sys.exit(1)
    with open('followers_for_stream.csv', 'w') as f:
        for file_path, file_name in file_names:
            df = pd.read_csv(file_path + '/' + file_name, names=['id', 'level'])
            ids = df[df['level'] > 0]['id']
            for id in ids:
                cur.execute('SELECT * FROM name_id WHERE user_id = (%s) AND collect_date is not null and screen_name is not null;', (str(id),))
                if cur.fetchone():
                    print "Already collected stream for: " + str(id)
                    continue
                f.write(str(id) + '\n')


aggregate_names(get_files('/home/amanda/bigDisk/Twitter/Debot2/'))

