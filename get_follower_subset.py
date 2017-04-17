import os
import psycopg2
import json
from dotenv import load_dotenv
import sys
import traceback
import ast
import random

'''
try:
    load_dotenv('creds/.env')
    user = os.getenv('DATABASE_USER')
    password = os.getenv('DATABASE_PASSWORD')
    conn_string = "dbname='twitter' user=" + user + " password = " + password
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
except (psycopg2.DatabaseError) as e:
    print 'Error %s' % e
    sys.exit(1)
with open('all_followers.csv', 'w') as f:
    followers = set()
    cur.execute('SELECT followers FROM name_id  WHERE followers IS NOT NULL')
    records = cur.fetchall()
    for record in records:
        record = ast.literal_eval(record[0])
        for id in record:
            if id not in followers:
                followers.add(id)
                f.write(str(id) + '\n')
                '''

total = 70000
i = 0
with open('all_followers.csv', 'r') as f:
    with open('followers_subset2.txt', 'w') as output:
        lines = [line for line in f if random.random() <= 0.00218]
        for line in lines:
            output.write(line + '\n')