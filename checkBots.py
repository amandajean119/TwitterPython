import os
import psycopg2
import json
from dotenv import load_dotenv
import sys
import traceback
import ast

class CheckBots:

    def __init__(self):
        self.count = 0
        self.bot_count = 0
        self.to_check = set()

    def check_follower_membership(self):
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
                if id not in self.to_check:
                    self.to_check.add(id)
                    cur.execute('SELECT * FROM name_id  WHERE user_id = (%s)', (str(id),))
                    if cur.fetchone():
                        self.bot_count += 1
                    self.count += 1
        print "Total number of unique followers: " + str(self.count)
        print "Number of unique followers identified as bots by Debot: " + str(self.bot_count)

    def check_friend_membership(self):
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
        cur.execute('SELECT friends FROM name_id  WHERE friends IS NOT NULL')
        records = cur.fetchall()
        for record in records:
            record = ast.literal_eval(record[0])
            for id in record:
                if id not in self.to_check:
                    self.to_check.add(id)
                    cur.execute('SELECT * FROM name_id  WHERE user_id = (%s)', (str(id),))
                    if cur.fetchone():
                        self.bot_count += 1
                    self.count += 1
        print "Total number of unique friends: " + str(self.count)
        print "Number of unique friends identified as bots by Debot: " + str(self.bot_count)

    def update_counts(self):
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
        cur.execute('SELECT user_id, followers FROM name_id  WHERE followers IS NOT NULL')
        records = cur.fetchall()
        for record in records:
            user_id = record[0]
            record = ast.literal_eval(record[1])
            cur.execute('UPDATE name_id SET num_followers = (%s) WHERE user_id = (%s)', (len(record), user_id))
            con.commit()
        cur.execute('SELECT user_id, friends FROM name_id  WHERE friends IS NOT NULL')
        records = cur.fetchall()
        for record in records:
            user_id = record[0]
            record = ast.literal_eval(record[1])
            cur.execute('UPDATE name_id SET num_friends = (%s) WHERE user_id = (%s)', (len(record), user_id))
            con.commit()

    def get_non_bots(self):
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
        with open('nonbots.csv', 'w') as f:
            for record in records:
                record = ast.literal_eval(record[0])
                for id in record:
                    if id not in self.to_check:
                        self.to_check.add(id)
                        cur.execute('SELECT * FROM name_id  WHERE user_id = (%s)', (str(id),))
                        if not cur.fetchone():
                            f.write(str(id) + '\n')

cb = CheckBots()
#cb.update_counts()
#cb.check_follower_membership()
#cb.check_friend_membership()
cb.get_non_bots()