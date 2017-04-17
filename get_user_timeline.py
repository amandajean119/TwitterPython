# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import tweepy
import time
import datetime
import sys
import os
import traceback
import psycopg2
import json
from dotenv import load_dotenv
import random

class GetUserTimeline:
    """


    """

    def __init__(self, tokens_ar):
        self.access_token = tokens_ar[0]
        self.access_token_secret = tokens_ar[1]
        self.consumer_key = tokens_ar[2]
        self.consumer_secret = tokens_ar[3]

        # This handles Twitter authentication and the connection to Twitter Streaming API
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.users_followers = {}
        self.start_time = None
        self.files = []
        self.users = set()
        try:
            load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
            user = os.getenv('DATABASE_USER')
            password = os.getenv('DATABASE_PASSWORD')
            conn_string = "dbname='twitter' user=" + user + " password = " + password
            self.con = psycopg2.connect(conn_string)
            self.cur = self.con.cursor()
        except (psycopg2.DatabaseError) as e:
            print 'Error %s' % e
            sys.exit(1)

    def sleep_time(self):
        cur_time = datetime.datetime.now()
        diff = (cur_time - self.start_time).total_seconds()
        sleep_time = 900 - diff
        print "sleep time!: " + str(sleep_time)
        time.sleep(sleep_time)
        self.start_time = datetime.datetime.now()

    def query_api(self, user):
        try:
            tmp_statuses = []
            items = tweepy.Cursor(self.api.user_timeline, id=user, include_rts=True, maxnumtweets=200).items(200)
            for item in items:
                json_status = item._json
                tmp_statuses.append(json_status)
            json.dump(tmp_statuses, open('/home/amanda/bigDisk/Twitter/Debot2/stream/' + user + '_stream.json', 'w'), indent=2,
                      separators=(',', ': '))
            print "Wrote file to " + self.current_filepath
            self.cur.execute('SELECT * FROM followers  WHERE user_id = (%s)', (user,))
            if self.cur.fetchone():
                self.cur.execute('UPDATE followers SET timeline = TRUE WHERE user_id = %s', (user,))
            else:
                self.cur.execute('INSERT INTO followers(user_id, deleted, suspended, timeline) VALUES (%s, FALSE, FALSE, TRUE)', (user, ))
            self.con.commit()
        except tweepy.RateLimitError:
            traceback.print_exc()
            self.sleep_time()
            self.query_api(user)
        except tweepy.TweepError as e:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: ' + user
            deleted = False
            suspended = False
            if e.response.status_code == 404:
                deleted = True
            elif e.response.status_code == 401:
                suspended = True
            else:
                print e.response.status_code
                print e.message
            self.cur.execute('SELECT * FROM followers WHERE user_id = %s', (user,))
            if self.cur.fetchone():
                self.cur.execute('UPDATE followers SET timeline = TRUE WHERE user_id = %s', (user,))
                if deleted:
                    self.cur.execute('UPDATE followers SET deleted = TRUE WHERE user_id = %s', (user,))
                if suspended:
                    self.cur.execute('UPDATE followers SET suspended = TRUE WHERE user_id = %s', (user,))
            else:
                self.cur.execute('INSERT INTO followers(user_id, deleted, suspended, timeline) VALUES (%s, %s, %s, TRUE)', (user, deleted, suspended))
            self.con.commit()

    def get_timeline(self):
        # Variables that contains the user credentials to access Twitter API:
        # ============================== Add your Twitter keys here.
        i = 0
        self.start_time = datetime.datetime.now()
        for file_path, file_name in self.files:
            self.current_filepath = file_path
            with open(file_path + '/' + file_name, 'r') as f:
                for line in f:
                    user = line.strip().split(',')[0]
                    if user not in self.users:
                        self.users.add(user)
                        self.cur.execute('SELECT suspended, deleted, timeline FROM followers WHERE user_id = %s', (user,))
                        record = self.cur.fetchone()
                        if record:
                            if record[0] or record[1] or record[2]:
                                print "Already have information for user number " + user
                                continue
                        self.query_api(user)
                        i += 1
                        print i

    def get_files(self, file_path):
        folders = os.walk(file_path)
        for f in folders:
            files = [fs for fs in f[2] if 'clique' in fs]
            if files:
                for i in range(0, len(files)):
                    self.files.append((f[0], files[i]))

    def get_random_users(self, file_name):
        with open(file_name, 'r') as f:
            for line in f:
                user = line.strip()
                self.users.add(user)
        i = 0
        users = list(self.users)
        print "Compiled users"
        random_users = set()
        while i < 70000:
            index = random.randrange(len(users))
            elem = users[index]
            random_users.add(elem)
            del users[index]
            i += 1
        self.users = random_users
        self.current_filepath = '/home/amanda/bigDisk/Twitter/random_streams'
        print "Got 700000 random users"
        with open(self.current_filepath + '/' + 'random_users.txt', 'w') as f:
            for user in self.users:
                f.write(user + '/n')
        print "Wrote users to file"

    def get_timeline_random_users(self):
        i = 0
        for user in self.users:
            self.cur.execute('SELECT suspended, deleted, timeline FROM followers WHERE user_id = %s', (user,))
            record = self.cur.fetchone()
            if record:
                if record[0] or record[1] or record[2]:
                    print "Already have information for user number " + user
                    continue
            self.query_api(user)
            print i
            i += 1

    def get_timeline_random_bots(self):
        i = 0
        self.cur.execute('SELECT user_id, timeline from followers WHERE bot = TRUE and suspended <> TRUE AND deleted <> TRUE;')
        self.users = self.cur.fetchall()
        self.current_filepath = '/home/amanda/bigDisk/Debot2/stream/'
        with open('random_bots.txt', 'w') as f:
            for record in self.users:
                f.write(str(record[0]) + '\n')
        for record in self.users:
            if record[1]:
                print "Already have information for user number " + str(record[0])
                continue
            self.query_api(str(record[0]))
            print i
            i += 1


if __name__ == '__main__':
    tokens = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
              "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
              "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    g_u_f = GetUserTimeline(tokens)
    # g_u_f.get_files('/home/amanda/bigDisk/Twitter/Debot2/')
    # g_u_f.get_random_users('/home/amanda/bigDisk/Twitter/followers.txt')
    g_u_f.get_timeline_random_bots()
    '''
    with open('/home/amanda/bigDisk/Twitter/random_streams/random_users.txt', 'r') as f:
        for line in f:
            g_u_f.users.add(line.strip())
    g_u_f.current_filepath = '/home/amanda/bigDisk/Twitter/random_streams'
    print "Got 700000 random users"
    g_u_f.get_timeline_random_users()
    '''

