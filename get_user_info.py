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


class GetUserInfo:
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
        self.users_to_query = set()
        self.user_info = {}
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

    def query_api(self):
        tmp_check = None
        try:
            i = 0
            users = list(self.users_to_query)
            while i < len(users):
                tmp_check = users[i: i+100]
                user_info = self.api.lookup_users(user_ids=tmp_check)
                i += 100
                for user in user_info:
                    user_json = user._json
                    print user_json['id_str']
                    self.cur.execute('SELECT * FROM followers  WHERE user_id = (%s)', (user_json['id_str'],))
                    if self.cur.fetchone():
                        self.cur.execute('UPDATE followers SET user_info = TRUE WHERE user_id = %s', (user_json['id_str'],))
                        self.cur.execute('UPDATE followers SET user_info_json = %s WHERE user_id = %s', (str(user_json), user_json['id_str']))
                    else:
                        self.cur.execute('INSERT INTO followers(user_id, user_info, user_info_json) VALUES (%s, TRUE, %s)', (user_json['id_str'], str(user_json)))
                    self.con.commit()
        except tweepy.RateLimitError:
            traceback.print_exc()
            self.sleep_time()
            self.query_api()
        except tweepy.TweepError as e:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: '
            for user in tmp_check:
                self.cur.execute('SELECT * FROM followers WHERE user_id = %s', (user,))
                if self.cur.fetchone():
                    self.cur.execute('UPDATE followers SET user_info = TRUE WHERE user_id = %s', (user,))
                else:
                    self.cur.execute('INSERT INTO followers(user_id, user_info) VALUES (%s, TRUE)', (user, ))
                self.con.commit()

    def get_user_ids(self, file_path):
        folders = os.walk(file_path)
        for f in folders:
            files = [fs for fs in f[2] if 'stream' in fs]
            if files:
                for i in range(0, len(files)):
                    user = files[i].split('_')[0]
                    if user not in self.users:
                        self.users.add(user)
                        self.cur.execute('SELECT suspended, deleted, user_info_json FROM followers WHERE user_id = %s', (user,))
                        record = self.cur.fetchone()
                        if record:
                            if record[0] or record[1] or record[2]:
                                print "Already have information for user number " + user
                                continue
                        self.users_to_query.add(user)


if __name__ == '__main__':
    tokens = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
              "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
              "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    g_u_f = GetUserInfo(tokens)
    g_u_f.get_user_ids('/home/amanda/bigDisk/Twitter/Debot2/')
    g_u_f.query_api()
#    json.dump(g_u_f.user_info, open('all_user_info.json', 'w'), indent=2, separators=(',', ': '))