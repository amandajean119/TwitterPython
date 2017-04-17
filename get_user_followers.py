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

class GetUserFollowers:
    """


    """

    def __init__(self, tokens_ar):
        self.access_token = tokens_ar[0]
        self.access_token_secret = tokens_ar[1]
        self.consumer_key = tokens_ar[2]
        self.consumer_secret = tokens_ar[3]

        # This handles Twitter authentification and the connection to Twitter Streaming API
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

    def load_from_file(self, f):
        filename = open(f, 'r')
        contents = filename.read()
        filename.close()
        items = [name for name in contents.split('\n') if name]
        return items

    def sleep_time(self):
        cur_time = datetime.datetime.now()
        diff = (cur_time - self.start_time).total_seconds()
        sleep_time = 900 - diff
        print "sleep time!: " + str(sleep_time)
        time.sleep(sleep_time)
        self.start_time = datetime.datetime.now()

    def query_api(self, user):
        try:
            followers = self.api.followers_ids(user)
            friends = self.api.friends_ids(user)
            self.users_followers[user] = (followers, friends)
            self.cur.execute('UPDATE name_id SET followers = (%s) WHERE screen_name = (%s)', (str(followers), user))
            self.cur.execute('UPDATE name_id SET friends = (%s) WHERE screen_name = (%s)', (str(friends), user))
            self.con.commit()
        except tweepy.RateLimitError:
            traceback.print_exc()
            self.sleep_time()
            self.query_api(user)
        except tweepy.TweepError:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: ' + user
            self.users_followers[user] = ([], [])

    def query_api_no_friendship(self, user):
        try:
            followers = self.api.followers_ids(user)
            self.users_followers[user] = followers
            self.cur.execute('UPDATE name_id SET followers = (%s) WHERE screen_name = (%s)', (str(followers), user))
            self.con.commit()
        except tweepy.RateLimitError:
            traceback.print_exc()
            self.sleep_time()
            self.query_api_no_friendship(user)
        except tweepy.TweepError:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: ' + user
            self.users_followers[user] = None

    def find_followers(self):
        # Variables that contains the user credentials to access Twitter API:
        # ============================== Add your Twitter keys here.
        i = 0
        self.start_time = datetime.datetime.now()
        for file_path, file_name in self.files:
            with open(file_path + '/' + file_name, 'r') as f:
                for line in f:
                    for user in line.strip().split(','):
                        if user not in self.users:
                            self.users.add(user)
                            if (i + 1) % 14 != 0:
                                self.query_api(user)
                            else:
                                self.sleep_time()
                                self.query_api(user)
                            i += 1
            json.dump(self.users_followers, open(file_path + '/' + 'bots_followers_friends.json', 'w'), indent=4,
                      sort_keys=True, separators=(',', ': '))
            self.users_followers = {}

    def get_files(self, file_path):
        folders = os.walk(file_path)
        for f in folders:
            files = [fs for fs in f[2] if 'clstrs' in fs]
            if files:
                for i in range(0, len(files)):
                    self.files.append((f[0], files[i]))

    def find_followers_clusters(self):
        i = 0
        self.start_time = datetime.datetime.now()
        with open('subset_users.csv', 'r') as f:
            next(f)
            for line in f:
                user = line.split(',')[0]
                if user not in self.users:
                    self.cur.execute('SELECT followers FROM name_id WHERE screen_name = (%s)', (user,))
                    if not self.cur.fetchone()[0]:
                        self.users.add(user)
                        if (i + 1) % 14 != 0:
                            self.query_api_no_friendship(user)
                        else:
                            self.sleep_time()
                            self.query_api_no_friendship(user)
                        i += 1
        json.dump(self.users_followers, open('bots_followers_friends_clusters.json', 'w'), indent=4, sort_keys=True,
                  separators=(',', ': '))



if __name__ == '__main__':
    tokens = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
              "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
              "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    g_u_f = GetUserFollowers(tokens)
    # json.dump(g_u_f.users_followers, open('test.json', 'w'), indent=4, sort_keys=True, separators=(',', ': '))
    #g_u_f.get_files('/home/amanda/bigDisk/Twitter/Debot2/')
    #g_u_f.find_followers()
    g_u_f.find_followers_clusters()
