# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import tweepy
import time
import MySQLdb
import datetime
import sys
import os
import traceback


class CheckClusterConnection:
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
        except tweepy.RateLimitError:
            traceback.print_exc()
            self.sleep_time()
            self.query_api(user)
        except tweepy.TweepError:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: ' + user
            self.users_followers[user] = ([], [])

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
            self.user_followers = {}

    def get_users(self, file_path):
        folders = os.walk(file_path)
        for f in folders:
            files = [fs for fs in f[2] if 'clstrs' in fs]
            if files:
                for i in range(0, len(files)):
                    self.files.append((f[0], files[i]))


if __name__ == '__main__':
    tokens = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
              "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
              "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    g_u_f = GetUserFollowers(tokens)
    # json.dump(g_u_f.users_followers, open('test.json', 'w'), indent=4, sort_keys=True, separators=(',', ': '))
    g_u_f.get_users('/home/amanda/bigDisk/Twitter/Debot2/')
    g_u_f.find_followers()
