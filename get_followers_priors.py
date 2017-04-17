import os
import psycopg2
from dotenv import load_dotenv
import sys
import traceback
import tweepy
import time
import json
import datetime
from collections import Counter
import ast
from math import floor
from get_features import GetFeatures
from sklearn.feature_extraction import DictVectorizer
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import normalize
from sklearn.utils.validation import check_is_fitted
from sklearn.utils import check_array
from get_user_info import *
import numpy as np
import matplotlib.pyplot as plt
import pickle


class GetFollowers:

    def __init__(self, tokens_ar):
        self.got_followers = set()
        self.to_check = set()
        self.ignore_users = set()
        self.access_token = tokens_ar[0]
        self.access_token_secret = tokens_ar[1]
        self.consumer_key = tokens_ar[2]
        self.consumer_secret = tokens_ar[3]
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        try:
            load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
            username = os.getenv('DATABASE_USER')
            password = os.getenv('DATABASE_PASSWORD')
            conn_string = "dbname='twitter' user=" + username + " password = " + password
            self.con = psycopg2.connect(conn_string)
            self.cur = self.con.cursor()
        except psycopg2.DatabaseError as e:
            print 'Error: ' + e
            traceback.print_exc()
            sys.exit(1)

    def get_followers(self):
        while self.to_check:
            user = self.to_check.pop()
            # if we haven't already found the followers for this user
            if user not in self.got_followers and user not in self.ignore_users:
                self.cur.execute('SELECT deleted, suspended, other_error FROM followers WHERE user_id = %s;', (str(user),))
                f = self.cur.fetchone()
                if f:
                    if f[0] or f[1] or f[2]:
                        self.ignore_users.add(user)
                        continue
                self.cur.execute("SELECT followers FROM name_id WHERE user_id = %s;", (str(user),))
                f = self.cur.fetchone()
                # If we have queried this user in the past it will be in the db, so we don't have to waste a query on it
                if f:
                    if f[0]:
                        self.ignore_users.add(user)
                        continue
                # Otherwise we query the Twitter API for this user's followers
                self.query_api(user)

    def query_api(self, user):
        """
        Query Twitter API for the followers of a given user. Add this entry to user_followers, add to followers, and
        add to database
        :param user: The user of interest
        :return:
        """
        try:
            # add in cursor to get all followers
            followers = self.api.followers_ids(user)
            self.got_followers.add(user)
            self.cur.execute('SELECT * FROM name_id WHERE user_id = %s;', (str(user),))
            f = self.cur.fetchone()
            if f:
                self.cur.execute('UPDATE name_id SET followers = (%s) WHERE user_id = %s', (str(followers), str(user)))
            else:
                self.cur.execute('INSERT INTO name_id (user_id, followers) VALUES (%s, %s);', (str(user), str(followers)))
            self.con.commit()
            print "Added followers for " + str(user)
        except tweepy.TweepError:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: ' + str(user)
            self.ignore_users.add(user)
            self.cur.execute('SELECT * FROM name_id WHERE user_id = %s;', (str(user),))
            f = self.cur.fetchone()
            if f:
                self.cur.execute('UPDATE name_id SET followers = (%s) WHERE user_id = %s', ('[]', str(user)))
            else:
                self.cur.execute('INSERT INTO name_id (user_id, followers) VALUES (%s, %s);', (str(user), '[]'))
            self.con.commit()

    def get_users(self, users_file):
        with open(users_file, 'r') as f:
            user_ids = set([line.strip() for line in f])
        print len(user_ids)
        folders = os.walk('/home/amanda/bigDisk/Twitter/Debot2/stream/')
        with open('sample_seed_users.txt', 'w') as output:
            with open('sample_seed_users_first.txt', 'r') as f:
                tmp_set = set()
                for line in f:
                    tmp_set.add(line.strip())
            print len(tmp_set)
            user_ids = user_ids - tmp_set
            for f in folders:
                files = [fs for fs in f[2] if 'stream.json' in fs]
                if files:
                    for i in range(0, len(files)):
                        if files[i].split('_')[0] in user_ids:
                            try:
                                json_file = json.load(open(f[0] + '/' + files[i], 'r'))
                                if len(json_file) >= 200:
                                    self.to_check.add(files[i].split('_')[0])
                                    output.write(files[i].split('_')[0] + '\n')
                            except Exception as e:
                                print e
                                print f[0]
                                print files[i]
                                continue
        print len(self.to_check)

    def get_subset(self):
        with open('sample_seed_users_first.txt', 'r') as input:
            for user in input:
                self.to_check.add(user)
        '''
        if len(self.to_check) > 15000:
            with open('sample_seed_users_subset.txt', 'w') as output:
                self.to_check = np.random.choice(self.to_check, size=15000, replace=False)
                for item in self.to_check:
                    output.write(item + '\n')
                    '''



tokens_ar = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd", "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0",
             "IrZza7bDaRKGQUc5ZZM2EiCsG", "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
g_f = GetFollowers(tokens_ar)
#g_f.get_users('random_streams/random_users.txt')
g_f.get_subset()
g_f.get_followers()