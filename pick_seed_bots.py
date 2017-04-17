import psycopg2
import json
from dotenv import load_dotenv
import sys
import os
import traceback
import ast
from numpy import mean
import random
import pandas as pd
import tweepy
import numpy as np

class PickSeedBots:

    def __init__(self, tokens_ar):
        try:
            load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
            user = os.getenv('DATABASE_USER')
            password = os.getenv('DATABASE_PASSWORD')
            conn_string = "dbname='twitter' user=" + user + " password = " + password
            self.con = psycopg2.connect(conn_string)
            self.cur = self.con.cursor()
        except psycopg2.DatabaseError as e:
            print 'Error %s' % e
            traceback.print_exc()
            sys.exit(1)
        self.user_followers = {}
        self.user_jaccard = {}
        self.access_token = tokens_ar[0]
        self.access_token_secret = tokens_ar[1]
        self.consumer_key = tokens_ar[2]
        self.consumer_secret = tokens_ar[3]
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def collect_followers(self):
        self.cur.execute("SELECT followers, user_id FROM name_id WHERE followers IS NOT NULL and followers <> '[]';")
        users = self.cur.fetchall()
        i = 0
        while i < 1000:
            index = random.randrange(len(users))
            elem = users[index]
            self.cur.execute('SELECT bot, suspended, deleted, other_error, user_info_json FROM followers WHERE user_id = %s;', (elem[1],))
            f = self.cur.fetchone()
            if f:
                if f[1] or f[2] or f[3]:
                    continue
                if f[0]:
                    self.user_followers[elem[1]] = set(ast.literal_eval(elem[0]))
                    del users[index]
                    i += 1
                    self.user_jaccard[elem[1]] = []
        print "Collected followers"
        for user1, followers1 in self.user_followers.iteritems():
            for user2, followers2 in self.user_followers.iteritems():
                if user1 != user2:
                    j_val = jaccard(followers1, followers2)
                    self.user_jaccard[user1].append(j_val)
                    self.user_jaccard[user2].append(j_val)
        json.dump(self.user_jaccard, open('all_user_jaccards.json', 'w'))

    def get_seed_bots(self):
        user_jaccard = json.load(open('all_user_jaccards.json', 'r'))
        user_max = {}
        user_mean = {}
        for key, value in user_jaccard.iteritems():
            # user_max[key] = (max(value), mean(value), std(value))
            user_mean[key] = mean(value)
            user_max[key] = max(value)
        user_mean2 = pd.DataFrame(user_mean.items(), columns=['user_id', 'mean_jaccard'])
        user_max2 = pd.DataFrame(user_max.items(), columns=['user_id', 'max_jaccard'])
        user_mean_max = user_mean2.merge(user_max2, on='user_id', how='inner')
        user_mean_max.sort_values(['mean_jaccard', 'max_jaccard'], ascending=[False, False], inplace=True)
        #user_mean_max.to_csv('all_possible_seeds.csv', index=False)
        with open('bot_seeds.csv', 'w') as output:
            for user_id in user_mean_max['user_id']:
                '''
                cur.execute('SELECT bot, suspended, deleted, other_error, user_info_json FROM followers WHERE user_id = %s;', (user_id,))
                f = cur.fetchone()
                if f:
                    if f[1] or f[2] or f[3]:
                        continue
                    if f[0]:
                        if f[4]:
                            user_info = ast.literal_eval(f[4])
                            lang = user_info['lang']
                            if lang == 'en':
                            '''
                output.write(user_id + ',' + str(user_mean[user_id]) + ',' + str(user_max[user_id]) + '\n')
        '''
        seed_users = []
        cur_val = 0
        #mn = OrderedDict(sorted(user_max.items, key=itemgetter(1)))
        mn = sorted(user_max.items(), key=lambda x: x[1], reverse=True)
        #mx = OrderedDict(sorted(user_max.items, key=itemgetter(1)[1]))
        seed_users = mn[0:100]
        with open('seed_users_max.csv', 'w') as f:
            for user in seed_users:
                item1 = user[0]
                item2 = user[1]
                f.write(item1 + ',' + str(item2) + '\n')
                '''

    def get_new_seeds_from_followers(self):
        with open('clique_expansion/all_four_domain_ids_for_debot.csv', 'r') as f:
            ids = np.asarray(list(set([l.strip() for l in f])))
        random_ids = np.random.choice(ids, size=1000, replace=False)
        with open('random_ids_from_followers.csv', 'w') as f:
            for id in random_ids:
                f.write(str(id) + '\n')
        for user in random_ids:
            self.cur.execute("SELECT followers FROM name_id WHERE user_id = %s AND followers IS NOT NULL and followers <> '[]';", (user,))
            f = self.cur.fetchone()
            if f:
                print "Already found followers for user " + str(user)
                self.user_followers[user] = set(ast.literal_eval(f[0]))
            else:
                self.query_api_no_friendship(user)
        print "Collected followers"
        for user1, followers1 in self.user_followers.iteritems():
            for user2, followers2 in self.user_followers.iteritems():
                if user1 != user2:
                    j_val = jaccard(followers1, followers2)
                    self.user_jaccard[user1].append(j_val)
                    self.user_jaccard[user2].append(j_val)
        json.dump(self.user_jaccard, open('all_user_jaccards_new.json', 'w'))

    def query_api_no_friendship(self, user):
        print "Have to get followers for user " + str(user)
        try:
            followers = self.api.followers_ids(user)
            self.user_followers[user] = set(followers)
            self.cur.execute('SELECT * FROM name_id WHERE user_id = %s;', (user,))
            f = self.cur.fetchone()
            if f:
                self.cur.execute('UPDATE name_id SET followers = (%s) WHERE user_id = (%s)', (str(followers), user))
            else:
                self.cur.execute('INSERT INTO name_id(user_id, followers) VALUES (%s, %s)', (user, str(followers)))
            self.con.commit()
        except tweepy.TweepError:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: ' + user
            self.user_followers[user] = None


def jaccard(a, b):
    try:
        return float(len(a & b))/float(len(a | b))
    except:
        return 0


tokens = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd", "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0",
          "IrZza7bDaRKGQUc5ZZM2EiCsG", "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
tokens = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
          "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]
psb = PickSeedBots(tokens)
psb.get_new_seeds_from_followers()
# psb.collect_followers()
# psb.get_seed_bots()
