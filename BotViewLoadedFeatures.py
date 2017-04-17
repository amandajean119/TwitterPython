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
from math import pow
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
from sklearn.ensemble import IsolationForest
from scipy.special import erf
from dbod import DBOD
from abod import ABOD
from abod import computeDistMatrix
from sklearn.feature_selection import VarianceThreshold
from copy import deepcopy


class BotView:
    """
    This class starts with a temporal bot cluster. If the cluster has above a minimum number of edges connecting the
    members, then it will examine the followers sets and find common followers who are highly connectd to many of the
    cluster members. It will then get the followers of these highly connected followers and repeat the process until
    enough steps have been taken or no more followers satisfy the criterion.
    """

    def __init__(self, tokens_ar, user, file_path, priors, id=False):
        """
        Initializes the data structures, connects to the PostgreSQL database, and sets up the Twitter API connection
        :param tokens_ar: array of tokens for Twitter API
        :param cluster: Seed cluster for this object
        :return:
        """
        # Clique is the current set of all highly-connected nodes at all levels
        self.clique = set()
        # to_check is the nodes that we need to find followers for
        self.to_check = set()
        # user_followers is the dictionary of node_id: followers_list for this round. Resets every round.
        self.got_followers = set()
        self.current_level_users = []
        # followers is the list of all followers for all nodes in to_check. Resets every round.
        self.followers = []
        self.user_info = {}
        self.users = set()
        self.current_level_timelines = {}
        self.ignore_users = set()
        self.current_filepath = file_path
        self.stream_filepath = '/home/amanda/bigDisk/Twitter/Debot2/stream'
        self.clique_features = {}
        self.vec = DictVectorizer()
        self.priors = priors
        self.len_priors = len(self.priors)
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

        # This handles Twitter authentication and the connection to Twitter Streaming API
        self.access_token = tokens_ar[0]
        self.access_token_secret = tokens_ar[1]
        self.consumer_key = tokens_ar[2]
        self.consumer_secret = tokens_ar[3]
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        self.n = 1.0
        self.original_n = 1.0
        self.level = 0
        if id:
            self.clique.add((user, self.level))
            self.to_check.add(user)
            self.seed_user = user
        else:
            self.cur.execute('SELECT user_id FROM name_id WHERE screen_name = %s;', (str(user),))
            f = self.cur.fetchone()
            if f:
                self.clique.add((f[0], self.level))
                self.to_check.add(f[0])
                self.seed_user = user
            else:
                print "Don't have id for: " + user
                self.clique.add((user, self.level))
                self.to_check.add(user)
                self.seed_user = user

    def explore(self):
        """
        Pops items from to_check and adds their followers to user_followers
        :return:
        """
        i = 0
        # Need to reset followers and user_followers for this round
        self.followers = []
        self.current_level_users = []
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
                        if f[0] == '[]':
                            self.ignore_users.add(user)
                            continue
                        try:
                            followers = ast.literal_eval(f[0])
                        except ValueError:
                            self.ignore_users.add(user)
                            continue
                        self.got_followers.add(user)
                        self.followers.extend(followers)
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
            self.followers.extend(followers)
            self.cur.execute('SELECT * FROM name_id WHERE user_id = %s;', (str(user),))
            f = self.cur.fetchone()
            if f:
                self.cur.execute('UPDATE name_id SET followers = (%s) WHERE user_id = %s', (str(followers), str(user)))
            else:
                self.cur.execute('INSERT INTO name_id (user_id, followers) VALUES (%s, %s);', (str(user), str(followers)))
            self.con.commit()
            #print "Added followers for " + str(user)
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

    def load_all_find_bots(self):
        features = pickle.load(open('clique_expansion/all_features/' + self.seed_user + '_all_features.p', 'rb'))
        # we can look at the out-degree of the collapsed ego network. We also calculate the average out degree,
        # which is the average number of followers per follower.
        # need to get the followers for all these
        user_features = dict()
        for key, value in features.iteritems():
            for user, f in value.iteritems():
                if user in user_features:
                    user_features[user].update(f)
                else:
                    user_features[user] = f
        self.priors = zip(self.priors['content'], self.priors['network'], self.priors['temporal'], self.priors['user'])
        current_features = []
        for a, b, c, d in self.priors:
            a.update(b)
            a.update(c)
            a.update(d)
            current_features.append(a)
        self.priors = deepcopy(current_features)
        print len(self.priors)
        for key, value in user_features.iteritems():
            self.current_level_users.append(key)
            current_features.append(value)
        print len(self.priors)
        print "Performing anomaly detection"
        #json.dump(priors, open('test.json', 'w'), indent=4, separators=(',', ': '))
        X = self.vec.fit_transform(current_features).toarray()
        current_features = {}
        print str(X.shape)
        sel = VarianceThreshold(threshold=0.001)
        X = sel.fit_transform(X)
        print str(X.shape)
        X_norm = normalize(X)
        #print np.any(np.isnan(X))
        #print np.all(np.isfinite(X))
        outliers = self.perform_outlier_detection(X)

        #How do I add back in the outliers to the anomaly detection? Mueen said not to so I will leave for now
        self.level += 1
        # Add highly connected followers to the clique and to_check
        clique_features = {}
        for follower in outliers:
            self.clique.add((follower, self.level))
            self.to_check.add(follower)
            self.clique_features[follower] = user_features[follower]
        user_features = {}
        print self.clique
        self.n = float(len(self.clique))
        print "Current size of cluster: " + str(self.n)

    def perform_outlier_detection(self, X):
        # LOF on all features

        clf = LocalOutlierFactor(n_neighbors=20)
        clf.fit(X)
        lof_scores = clf._decision_function(X)
        lof_scores = clf._decision_function(X)

        # Isolation forest on all features
        clf = IsolationForest()
        clf.fit(X)
        forest_scores = clf.decision_function(X)
        '''
        clf = DBOD()
        clf.fit(X)
        distance_scores = clf.decision_function_distance(X)

        #abod_scores = ABOD(X, self.seed_user)
        abod_scores = clf.decision_function_angle(X)

        scores = self.combine([lof_scores, forest_scores, distance_scores, abod_scores])
        '''
        # scores = forest_scores
        scores = self.combine([lof_scores, forest_scores])
        '''
        with open('clique_expansion/' + self.seed_user + '_unnormalized_scores.csv', 'w') as f:
            for score in scores:
                f.write(str(score) + '\n')
                '''
        new_scores = scores[self.len_priors:]
        user_scores = sorted(zip(self.current_level_users, new_scores), key=lambda x: x[1], reverse=True)
        threshold = np.percentile(new_scores, 8)
        outliers = [u[0] for u in user_scores if u[1] <= threshold]
        return outliers

    def combine(self, scores_list):

        # For both LOF and random forest, lower scores are outliers, so we do a log transform for both
        sqrt_two = pow(2, 2)
        scores_norms = []
        # lof_max = max(lof_scores)
        # lof_scores = [lof_max - x for x in lof_scores]
        for scores in scores_list:
            scores_mean = np.mean(scores)
            scores_std = np.std(scores)
            scores_norms.append([max([(erf((x-scores_mean)/(scores_std*sqrt_two))), 0]) for x in scores])

        norms = [np.mean(list(s)) for s in zip(*scores_norms)]
        with open('clique_expansion/' + self.seed_user + '_normalized_scores.csv', 'w') as f:
            for item in norms:
                f.write(str(item) + '\n')
        return norms


def seed_users(priors):
    """
    tokens_ar = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
                 "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
                 "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    """
    tokens_ar = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
                 "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]
    files = os.listdir('clique_expansion/')
    ignore_seeds = set()
    files = [fs for fs in files if 'clique' in fs]
    if files:
        for i in range(0, len(files)):
            ignore_seeds.add(files[i].split('_')[0])
    print ignore_seeds
    users = {'2770018159', '148253367', '4246877778', '3942027736', '19264063', '4827286170', '136960509',
              '1455286956', '4177176192', '258240944', '2216736289', '286922738', '26102775', '1514534004',
             '114494345', '2868389650'} - ignore_seeds
    '''
    users = {'4827286170', '2868389650', '2770018159', '114494345', '26102775', '2216736289', '286922738', '258240944',
             '148253367', '1514534004'} - ignore_seeds
    users2 = {'454570392', '453507246', '1455286956', '2696294077', '136960509', '4177176192', '4246877778',
              '3942027736', '90379747'} - ignore_seeds
              '''
    file_path = '/home/amanda/bigDisk/Twitter/'
    for user in users:
        bv = BotView(tokens_ar, user, file_path, priors, id=True)
        print "Exploring for user " + user
        while bv.to_check:
            print "Exploring..."
            bv.explore()
            bv.load_all_find_bots()
            if bv.level >= 1:
                break
        clique = sorted(list(bv.clique), key=lambda c: c[1])
        if bv.n == bv.original_n:
            print "cluster did not grow!"
        else:
            with open('clique_expansion/' + user + '_clique_expansion' + '.csv', 'w') as f:
                print file_path
                for id, level in clique:
                    f.write(str(id) + ',' + str(int(level)) + '\n')
            with open('clique_expansion/' + user + '_features.p', 'wb') as f:
                pickle.dump(bv.clique_features, f)


def create_distance_matrices(priors):
    users = {'2770018159', '148253367', '4246877778', '3942027736', '19264063', '4827286170', '136960509',
             '1455286956', '4177176192', '258240944', '2216736289', '286922738', '26102775', '1514534004',
             '114494345', '2868389650'}
    vec = DictVectorizer()
    priors = zip(priors['content'], priors['network'], priors['temporal'], priors['user'])
    current_features = []
    for a, b, c, d in priors:
        a.update(b)
        a.update(c)
        a.update(d)
        current_features.append(a)
    priors = current_features
    for seed_user in users:
        current_features = priors
        features = pickle.load(open('clique_expansion/all_features/' + seed_user + '_all_features.p', 'rb'))
        user_features = dict()
        for key, value in features.iteritems():
            for user, f in value.iteritems():
                if user in user_features:
                    user_features[user].update(f)
                else:
                    user_features[user] = f
        for key, value in user_features.iteritems():
            current_features.append(value)
        X = vec.fit_transform(current_features).toarray()
        distTable = computeDistMatrix(X)
        pickle.dump(distTable, open('clique_expansion/dist_matrix_' + seed_user + '.p', 'wb'))

def get_priors():
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
    with open('sample_seed_users_first.txt', 'r') as f:
    #with open('sample_seed_users_first.txt', 'r') as f:
        user_ids = [line.strip() for line in f]
    file_names = []
    features_list = []
    folders = os.walk('/home/amanda/bigDisk/Twitter/Debot2/stream/')
    # only one folder so can put i check inside inner loop
    for f in folders:
        files = [fs for fs in f[2] if 'stream.json' in fs]
        if files:
            for i in range(0, len(files)):
                if files[i].split('_')[0] in user_ids:
                    file_names.append((f[0], files[i]))
                    '''
                if i >= 30000:
                    break
                    '''
    print "got filenames"
    for file_path, file_name in file_names:
        try:
            f = json.load(open(file_path +'/' + file_name, 'r'))
        except:
            print file_path
            print file_name
            continue
        if len(f) > 150:
            user_id = file_name.split('_')[0]
            cur.execute('SELECT user_info_json FROM followers WHERE user_id = %s', (user_id,))
            record = cur.fetchone()
            if record:
                if record[0]:
                    user_info = ast.literal_eval(record[0])
                else:
                    continue
                gf = GetFeatures(user_id, user_info, f)
                gf.user_features()
                gf.collect_tweets()
                gf.content_features()
                gf.temporal_features()
                # need to incorporate other network features
        #        gf.features['num_shared_edges'] = follower_counts[user]
                features_list.append(gf.features)

    print len(features_list)
    pickle.dump(features_list, open('priors_feature_list.p', 'wb'))
    # Do we want to perform outlier detection and remove outliers from priors?
    #prior_feature_matrix = vec.fit_transform(features_list).toarray()
    #np.save(open('priors.npy', 'wb'), prior_feature_matrix)
    #return prior_feature_matrix

def get_files(file_path):
    folders = os.walk(file_path)
    file_names = []
    for f in folders:
        files = [fs for fs in f[2] if 'clstrs' in fs]
        if files:
            for i in range(0, len(files)):
                file_names.append((f[0], files[i]))
    return file_names

file_names = get_files('/home/amanda/bigDisk/Twitter/Debot2/')
#get_priors()
priors = pickle.load(open('priors_feature_list2.p', 'rb'))
#priors = np.load(open('priors.npy', 'rb'))
#print priors.shape
#aggregate_users(file_names, priors)
seed_users(priors)
#create_distance_matrices(priors)