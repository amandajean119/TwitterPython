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
from math import floor

class BotView:
    """
    This class starts with a temporal bot cluster. If the cluster has above a minimum number of edges connecting the
    members, then it will examine the followers sets and find common followers who are highly connectd to many of the
    cluster members. It will then get the followers of these highly connected followers and repeat the process until
    enough steps have been taken or no more followers satisfy the criterion.
    """

    def __init__(self, tokens_ar, users, file_path):
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
        # user_followers is the dictionary of node_id: followers_list for this round. Resets every round. WHY??
        self.got_followers = set()
        self.current_level_users = []
        # followers is the set of all followers for all nodes in to_check. Resets every round. WHY??
        self.followers = []
        self.user_info = {}
        self.users = set()
        self.current_level_timelines = {}
        self.ignore_users = set()
        self.current_filepath = file_path
        self.stream_filepath = '/home/amanda/bigDisk/Twitter/Debot2/stream'
        self.user_features = {}
        self.features_list = []
        self.vec = DictVectorizer()

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

        self.level = 0
        for user in users:
            self.cur.execute('SELECT user_id FROM name_id WHERE screen_name = %s;', (str(user),))
            f = self.cur.fetchone()
            if f:
                self.clique.add((f[0], self.level))
                self.to_check.add(f[0])
            else:
                print "Don't have id for: " + user
                self.clique.add((user, self.level))
                self.to_check.add(user)
        self.n = float(len(self.clique))
        self.original_n = self.n

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
                self.cur.execute('SELECT deleted, suspended, other_error FROM followers WHERE user_id = %s;', (str(user),))
                f = self.cur.fetchone()
                if f:
                    if f[0] or f[1] or f[2]:
                        self.ignore_users.add(user)
                        continue
                self.query_api(user)

    def query_api(self, user):
        """
        Query Twitter API for the followers of a given user. Add this entry to user_followers, add to followers, and
        add to database
        :param user: The user of interest
        :return:
        """
        try:
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

    def find_bots(self, priors):
        print "Getting all user info..."
        self.users_to_query = set()
        followers_set = set(self.followers)
        print "Number of followers: " + str(len(self.followers))
        follower_counts = Counter(self.followers).most_common()
        # should fix this to be a more precise measure
        size_to_keep = int(.15*len(self.followers))
        connectedness_threshold = floor(0.3*self.n)
        tmp_followers = [f[0] for f in follower_counts if f[1] >= connectedness_threshold]
        if len(tmp_followers) < size_to_keep:
            tmp_followers.extend([f[0] for f in follower_counts[:size_to_keep] if f[1] > 1])
        followers_set = set(tmp_followers)
        print "Number of connected followers: " + str(len(followers_set))
        for follower in followers_set:
            user_info = None
            follower = str(follower)
            if follower not in self.users and follower not in self.ignore_users:
                self.cur.execute('SELECT suspended, deleted, other_error, user_info_json FROM followers WHERE user_id = %s', (follower,))
                record = self.cur.fetchone()
                if record:
                    if record[0] or record[1] or record[2]:
                        self.ignore_users.add(follower)
                        # print "User is suspended or deleted"
                        continue
                    if record[3]:
                        # print "Already have profile information for user number " + follower
                        self.user_info[follower] = ast.literal_eval(record[3])
                        continue
                self.users_to_query.add(follower)
        get_user_info(self)
        print "Getting all timeline info and extracting features"
        for follower in followers_set:
            timeline = None
            follower = str(follower)
            if follower not in self.users and follower not in self.ignore_users:
                self.users.add(follower)
                self.cur.execute('SELECT suspended, deleted, other_error, timeline FROM followers WHERE user_id = %s', (follower,))
                record = self.cur.fetchone()
                if record:
                    if record[0] or record[1] or record[2]:
                        self.ignore_users.add(follower)
                        # print "User is suspended or deleted"
                        continue
                    if record[3]:
                        # print "Already have timeline information for user number " + follower
                        # Have to read in file to get timeline info
                        timeline = get_timeline_from_file(self, follower)
                    else:
                        timeline = get_user_timeline(self, follower)
                else:
                    timeline = get_user_timeline(self, follower)
                if timeline and self.user_info.get(follower) and len(timeline) > 50:
                    gf = GetFeatures(follower, self.user_info[follower], timeline)
                    try:
                        gf.user_features()
                        gf.collect_tweets()
                        gf.content_features()
                        gf.temporal_features()
                    except Exception as e:
                        print "ERROR GETTING FEATURES"
                        print e
                        print follower
                        print self.user_info[follower]
                    # need to incorporate other network features
                    #gf.features['num_shared_edges'] = follower_counts[user]
                    #cself.user_features[user] = gf.features
                    self.current_level_users.append(follower)
                    self.features_list.append(gf.features)
        # Axis=0 should be vertical
        len_priors = len(priors)
        current_features = priors
        current_features.extend(self.features_list)
        print "Performing anomaly detection"
        #json.dump(priors, open('test.json', 'w'), indent=4, separators=(',', ': '))
        X = self.vec.fit_transform(current_features).toarray()
        current_features = {}
        X_norm = normalize(X)
        #print np.any(np.isnan(X))
        #print np.all(np.isfinite(X))
        print X.shape
        # X = np.stack([current_features, priors], axis=0) Every round will find outliers, how do we stop exploring?
        clf = LocalOutlierFactor(n_neighbors=20)
        clf.fit(X)
        check_is_fitted(clf, ["threshold_", "negative_outlier_factor_", "n_neighbors_", "_distances_fit_X_"])
        if X is not None:
            X = check_array(X, accept_sparse='csr')
            y_pred = clf._decision_function(X)
        else:
            y_pred = clf.negative_outlier_factor_
        #y_pred = clf.fit_predict(X)
        y_pred_new = y_pred[len_priors:]
        # Do anomaly detection and set connected followers to certain outliers
        # this line is a stand-in
        users_scores = zip(self.current_level_users, y_pred_new)
        connected_followers = [u[0] for u in users_scores if u[1] <= clf.threshold_]
        #How do I add back in the outliers to the anomaly detection? Mueen said not to so I will leave for now
        self.level += 1
        # Add highly connected followers to the clique and to_check
        for follower in connected_followers:
            self.clique.add((follower, self.level))
            self.to_check.add(follower)
        print self.clique
        self.n = float(len(self.clique))
        print "Current size of cluster: " + str(self.n)


def aggregate_users(file_names, priors):
    clusters = []
    i = 0
    for file_path, file_name in file_names:
        with open(file_path + '/' + file_name, 'r') as f:
            for line in f:
                clusters.append((line.strip().split(','), file_path))
    '''
    tokens_ar = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
                 "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
                 "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    '''
    tokens_ar = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
                 "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]

    for cluster, file_path in clusters:
        if len(cluster) > 10:
            bv = BotView(tokens_ar, cluster, file_path)
            while bv.to_check:
                print "Exploring..."
                bv.explore()
                bv.find_bots(priors)
            clique = sorted(list(bv.clique), key=lambda c: c[1])
            if bv.n == bv.original_n:
                print "cluster did not grow!"
            else:
                with open(file_path + '/clique_expansion' + '.csv', 'w') as f:
                    print file_path
                    for id, level in clique:
                        f.write(str(id) + ',' + str(int(level)) + '\n')


def get_priors():
    folders = os.walk('/home/amanda/bigDisk/Twitter/random_streams/')
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
    file_names = []
    features_list = []
    vec = DictVectorizer()
    for f in folders:
        files = [fs for fs in f[2] if 'stream.json' in fs]
        if files:
            for i in range(0, len(files)):
                file_names.append((f[0], files[i]))
    for file_path, file_name in file_names:
        f = json.load(open(file_path +'/' + file_name, 'r'))
        if len(f) > 100:
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
                if len(gf.features) > 37:
                    print "too long"
                    print gf.features
                features_list.append(gf.features)
    pickle.dump(features_list, open('priors_feature_list.p', 'wb'))
    # Do we want to use another experiment's feature matrix to help with the anomaly detection?
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
priors = pickle.load(open('priors_feature_list.p', 'rb'))
#priors = np.load(open('priors.npy', 'rb'))
#print priors.shape
aggregate_users(file_names, priors)
