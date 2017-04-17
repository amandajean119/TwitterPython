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
from get_domain_specific_features import GetFeatures
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
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import OneHotEncoder
import pandas as pd
from collections import OrderedDict


class BotView:
    """
    This class starts with a temporal bot cluster. If the cluster has above a minimum number of edges connecting the
    members, then it will examine the followers sets and find common followers who are highly connectd to many of the
    cluster members. It will then get the followers of these highly connected followers and repeat the process until
    enough steps have been taken or no more followers satisfy the criterion.
    """

    def __init__(self, tokens_ar, user, file_path, id=False):
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
        self.norms = []

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
            self.seed_user = user
            self.clique.add((user, self.level))
            self.to_check.add(user)
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

    def find_bots(self, priors):
        self.users_to_query = set()
        features = {'temporal': {}, 'content': {}, 'user': {}, 'network': {}}
        followers_set = set(self.followers)
        if self.level > 0:
            print "Number of followers: " + str(len(self.followers))
            follower_counts = Counter(self.followers).most_common()
            # should fix this to be a more precise measure
            size_to_keep = int(.08*len(self.followers))
            connectedness_threshold = floor(0.3*self.n)
            print size_to_keep
            print connectedness_threshold
            tmp_followers = [f[0] for f in follower_counts if f[1] > connectedness_threshold]
            print "NUmber of followers over threshold = " + str(len(tmp_followers))
            if len(tmp_followers) < size_to_keep:
                tmp_followers.extend([f[0] for f in follower_counts[:size_to_keep] if f[1] > 1])
            followers_set = set(tmp_followers)
            print "Number of connected followers: " + str(len(followers_set))
        print "Getting all user info..."
        for follower in followers_set:
            user_info = None
            follower = str(follower)
            if follower not in self.users and follower not in self.ignore_users:
                self.cur.execute('SELECT suspended, deleted, other_error, user_info, user_info_json FROM followers WHERE user_id = %s', (follower,))
                record = self.cur.fetchone()
                if record:
                    if record[0] or record[1] or record[2]:
                        self.ignore_users.add(follower)
                        continue
                    if record[3] and not record[4]:
                        self.ignore_users.add(follower)
                        continue
                    if record[3] and record[4]:
                        try:
                            self.user_info[follower] = ast.literal_eval(record[4])
                            continue
                        except:
                            self.ignore_users.add(follower)
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
                        gf.get_user_features()
                        gf.collect_tweets()
                        gf.get_content_features()
                        gf.get_temporal_features()
                        features['temporal'][follower] = gf.temporal_features
                        features['content'][follower] = gf.content_features
                        features['network'][follower] = gf.network_features
                        features['user'][follower] = gf.user_features
                        self.current_level_users.append(follower)
                    except Exception as e:
                        print "ERROR GETTING FEATURES"
                        print e
                        print follower
                        print self.user_info[follower]
                    # need to incorporate other network features
                    #gf.features['num_shared_edges'] = follower_counts[user]

        # we can look at the out-degree of the collapsed ego network. We also calculate the average out degree,
        # which is the average number of followers per follower.
        # need to get the followers for all these
        with open('clique_expansion/' + self.seed_user + '_all_features.p', 'wb') as f:
                pickle.dump(features, f)
        len_priors = len(priors['temporal'])
        current_features = priors
        current_features['temporal'].extend(features['temporal'].values())
        current_features['content'].extend(features['content'].values())
        current_features['network'].extend(features['network'].values())
        current_features['user'].extend(features['user'].values())
        print "Performing anomaly detection"
        X = dict()
        X['temporal'] = self.vec.fit_transform(current_features['temporal']).toarray()
        X['content'] = self.vec.fit_transform(current_features['content']).toarray()
        X['network'] = self.vec.fit_transform(current_features['network']).toarray()
        X['user'] = self.vec.fit_transform(current_features['user']).toarray()
        current_features = dict()
        for key, value in X.iteritems():
            X[key] = normalize(value)

        outliers = self.perform_outlier_detection(X, len_priors)

        self.level += 1
        self.clique_features = {'temporal': {}, 'content': {}, 'user': {}, 'network': {}}
        for follower in outliers:
            self.clique.add((follower, self.level))
            self.to_check.add(follower)
            self.clique_features['content'][follower] = features['content'][follower]
            self.clique_features['network'][follower] = features['network'][follower]
            self.clique_features['user'][follower] = features['user'][follower]
            self.clique_features['temporal'][follower] = features['temporal'][follower]
        features = dict()
        print self.clique
        self.n = float(len(self.clique))
        print "Current size of cluster: " + str(self.n)

    def load_all_find_bots(self, priors):
        features = pickle.load(open('clique_expansion/all_features/' + self.seed_user + '_all_features.p', 'rb'))
        # we can look at the out-degree of the collapsed ego network. We also calculate the average out degree,
        # which is the average number of followers per follower.
        # need to get the followers for all these
        len_priors = len(priors['temporal'])
        for user, value in features['temporal'].iteritems():
            self.current_level_users.append(user)
        current_features = priors
        current_features['temporal'].extend(features['temporal'].values())
        current_features['content'].extend(features['content'].values())
        current_features['network'].extend(features['network'].values())
        current_features['user'].extend(features['user'].values())
        print "Performing anomaly detection"
        X = dict()
        X['temporal'] = self.vec.fit_transform(current_features['temporal']).toarray()
        X['content'] = self.vec.fit_transform(current_features['content']).toarray()
        X['network'] = self.vec.fit_transform(current_features['network']).toarray()
        X['user'] = self.vec.fit_transform(current_features['user']).toarray()
        current_features = dict()
        for key, value in X.iteritems():
            X[key] = normalize(value)
        outliers = self.perform_outlier_detection_all_combos(X, len_priors)
        self.level += 1
        self.clique_features = {'temporal': {}, 'content': {}, 'user': {}, 'network': {}}
        for follower in outliers:
            self.clique.add((follower, self.level))
            self.to_check.add(follower)
            self.clique_features['content'][follower] = features['content'][follower]
            self.clique_features['network'][follower] = features['network'][follower]
            self.clique_features['user'][follower] = features['user'][follower]
            self.clique_features['temporal'][follower] = features['temporal'][follower]
        features = dict()
        print self.clique
        self.n = float(len(self.clique))
        print "Current size of cluster: " + str(self.n)

    def perform_outlier_detection_all_combos(self, X, len_priors):
        # LOF on all features
        scores = {'temporal': {}, 'content': {}, 'user': {}, 'network': {}}
        print "Starting anomaly detection loop"
        for key, value in X.iteritems():
            if key == 'user':
                continue
            print key
            clf = IsolationForest()
            clf.fit(value)
            scores[key]['iforest'] = clf.decision_function(value)
            print "Finished iforest"
            clf = LocalOutlierFactor(n_neighbors=20)
            clf.fit(value)
            scores[key]['lof'] = clf._decision_function(value)
        print "Finished anomaly detection loop"
        with open('clique_expansion/' + self.seed_user + '_unnormalized_scores.csv', 'w') as f:
            for domain, value in scores.iteritems():
                for type_score, all_scores in value.iteritems():
                    f.write(domain + ' ' + type_score + ',')
                    for item in all_scores:
                        f.write(str(item) + ',')
                    f.write('\n')
        combined_scores = self.combine_all(scores)
        scores = None
        new_scores = combined_scores[len_priors:]
        user_scores = sorted(zip(self.current_level_users, new_scores), key=lambda x: x[1], reverse=True)
        threshold = np.percentile(new_scores, 8)
        outliers = [u[0] for u in user_scores if u[1] <= threshold]
        return outliers

    def perform_outlier_detection(self, X, len_priors):
        # LOF on all features
        scores = dict()
        for key, value in X.iteritems():
            if key == 'user':
                clf = IsolationForest()
                clf.fit(value)
                scores[key] = clf.decision_function(value)
            else:
                clf = LocalOutlierFactor(n_neighbors=20)
                clf.fit(value)
                check_is_fitted(clf, ["threshold_", "negative_outlier_factor_", "n_neighbors_", "_distances_fit_X_"])
                if value is not None:
                    value = check_array(value, accept_sparse='csr')
                    scores[key] = clf._decision_function(value)
                else:
                    scores[key] = clf.negative_outlier_factor_
        with open('clique_expansion/' + self.seed_user + '_unnormalized_scores.csv', 'w') as f:
            for domain, all_scores in scores.iteritems():
                for item in all_scores:
                    f.write(str(item) + ',')
                f.write('\n')
        combined_scores = self.combine(scores)
        scores = None
        new_scores = combined_scores[len_priors:]
        user_scores = sorted(zip(self.current_level_users, new_scores), key=lambda x: x[1], reverse=True)
        threshold = np.percentile(new_scores, 95)
        outliers = [u[0] for u in user_scores if u[1] >= threshold]
        return outliers

    def combine(self, scores):
        # For both LOF and random forest, lower scores are outliers, so we do a log transform for both
        sqrt_two = pow(2, 2)
        norms = []
        for key, value in scores.iteritems():
            v_max = max(value)
            v_scores = [v_max - x for x in value]
            v_mean = np.mean(value)
            v_std = np.std(value)
            norms.append([max([(erf((x-v_mean)/(v_std*sqrt_two))), 0]) for x in v_scores])
        with open('clique_expansion/' + self.seed_user + '_normalized_scores.csv', 'w') as f:
            for domain in norms:
                for item in domain:
                    f.write(str(item) + ',')
                f.write('\n')
        return [np.mean(list(x)) for x in zip(*norms)]

    def combine_all(self, scores):
        # For both LOF and random forest, lower scores are outliers, so we do a log transform for both
        sqrt_two = pow(2, 2)
        norms = []
        for key, v in scores.iteritems():
            for outlier_type, value in v.iteritems():
                #v_max = max(value)
                #v_scores = [v_max - x for x in value]
                v_mean = np.mean(value)
                v_std = np.std(value)
                norms.append([max([(erf((x-v_mean)/(v_std*sqrt_two))), 0]) for x in value])
        with open('clique_expansion/' + self.seed_user + '_normalized_scores.csv', 'w') as f:
            for domain in norms:
                for item in domain:
                    f.write(str(item) + ',')
                f.write('\n')
        return [np.mean(list(x)) for x in zip(*norms)]

    def save_features(self):
        self.users_to_query = set()
        features = {'temporal': {}, 'content': {}, 'user': {}, 'network': {}}
        followers_set = set(self.followers)
        if self.level > 0:
            print "Number of followers: " + str(len(self.followers))
            follower_counts = Counter(self.followers).most_common()
            # should fix this to be a more precise measure
            size_to_keep = int(.08*len(self.followers))
            connectedness_threshold = floor(0.3*self.n)
            print size_to_keep
            print connectedness_threshold
            tmp_followers = [f[0] for f in follower_counts if f[1] > connectedness_threshold]
            print "NUmber of followers over threshold = " + str(len(tmp_followers))
            if len(tmp_followers) < size_to_keep:
                tmp_followers.extend([f[0] for f in follower_counts[:size_to_keep] if f[1] > 1])
            followers_set = set(tmp_followers)
            print "Number of connected followers: " + str(len(followers_set))
        print "Getting all user info..."
        for follower in followers_set:
            user_info = None
            follower = str(follower)
            if follower not in self.users and follower not in self.ignore_users:
                self.cur.execute('SELECT suspended, deleted, other_error, user_info, user_info_json FROM followers WHERE user_id = %s', (follower,))
                record = self.cur.fetchone()
                if record:
                    if record[0] or record[1] or record[2]:
                        self.ignore_users.add(follower)
                        continue
                    if record[3] and not record[4]:
                        self.ignore_users.add(follower)
                        continue
                    if record[3] and record[4]:
                        try:
                            self.user_info[follower] = ast.literal_eval(record[4])
                            continue
                        except:
                            self.ignore_users.add(follower)
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
                        gf.get_user_features()
                        gf.collect_tweets()
                        gf.get_content_features()
                        gf.get_temporal_features()
                        features['temporal'][follower] = gf.temporal_features
                        features['content'][follower] = gf.content_features
                        features['network'][follower] = gf.network_features
                        features['user'][follower] = gf.user_features
                        self.current_level_users.append(follower)
                    except Exception as e:
                        print "ERROR GETTING FEATURES"
                        print e
                        print follower
                        print self.user_info[follower]
        with open('clique_expansion/all_features/' + self.seed_user + '_all_features.p', 'wb') as f:
                pickle.dump(features, f)

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
        bv = BotView(tokens_ar, user, file_path, id=True)
        print "Exploring for user " + user
        while bv.to_check:
            print "Exploring..."
            bv.explore()
            bv.load_all_find_bots(priors)
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


def get_features():
    """
    tokens_ar = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
                 "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
                 "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    """
    tokens_ar = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
                 "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]
    files = os.listdir('clique_expansion/all_features/')
    ignore_seeds = set()
    files = [fs for fs in files if 'all_features' in fs]
    if files:
        for i in range(0, len(files)):
            ignore_seeds.add(files[i].split('_')[0])
    print ignore_seeds
    users = {'4827286170', '2868389650', '2770018159', '114494345', '26102775', '2216736289', '286922738', '258240944',
             '148253367', '1514534004'} - ignore_seeds
    users2 = {'454570392', '453507246', '1455286956', '2696294077', '136960509', '4177176192', '4246877778',
              '3942027736', '90379747', '19264063'} - ignore_seeds
    #'19264063' keeps seg faulting
    file_path = '/home/amanda/bigDisk/Twitter/'
    for user in users2:
        bv = BotView(tokens_ar, user, file_path, id=True)
        print "Exploring for user " + user
        bv.explore()
        bv.save_features()

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
    features = {'temporal': [], 'content': [], 'network': [], 'user': []}
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
                gf.get_user_features()
                gf.collect_tweets()
                gf.get_content_features()
                gf.get_temporal_features()
                features['temporal'].append(gf.temporal_features)
                features['content'].append(gf.content_features)
                features['network'].append(gf.network_features)
                features['user'].append(gf.user_features)
    pickle.dump(features, open('priors_feature_list.p', 'wb'))


def get_priors_users():
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
    features = []
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
                gf.get_user_features()
                features.append(gf.user_features)
    priors = pickle.load(open('priors_feature_list.p', 'rb'))
    priors['user'] = features
    pickle.dump(priors, open('priors_feature_list2.p', 'wb'))


def get_files(file_path):
    folders = os.walk(file_path)
    file_names = []
    for f in folders:
        files = [fs for fs in f[2] if 'clstrs' in fs]
        if files:
            for i in range(0, len(files)):
                file_names.append((f[0], files[i]))
    return file_names

#file_names = get_files('/home/amanda/bigDisk/Twitter/Debot2/')
#get_priors_users()
priors = pickle.load(open('priors_feature_list2.p', 'rb'))
seed_users(priors)
#get_features()
