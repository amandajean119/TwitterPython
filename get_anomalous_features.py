import pickle
from scipy import stats
import psycopg2
from dotenv import load_dotenv
import os
import traceback
import sys
import ast
import json
from get_features import GetFeatures
import pandas as pd
from itertools import combinations
from numpy.random import choice
import time
import botornot
import random
import numpy as np
from scipy import stats
import pylab as pl
import pandas as pd
from math import log
from collections import OrderedDict


class Validation:

    def __init__(self, priors=True):
        self.users = dict()
        self.user_features = dict()
        if priors:
            self.priors = pickle.load(open('processed_priors.p', 'rb'))
        self.botornot_scores = dict()
        self.random_users_dict = dict()
        try:
            load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
            username = os.getenv('DATABASE_USER')
            password = os.getenv('DATABASE_PASSWORD')
            conn_string = "dbname='twitter' user=" + username + " password = " + password
            self.con = psycopg2.connect(conn_string)
            self.cur = self.con.cursor()
        except psycopg2.DatabaseError as e:
            print 'Error: ' + str(e)
            traceback.print_exc()
            sys.exit(1)
        self.tokens_ar = [["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
                           "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
                           "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"],
                          ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n",
                           "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD","tlUFi9tJGX1NxIA7JWBET2f4K",
                           "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]]

    def collect_users(self):
        folders = os.walk('/home/amanda/bigDisk/Twitter/clique_expansion/')
        bot_sizes = dict()
        for f in folders:
            exp_type = f[0].split('/')[-1]
            if exp_type not in ['LOF', 'LOF_domain', 'all_four_domain', 'all_four']:
            #if exp_type not in ['all_four_domain',  'random_seeds']:
            #if exp_type not in ['random_seeds']:
                continue
            self.users[exp_type] = dict()
            bot_sizes[exp_type] = []
            files = [fs for fs in f[2] if 'clique_expansion.csv' in fs]
            if files:
                for i in range(0, len(files)):
                    seed_user = files[i].split('_')[0]
                    self.users[exp_type][seed_user] = set()
                    with open(f[0] + '/' + files[i], 'r') as user_file:
                        for line in user_file:
                            self.users[exp_type][seed_user].add(line.split(',')[0])
                    bot_sizes[exp_type].append(len(self.users[exp_type][seed_user]))
        json.dump(bot_sizes, open('clique_expansion/size_statistics.json', 'w'), indent=4, separators=(',', ': '))

    def collect_users_long_experiments(self):
        bot_sizes = dict()
        with open('clique_expansion/long_experiment/followers_round_0.csv', 'r') as f:
            users = [l.strip().split(',')[0] for l in f]
            exp_type = 'expl_2'
            bot_sizes[exp_type] = []
            self.users[exp_type] = dict()
            files = os.listdir('clique_expansion/long_experiment')
            files = [fs for fs in files if 'clique_expansion.csv' in fs]
            if files:
                for i in range(0, len(files)):
                    seed_user = files[i].split('_')[0]
                    if seed_user in users:
                        self.users[exp_type][seed_user] = set()
                        with open('clique_expansion/long_experiment/' + files[i], 'r') as user_file:
                            for line in user_file:
                                self.users[exp_type][seed_user].add(line.split(',')[0])
                        bot_sizes[exp_type].append(len(self.users[exp_type][seed_user]))


        with open('clique_expansion/long_experiment/followers_round_1.csv', 'r') as f:
            users = [l.strip().split(',')[0] for l in f]
            exp_type = 'expl_3'
            self.users[exp_type] = dict()
            bot_sizes[exp_type] = []
            files = os.listdir('clique_expansion/long_experiment')
            files = [fs for fs in files if 'clique_expansion.csv' in fs]
            if files:
                for i in range(0, len(files)):
                    seed_user = files[i].split('_')[0]
                    if seed_user in users:
                        self.users[exp_type][seed_user] = set()
                        with open('clique_expansion/long_experiment/' + files[i], 'r') as user_file:
                            for line in user_file:
                                self.users[exp_type][seed_user].add(line.split(',')[0])
                        bot_sizes[exp_type].append(len(self.users[exp_type][seed_user]))
        json.dump(bot_sizes, open('clique_expansion/size_statistics_expl_2_3.json', 'w'), indent=4, separators=(',', ': '))


    def create_file_for_Debot(self):
        users_set = set()
        for exp_type, value in self.users.iteritems():
            for seed_user, users in value.iteritems():
                for user in users:
                    users_set.add(user)
        with open('clique_expansion/random_seed_ids_expl1_2_for_debot.csv', 'w') as f:
            for user in users_set:
                f.write(user + '\n')

    def check_features(self):

        files = os.listdir('clique_expansion/all_features/')
        if files:
            for file in files:
                features = pickle.load(open('clique_expansion/all_features/' + file, 'rb'))
                # we can look at the out-degree of the collapsed ego network. We also calculate the average out degree,
                # which is the average number of followers per follower.
                # need to get the followers for all these
                for key, value in features.iteritems():
                    for user, f in value.iteritems():
                        if user in self.user_features:
                            self.user_features[user].update(f)
                        else:
                            self.user_features[user] = f
        features_list = self.user_features.values()
        features_list_tmp = []
        for item in features_list:
            tmp_dict = {}
            for key, value in item.iteritems():
                if not isinstance(value, basestring):
                    tmp_dict[key] = value
            features_list_tmp.append(tmp_dict)
        pd.DataFrame(features_list_tmp).to_csv(open('clique_expansion/expl_1_features.csv', 'w'), index=False)

        scores = dict()
        score_counts = dict()
        for exp_type, value in self.users.iteritems():
            score_counts[exp_type] = []
            for seed_user, users in value.iteritems():
                for user in users:
                    score_tmp = 0
                    if user not in self.user_features:
                        if user != seed_user:
                            print user
                            print exp_type
                            print seed_user
                        continue
                    scores[user] = dict()
                    features = self.user_features[user]
                    for k, v in features.iteritems():
                        if k in self.priors:
                            score = stats.percentileofscore(self.priors[k], v)
                            if score <= 10 or score >= 90:
                                scores[user][k] = score
                                score_tmp += 1
                    score_counts[exp_type].append(score_tmp)
                json.dump(scores, open('clique_expansion/feature_analysis/' + exp_type + '_' + seed_user +
                                       '_feature_scores.json', 'w'), indent=4, separators=(',', ': '))

        json.dump(score_counts, open('clique_expansion/expl_2_bots_scores.csv', 'w'))

    def quantify_extreme_features(self):
        # examining the number of outlying features for the outliers found with each outlier detection experiment
        # also examining the number of outliers per feature
        # need to test and walk through

        num_extreme_features = dict()
        # most_extreme_features = dict()
        for exp_type, value in self.users.iteritems():
            num_extreme_features[exp_type] = []
            for seed_user, users in value.iteritems():
                scores = json.load(open('clique_expansion/feature_analysis/' + exp_type + '_' + seed_user +
                                        '_feature_scores.json', 'r'))
                for user, features in scores.iteritems():
                    num_extreme_features[exp_type].append(len(features))
                    '''
                    for feature_type, feature_value in features.iteritems():
                        if feature_type in most_extreme_features:
                            most_extreme_features[feature_type].append(feature_value)
                        else:
                            most_extreme_features[feature_type] = [feature_value]
                            '''

        #json.dump(most_extreme_features, open('clique_expansion/most_extreme_features.json', 'w'), indent=4, separators=(',', ': '))
        json.dump(num_extreme_features, open('clique_expansion/num_extreme_features.json', 'w'), indent=4, separators=(',', ': '))

        # most_extreme_features = json.load(open('clique_expansion/most_extreme_features.json', 'r'))
        # num_extreme_features = json.load(open('clique_expansion/num_extreme_features.json', 'r'))

    def quantify_botornot(self):
        ids = set()
        lof_botornot_ids = json.load(open('clique_expansion/random_users_for_botornotlof.json', 'r'))['LOF']
        #got LOF ids
        for key, value in lof_botornot_ids.iteritems():
            ids.update(set(value))
        true_botornot = 0
        total_queried = 0
        for user in ids:
            self.cur.execute('SELECT botornot FROM followers WHERE user_id = %s', (user,))
            f = self.cur.fetchone()
            if f[0]:
                try:
                    score = ast.literal_eval(f[0])['score']
                    if score > 0.5:
                        true_botornot += 1
                except:
                    continue
                total_queried += 1
        print 'LOF'
        print true_botornot
        print total_queried
        ids = set()
        lof_domain_botornot_ids = json.load(open('clique_expansion/random_users_for_botornot_lof_ensemble.json', 'r'))['LOF_domain']
        # got LOF ids
        for key, value in lof_domain_botornot_ids.iteritems():
            ids.update(set(value))
        true_botornot = 0
        total_queried = 0
        for user in ids:
            self.cur.execute('SELECT botornot FROM followers WHERE user_id = %s', (user,))
            f = self.cur.fetchone()
            if f[0]:
                try:
                    score = ast.literal_eval(f[0])['score']
                    if score > 0.5:
                        true_botornot += 1
                except:
                    continue
                total_queried += 1
        print 'LOF_domain'
        print true_botornot
        print total_queried
        ids = set()
        ensemble_botornot_ids = json.load(open('clique_expansion/random_users_for_botornot_ensemble.json', 'r'))
        # got LOF ids
        for key, value in ensemble_botornot_ids.iteritems():
            for users in value.values():
                ids.update(set(users))
        true_botornot = 0
        total_queried = 0
        for user in ids:
            self.cur.execute('SELECT botornot FROM followers WHERE user_id = %s', (user,))
            f = self.cur.fetchone()
            if f[0]:
                try:
                    score = ast.literal_eval(f[0])['score']
                    if score > 0.5:
                        true_botornot += 1
                except:
                    continue
                total_queried += 1
        print 'ensemble'
        print true_botornot
        print total_queried
        ids = set()


    def quantify_debot(self):

        #debot_ids = [line.strip() for line in open('clique_expansion/all_ids_for_debot.csv', 'r')]
        for key, value in self.users.iteritems():
            true_debot = 0
            total_queried = 0
            for seed_user, users in value.iteritems():
                for user in users:
                    total_queried += 1
                    self.cur.execute('SELECT bot, new_debot FROM followers WHERE user_id = %s', (user,))
                    f = self.cur.fetchone()
                    if f[0] or f[1]:
                        true_debot += 1
            print key
            print true_debot
            print total_queried

    def get_URLS(self):
        urls = dict()
        ids = set()
        for exp_typ, value in self.users.iteritems():
            urls[exp_typ] = dict()
            for seed_user, users in value.iteritems():
                urls[exp_typ][seed_user] = []
                i = 0
                users = list(users)
                while i < 10:
                    index = random.randrange(len(users))
                    user = users[index]
                    while user in ids:
                        index = random.randrange(len(users))
                        user = users[index]
                    ids.add(user)
                    del users[index]
                    i += 1
                    self.cur.execute('SELECT user_info_json FROM followers WHERE user_id = %s;', (user,))
                    f = self.cur.fetchone()
                    if f:
                        try:
                            user_info = ast.literal_eval(f[0])
                            screen_name = user_info['screen_name']
                            url = 'https://twitter.com/' + screen_name
                            urls[exp_typ][seed_user].append(url)
                        except Exception as e:
                            print e
                            continue
        json.dump(urls, open('clique_expansion/expl_1_2_URLs.json', 'w'),  indent=4, separators=(',', ': '))

    def find_bots_following_bots(self):
        bot_follows_bot = 0
        all_bots = 0
        num_bots_in_followers = []
        self.cur.execute('SELECT followers FROM name_id WHERE cast (user_id as bigint) IN (SELECT user_id FROM followers WHERE bot = true);')
        f = self.cur.fetchall()
        for record in f:
            if record[0] and record[0] != '[]':
                followers_list = ast.literal_eval(record[0])
                tmp = 0
                all_bots += 1
                for follower in followers_list:
                    self.cur.execute('SELECT bot FROM followers WHERE user_id = %s;', (follower,))
                    f = self.cur.fetchone()
                    if f:
                        if f[0]:
                            bot_follows_bot += 1
                            tmp += 1
                num_bots_in_followers.append(tmp)
        print bot_follows_bot
        print all_bots
        with open('clique_expansion/num_bots_in_followers.csv', 'w') as f:
            for num in num_bots_in_followers:
                f.write(str(num) + '\n')


    def get_Jaccard(self):

        user_sets = dict()
        common_core = dict()
        for exp_type, value in self.users.iteritems():
            for seed_user, users in value.iteritems():
                if seed_user in user_sets:
                    user_sets[seed_user].append((exp_type, users))
                else:
                    user_sets[seed_user] = [(exp_type, users)]
                if seed_user in common_core:
                    common_core[seed_user].update(users)
                else:
                    common_core[seed_user] = users
        shared_bots = set()
        for key, value in common_core.iteritems():
            common_core[key] = list(value)
            shared_bots.update(value)
        with open('clique_expansion/common_core.csv', 'w') as f:
            for bot in shared_bots:
                f.write(bot + '\n')
        json.dump(common_core, open('clique_expansion/common_cores.json', 'w'), indent=4, separators=(',', ': '))

        user_jaccards = {}
        for seed_user, value in user_sets.iteritems():
            combos = list(combinations(value, 2))
            jaccards = {str((a[0], b[0])): jaccard((a[1], b[1])) for a, b in combos}
            user_jaccards[seed_user] = jaccards
        json.dump(user_jaccards, open('clique_expansion/jaccards.json', 'w'), indent=4, separators=(',', ': '))
        '''
        user_jaccards = json.load(open('clique_expansion/jaccards.json', 'r'))
        jaccards_by_exp = dict()
        for seed_user, value in user_jaccards.iteritems():
            for exp_combo, jac_value in value.iteritems():
                if exp_combo in jaccards_by_exp:
                    jaccards_by_exp[exp_combo].append(jac_value)
                else:
                    jaccards_by_exp[exp_combo] = [jac_value]
        # also should find the core of each and profile it
        fig = pl.figure()
        for key, value in jaccards_by_exp.iteritems():
            key = ast.literal_eval(key)
            data = sorted(value)
            # h_bot = [log(x + 1, 2) for x in h_bot]
            # fit_data = stats.norm.pdf(data, np.mean(data), np.std(data))
            # pl.plot(data, fit_data, '-o')
            if key == ("all_four", "LOF"):
                label = 'LOF and Ensemble'
            elif key == ("all_four", "all_four_domain"):
                label = 'Ensemble and Ensemble across domains'
            else:
                label = 'LOF and Ensemble across domain'

            pl.hist(data, normed=True, bins=10, label=label, alpha=0.5)
        pl.ylabel('Frequency', fontsize=16)
        pl.xlabel("Jaccard similarity of clusters", fontsize=16)
        # axes = pl.gca()
        # axes.set_xlim([min(h_bot[0], h_user[0]), max(h_bot[-1], h_bot[-1])])
        pl.legend(loc='upper right')
        pl.show()
        fig.savefig('jaccard_clusters.eps', dpi=60)
        '''

    def pick_for_botornot(self):

        for exp_typ, value in self.users.iteritems():
            self.random_users_dict[exp_typ] = dict()
            for seed_user, users in value.iteritems():
                print len(users)
                if len(users) <= 50:
                    random_users = users
                else:
                    random_users = choice(list(users), 50, replace=False)
                self.random_users_dict[exp_typ][seed_user] = list(random_users)
        json.dump(self.random_users_dict, open('clique_expansion/random_users_for_botornot_level3.json', 'w'), indent=4, separators=(',', ': '))

    def query_API(self, j):

        tokens_ar = self.tokens_ar[j]
        if j == 0:
            j = 1
        else:
            j = 0
        twitter_app_auth = {
            'consumer_key': tokens_ar[2],
            'consumer_secret': tokens_ar[3],
            'access_token': tokens_ar[0],
            'access_token_secret': tokens_ar[1],
        }
        bon = botornot.BotOrNot(wait_on_ratelimit=True, warn_on_ratelimit=True, **twitter_app_auth)
        i = 1
        for exp_typ, value in self.random_users_dict.iteritems():
            self.botornot_scores[exp_typ] = dict()
            for seed_user, random_users in value.iteritems():
                self.botornot_scores[exp_typ][seed_user] = {}
                k = 0
                for user in random_users:
                    if k > 50:
                        break
                    k += 1
                    print k
                    self.cur.execute('SELECT botornot, user_info_json FROM followers WHERE user_id = %s;', (user,))
                    f = self.cur.fetchone()
                    if f:
                        if f[0]:
                            print "already have botornot score"
                            self.botornot_scores[exp_typ][seed_user][user] = ast.literal_eval(f[0])
                            continue
                        try:
                            user_info = ast.literal_eval(f[1])
                            timeline = json.load(open('Debot2/stream/' + user + '_stream.json', 'r'))
                        except:
                            print "Error"
                            print user
                            continue
                    else:
                        print "User " + user + " does not exist in db"
                        continue
                    if i % 180 == 0:
                        print "sleep"
                        self.sleep(j)
                    try:
                        score = bon._check_account(user_info, timeline)
                        print score
                        self.cur.execute('UPDATE followers SET botornot = %s WHERE user_id = %s', (str(score), user))
                        self.con.commit()
                        self.botornot_scores[exp_typ][seed_user][user] = score
                        i += 1
                    except Exception as e:
                        print "Blocked"
                        print e
                        self.sleep(j)

    def get_botornot_for_paper(self):
        for exp_type, value in self.users.iteritems():
            print exp_type
            bots = 0
            total = 0
            for seed_user, random_users in value.iteritems():
                for user in random_users:
                    self.cur.execute('SELECT botornot, user_info_json FROM followers WHERE user_id = %s;', (user,))
                    f = self.cur.fetchone()
                    if f:
                        if f[0]:
                            score = ast.literal_eval(f[0])['score']
                            if score > 0.5:
                                bots += 1
                            total += 1
            print bots
            print total
            print str(float(bots)/float(total))

    def sleep(self, j):
        time.sleep(900)
        self.query_API(j)

    def process_debot_results(self):
        # get all odd columns because those contain the user_ids
        users_set = set()
        with open('clique_expansion/debot_results_part5.txt', 'r') as f:
            for line in f:
                line = line.strip().split(',')[1::2]
                for item in line:
                    users_set.add(item)
        for user in users_set:
            self.cur.execute('UPDATE followers SET new_debot = TRUE where user_id = %s', (user,))
            self.con.commit()
            print user
        '''
        users_set = set()
        with open('clique_expansion/debot_id_results.csv', 'r') as f:
            for line in f:
                line = line.strip().split(',')
                if line[1] == '1':
                    users_set.add(line[0])
        for user in users_set:
            self.cur.execute('UPDATE followers SET bot = TRUE where user_id = %s', (user,))
            self.con.commit()
            '''

    def get_size_data(self):
        self.cur.execute("SELECT followers FROM name_id WHERE followers IS NOT NULL AND followers <> '[]';")
        f = self.cur.fetchall()
        total_followers = 0
        total_with_followers = 0
        for item in f:
            followers_num = len(ast.literal_eval(item[0]))
            total_followers += followers_num
            total_with_followers += 1
        print total_followers
        print total_with_followers

def check_priors_features():
    unprocessed_priors = pickle.load(open('priors_feature_list2.p', 'rb'))
    processed_priors = pickle.load(open('processed_priors.p', 'rb'))
    priors_scores = []
    for key, value in unprocessed_priors.iteritems():
        for item in value:
            scores = 0
            for key, value in item.iteritems():
                if key in processed_priors:
                    score = stats.percentileofscore(processed_priors[key], value)
                    if score <= 10 or score >= 90:
                        scores += 1
            priors_scores.append(scores)
    data = sorted(priors_scores)
    with open('priors_scores.csv', 'w') as f:
        for item in priors_scores:
            f.write(str(item) + '\n')
    pl.hist(data, normed=True, bins=20, alpha=0.5)

    pl.ylabel('Frequency', fontsize=16)
    pl.xlabel("Number of outlying features", fontsize=16)
    pl.show()
    #fig.savefig('outlyingfeatures30normed.eps', dpi=60)


def jaccard((a, b)):
    a = set(a)
    b = set(b)
    try:
        return float(len(a & b))/float(len(a | b))
    except:
        return 0

def process_priors():
    unprocessed_priors = pickle.load(open('priors_feature_list2.p', 'rb'))
    priors = {}
    for key, value in unprocessed_priors.iteritems():
        for item in value:
            for key, value in item.iteritems():
                if isinstance(value, basestring):
                    continue
                if key in priors:
                    priors[key].append(value)
                else:
                    priors[key] = [value]
    pickle.dump(priors, open('processed_priors.p', 'wb'))


def add_bon_scores_to_db():
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
    with open('clique_expansion/botornot_scores.txt', 'r') as f:
        for line in f:
            try:
                l = ast.literal_eval(line)
                user_id = l['meta']['user_id']
                cur.execute('UPDATE followers SET botornot = %s WHERE user_id = %s', (str(l), user_id))
                con.commit()
            except Exception as e:
                print e
                print line
                continue


#va = Validation()
#print "Validation object created"
#va.collect_users_long_experiments()
#va.collect_users()
#print "Users collected"
#va.check_features()
#print "Extreme feature values calculated and saved"

va = Validation(priors=False)
#va.collect_users_long_experiments()

#va.get_size_data()
#print "Validation object created"
#va.find_bots_following_bots()
va.collect_users()
#va.get_botornot_for_paper()
#print "Users collected"
#va.check_features()
#va.create_file_for_Debot()
# print "Debot file created
#va.get_URLS()
#va.quantify_extreme_features()
#print "URLs generated and random samples selected"
# should double check jaccard did right thing
#va.get_Jaccard()
# print "Jaccard similarities generated"
#va.pick_for_botornot()
#va.random_users_dict = json.load(open('clique_expansion/random_users_for_botornot_level3.json', 'r'))
#va.query_API(0)
# json.dump(va.botornot_scores, open('clique_expansion/bot_or_not_scores.json', 'w'), indent=4, separators=(',', ': '))
#print "botornot scores gathered"
#va.process_debot_results()
#va.quantify_debot()
#va.quantify_botornot()
#va.get_botornot_for_paper()
#print "Debot results added to db"
#add_bon_scores_to_db()
#va.quantify_debot_and_botornot()


# check_priors_features()
