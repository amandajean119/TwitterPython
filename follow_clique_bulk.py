import os
import psycopg2
from dotenv import load_dotenv
import sys
import traceback
import tweepy
import time
import datetime
from collections import Counter
import ast


class FollowClique:
    """
    This class starts with a temporal bot cluster. If the cluster has above a minimum number of edges connecting the
    members, then it will examine the followers sets and find common followers who are highly connectd to many of the
    cluster members. It will then get the followers of these highly connected followers and repeat the process until
    enough steps have been taken or no more followers satisfy the criterion.
    """

    def __init__(self, tokens_ar, cluster):
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
        self.users_followers = {}
        # followers is the list of all followers for all nodes in to_check. Resets every round.
        self.followers = []

        self.ignore_users = set()
        try:
            load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
            username = os.getenv('DATABASE_USER')
            password = os.getenv('DATABASE_PASSWORD')
            conn_string = "dbname='twitter' user=" + username + " password = " + password
            self.con = psycopg2.connect(conn_string)
            self.cur = self.con.cursor()
        except psycopg2.DatabaseError as e:
            print 'Error %s' % e
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
        self.users_followers = {}
        self.start_time = None

        self.n = float(len(cluster))
        self.original_n = self.n
        self.level = 0
        for user in cluster:
            self.cur.execute('SELECT user_id FROM name_id WHERE screen_name = %s;', (str(user),))
            f = self.cur.fetchone()
            if f:
                self.clique.add((f[0], self.level))
                self.to_check.add(f[0])
            else:
                print "Don't have id for: " + user
                self.clique.add((user, self.level))
                self.to_check.add(user)

    def explore(self):
        """
        Pops items from to_check and adds their followers to user_followers
        :return:
        """
        i = 0
        # Need to reset followers and user_followers for this round
        self.followers = []
        self.users_followers = {}
        self.start_time = datetime.datetime.now()
        while self.to_check:
            user = self.to_check.pop()
            # if we haven't already found the followers for this user
            if user not in self.users_followers and user not in self.ignore_users:
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
                            print user
                            sys.exit(1)
                        self.users_followers[user] = followers
                        self.followers.extend(followers)
                        continue
                # Otherwise we query the Twitter API for this user's followers
                self.cur.execute('SELECT deleted, suspended FROM followers WHERE user_id = %s;', (str(user),))
                f = self.cur.fetchone()
                if f:
                    if f[0] == True or f[1] == True:
                        self.ignore_users.add(user)
                        continue
                #if (i + 1) % 14 != 0:
                self.query_api_no_friendship(user)
                # If we have queried too recently, need to sleep and try again
                '''
                else:
                    self.sleep_time()
                    self.query_api_no_friendship(user)
                    '''
                i += 1

    def sleep_time(self):
        """
        Sleep for a bit
        :return:
        """
        cur_time = datetime.datetime.now()
        diff = (cur_time - self.start_time).total_seconds()
        sleep_time = 900 - diff
        print "sleep time!: " + str(sleep_time)
        try:
            time.sleep(sleep_time)
        except:
            time.sleep(900)
        self.start_time = datetime.datetime.now()

    def query_api_no_friendship(self, user):
        """
        Query Twitter API for the followers of a given user. Add this entry to user_followers, add to followers, and
        add to database
        :param user: The user of interest
        :return:
        """
        try:
            followers = self.api.followers_ids(user)
            self.users_followers[user] = followers
            self.followers.extend(followers)
            self.cur.execute('SELECT * FROM name_id WHERE user_id = %s;', (str(user),))
            f = self.cur.fetchone()
            if f:
                self.cur.execute('UPDATE name_id SET followers = (%s) WHERE user_id = %s', (str(followers), str(user)))
            else:
                self.cur.execute('INSERT INTO name_id (user_id, followers) VALUES (%s, %s);', (str(user), str(followers)))
            self.con.commit()
            print "Added followers for " + str(user)
        except tweepy.RateLimitError:
            traceback.print_exc()
            self.sleep_time()
            self.query_api_no_friendship(user)
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

    def find_connected_followers(self):
        follower_counts = Counter(self.followers).most_common()
        # Keep only highly connected followers
        # Can adjust this threshold, NEED TO EXPLORE
        connectedness_threshold = .6*self.n
        connected_followers = [f[0] for f in follower_counts if f[1] >= connectedness_threshold]
        self.level += 1
        # Add highly connected followers to the clique and to_check
        for follower in connected_followers:
            self.clique.add((follower, self.level))
            self.to_check.add(follower)
        print self.clique
        self.n = float(len(self.clique))
        print "Current size of cluster: " + str(self.n)


def aggregate_users(file_names):
    clusters = []
    for file_path, file_name in file_names:
        with open(file_path + '/' + file_name, 'r') as f:
            for line in f:
                clusters.append((line.strip().split(','), file_path))

    tokens_ar = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
                 "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
                 "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    '''
    tokens_ar = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
                 "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]
    '''
    for cluster, file_path in clusters:
        fc = FollowClique(tokens_ar, cluster)
        print "Original size of cluster: " + str(fc.original_n)
        if fc.original_n < 12:
            continue
        while fc.to_check:
            fc.explore()
            fc.find_connected_followers()
        clique = sorted(list(fc.clique), key=lambda c: c[1])
        if fc.n == fc.original_n:
            print "cluster did not grow!"
        else:
            with open(file_path + '/clique_expansion' + '.csv', 'a') as f:
                print file_path
                for id, level in clique:
                    f.write(str(id) + ',' + str(int(level)) + '\n')


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
aggregate_users(file_names)
