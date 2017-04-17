import os
import psycopg2
from dotenv import load_dotenv
import sys
import traceback
import tweepy
import ast

def explore(users):
    """
    Pops items from to_check and adds their followers to user_followers
    :return:
    """
    
    for user in users:
        if user not in ignore_users and user not in got_followers:
            cur.execute('SELECT deleted, suspended, other_error FROM followers WHERE screen_name = %s;', (str(user),))
            f = cur.fetchone()
            if f:
                if f[0] or f[1] or f[2]:
                    ignore_users.add(user)
                    continue
            cur.execute("SELECT followers FROM name_id WHERE screen_name = %s;", (str(user),))
            f = cur.fetchone()
            # If we have queried this user in the past it will be in the db, so we don't have to waste a query on it
            if f:
                if f[0]:
                    if f[0] == '[]':
                        ignore_users.add(user)
                        continue
                    try:
                        followers = ast.literal_eval(f[0])
                    except ValueError:
                        ignore_users.add(user)
                        continue
                    got_followers.add(user)
                    for follower in followers:
                        if follower in users:
                            followers.append((user, follower))
                    continue
            # Otherwise we query the Twitter API for this user's followers
            query_api(user)
    with open('more_edges_for_gephi.csv', 'w') as fp:
        fp.write('source,target\n')
        for user1, user2 in followers:
            fp.write(user1 + ',' + user2 + '\n')

def query_api(user):
    """
    Query Twitter API for the followers of a given user. Add this entry to user_followers, add to followers, and
    add to database
    :param user: The user of interest
    :return:
    """
    try:
        # add in cursor to get all followers
        followers = api.followers_ids(user)
        got_followers.add(user)
        followers.extend(followers)
        cur.execute('SELECT * FROM name_id WHERE screen_name = %s;', (str(user),))
        f = cur.fetchone()
        if f:
            cur.execute('UPDATE name_id SET followers = (%s) WHERE screen_name = %s', (str(followers), str(user)))
        else:
            cur.execute('INSERT INTO name_id (screen_name, followers) VALUES (%s, %s);', (str(user), str(followers)))
        con.commit()
    except tweepy.TweepError:
        traceback.print_exc()
        print '>>>>>>>>>>>>>>> exception: ' + str(user)
        ignore_users.add(user)
        cur.execute('SELECT * FROM name_id WHERE screen_name = %s;', (str(user),))
        f = cur.fetchone()
        if f:
            cur.execute('UPDATE name_id SET followers = (%s) WHERE screen_name = %s', ('[]', str(user)))
        else:
            cur.execute('INSERT INTO name_id (screen_name, followers) VALUES (%s, %s);', (str(user), '[]'))
        con.commit()

tokens_ar = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
                 "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]
access_token = tokens_ar[0]
access_token_secret = tokens_ar[1]
consumer_key = tokens_ar[2]
consumer_secret = tokens_ar[3]
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

ignore_users = set()
got_followers = set()
followers = []
try:
    load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
    username = os.getenv('DATABASE_USER')
    password = os.getenv('DATABASE_PASSWORD')
    conn_string = "dbname='twitter' user=" + username + " password = " + password
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
except psycopg2.DatabaseError as e:
    print 'Error: ' + str(e)
    traceback.print_exc()
    sys.exit(1)
cluster_follows = {}
user_ids = set()
with open('data_for_gephi.csv', 'r') as f:
    for line in f:
        line = line.split(',')
        user_id = line[0]
        followers = ast.literal_eval(line[1])
        clusters = line[2].split(',')
        user_ids.add(user_id)
        for cluster in clusters:
            if cluster in cluster_follows:
                cluster_follows[cluster].update({user_id: followers})
            else:
                cluster_follows[cluster] = {user_id: followers}

explore(users)
