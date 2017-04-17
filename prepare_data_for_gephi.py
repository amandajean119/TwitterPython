import ast
import json
import tweepy
import traceback
import psycopg2
from dotenv import load_dotenv
import os
import sys

def create_files():

    cluster_follows = {}
    user_ids = set()
    with open('data_for_gephi.csv', 'r') as f:
        for line in f:
            line = line.split(';')
            try:
                user_id = line[0]
                followers = line[1] #ast.literal_eval(line[1])
                clusters = line[2].split(',')
                user_ids.add(user_id)
                for cluster in clusters:
                    if cluster in cluster_follows:
                        cluster_follows[cluster].append((user_id, followers))
                    else:
                        cluster_follows[cluster] = [(user_id, followers)]
            except:
                print "exception"
    
    i = 0
    tmp_dict = {}
    for key, value in cluster_follows.iteritems():
        if len(value) > 20:
            tmp_dict[key] = value
            i += 1
    print i
    json.dump(tmp_dict, open('clusters_of_interest.json', 'w'), indent=4, separators=(',', ': '))
    cluster_follows = tmp_dict

    cluster_follows = json.load(open('clusters_of_interest.json', 'r'))
    users = set()
    with open('/home/amanda/bigDisk/Dropbox/gephi_users.csv', 'w') as f:
        f.write('Id,Cluster\n')
        for key, value in cluster_follows.iteritems():
            for user, followers in value:
                users.add(user)
                f.write(user + ',' + key + '\n')
    
    i = 0
    tokens_ar = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
                 "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]
    access_token = tokens_ar[0]
    access_token_secret = tokens_ar[1]
    consumer_key = tokens_ar[2]
    consumer_secret = tokens_ar[3]
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
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
    with open('/home/amanda/bigDisk/Dropbox/gephi_edges.csv', 'w') as f:
        f.write('Source,Target\n')
        for key, value in cluster_follows.iteritems():
            for user, followers in value:
                try:
                    followers = ast.literal_eval(followers)
                    if len(followers) == 5000:
                        followers = query_api(user, cur, con, api)
                    for follower in followers:
                        follower = str(follower)
                        if follower in users:
                            f.write(user + ',' + follower + '\n')
                except Exception as e:
                    print e
                    print followers
                    continue
    print i
    

def query_api(user, cur, con, api):
    """
    Query Twitter API for the followers of a given user. Add this entry to user_followers, add to followers, and
    add to database
    :param user: The user of interest
    :return:
    """
    try:
        followers = []
        for page in tweepy.Cursor(api.followers_ids, user_id=user).pages():
            followers.extend(page)
        cur.execute('SELECT * FROM name_id WHERE user_id = %s;', (str(user),))
        f = cur.fetchone()
        if f:
            cur.execute('UPDATE name_id SET followers = (%s) WHERE user_id = %s', (str(followers), str(user)))
        else:
            cur.execute('INSERT INTO name_id (user_id, followers) VALUES (%s, %s);', (str(user), str(followers)))
        con.commit()
        print len(followers)
        return followers
    except tweepy.TweepError:
        traceback.print_exc()
        return []

create_files()