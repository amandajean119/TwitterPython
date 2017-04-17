import botornot
import psycopg2
from dotenv import load_dotenv
import os
import traceback
import sys
import ast
import json
import time


def load_from_file (f):
    filename = open(f, 'r')
    contents = filename.read()
    filename.close()
    items = [name for name in contents.split('\n') if name]
    return items


def query_API():

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

    tokens_ar = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
                 "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
                 "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    """
    tokens_ar = ["781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n", "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD",
                 "tlUFi9tJGX1NxIA7JWBET2f4K", "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"]
    """
    twitter_app_auth = {
        'consumer_key': tokens_ar[2],
        'consumer_secret': tokens_ar[3],
        'access_token': tokens_ar[0],
        'access_token_secret': tokens_ar[1],
    }
    bon = botornot.BotOrNot(wait_on_ratelimit=True, warn_on_ratelimit=True, **twitter_app_auth)

    f_out = open('clique_expansion/just_LOF/botornot_scores.txt', 'a')
    users = load_from_file('all_no_dups2.txt')
    i = 1
    for user in users:
        print user
        cur.execute('SELECT user_info_json FROM followers WHERE user_id = %s;', (user,))
        f = cur.fetchone()
        if f:
            try:
                user_info = ast.literal_eval(f[0])
                timeline = json.load(open('Debot2/stream/' + user + '_stream.json', 'r'))
            except:
                print "Error"
                continue
        else:
            print "Does not exist in db"
            continue
        if i % 180 == 0:
            print "sleep"
            time.sleep(900)
        try:
            score = bon._check_account(user_info, timeline)
            print score
            f_out.write(str(score) + '\n')
            i += 1
        except:
            print "Blocked"
            time.sleep(700)
            try:
                score = bon._check_account(user_info, timeline)
                print score
                f_out.write(str(score) + '\n')
                i += 1
            except:
                print "Need to sleep more"
                time.sleep(200)
                score = bon._check_account(user_info, timeline)
                print score
                f_out.write(str(score) + '\n')
                i += 1

query_API()