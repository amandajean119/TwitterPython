import psycopg2
from dotenv import load_dotenv
import os
import traceback
import sys
import ast
import json
import time
import botornot


class botornot_scores:

    def __init__(self):
        self.ids = set([line.strip() for line in open('clique_expansion/all_ids_for_debot.csv', 'r')])
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
        for user in self.ids:
            self.cur.execute('SELECT botornot, user_info_json FROM followers WHERE user_id = %s;', (user,))
            f = self.cur.fetchone()
            if f:
                if f[0]:
                    print "already have botornot score"
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
                i += 1
            except Exception as e:
                print "Blocked"
                print e
                self.sleep(j)

    def sleep(self, j):
        time.sleep(900)
        self.query_API(j)

    def quantify_botornot(self):
        true_botornot = 0
        total_queried = 0
        for user in self.ids:
            total_queried += 1
            self.cur.execute('SELECT botornot FROM followers WHERE user_id = %s', (user,))
            f = self.cur.fetchone()
            if f[0]:
                    try:
                        score = ast.literal_eval(f[0])['score']
                        if score > 0.5:
                            print score
                            true_botornot += 1
                    except:
                        continue
                    total_queried += 1
        print true_botornot
        print total_queried

bs = botornot_scores()
bs.query_API(0)
bs.quantify_botornot()
