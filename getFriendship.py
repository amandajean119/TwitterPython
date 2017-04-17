import datetime
import json
import time
import traceback
from itertools import product

import tweepy


class GetFriendship:
    """


    """

    def __init__(self, tokens_ar):
        self.access_token = tokens_ar[0]
        self.access_token_secret = tokens_ar[1]
        self.consumer_key = tokens_ar[2]
        self.consumer_secret = tokens_ar[3]

        # This handles Twitter authentification and the connection to Twitter Streaming API
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, parser=tweepy.parsers.JSONParser())
        self.users_friendship = {}
        self.start_time = None
        self.files = []
        self.user_clusters = []
        self.users = set()

    def load_from_file(self, f):
        filename = open(f, 'r')
        contents = filename.read()
        filename.close()
        items = [name for name in contents.split('\n') if name]
        for item in items:
            item = sorted(item.strip(';').split(';'))
            self.user_clusters.append(item)

    def sleep_time(self):
        cur_time = datetime.datetime.now()
        diff = (cur_time - self.start_time).total_seconds()
        sleep_time = 900 - diff
        print "sleep time!: " + str(sleep_time)
        time.sleep(sleep_time)
        self.start_time = datetime.datetime.now()

    def query_api(self, u_pair):
        usera = u_pair[0]
        userb = u_pair[1]
        try:
            a = self.api.show_friendship(source_screen_name=usera, target_screen_name=userb)
            self.users_friendship[(usera, userb)] = a
        except tweepy.RateLimitError:
            traceback.print_exc()
            self.sleep_time()
            self.query_api(usera, userb)
        except tweepy.TweepError:
            traceback.print_exc()
            print '>>>>>>>>>>>>>>> exception: ' + usera + ' ' + userb
            self.users_friendship[(usera, userb)] = None

    def find_friendship(self):
        # Variables that contains the user credentials to access Twitter API:
        # ============================== Add your Twitter keys here.
        self.start_time = datetime.datetime.now()
        k = 0
        user_pairs = []
        for users in self.user_clusters:
            i = 0
            j = 1
            b = []
            while i < len(users):
                tmp1 = users[i]
                j = i + 1
                while j < len(users):
                    tmp2 = users[j]
                    b.append((tmp1, tmp2))
                    j += 1
                i += 1
            user_pairs.append(b)
        for u_pairs in user_pairs:
            for u_pair in u_pairs:
                if u_pair in self.users_friendship:
                    continue
                if (k + 1) % 14 != 0:
                    self.query_api(u_pair)
                else:
                    self.sleep_time()
                    self.query_api(u_pair)
                k += 1
        uf = {str(k): v for k, v in self.users_friendship.iteritems()}
        json.dump(uf, open('bots_friendships.json', 'w'), indent=4, sort_keys=True, separators=(',', ': '))


if __name__ == '__main__':
    tokens = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
              "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
              "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
    g_u_f = GetFriendship(tokens)
    # json.dump(g_u_f.users_followers, open('test.json', 'w'), indent=4, sort_keys=True, separators=(',', ': '))
    g_u_f.load_from_file('/home/amanda/bigDisk/Twitter/subset_clusters.txt')
    #g_u_f.load_from_file('/home/amanda/bigDisk/Twitter/testcluster.txt')
    g_u_f.find_friendship()
