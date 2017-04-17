from __future__ import division
import json
import sys
import os
from datetime import *
import numpy as np
from tweet import Tweet
import psycopg2
from dotenv import load_dotenv
import ast
import math
import matplotlib.pyplot as plt

class ProcessTimeline:
    """
    Code to process a user's timeline
    """

    def __init__(self, (filepath, filename)):
        self.filename = filepath + '/' + filename
        self.user_id = filename.split('_')[0]
        self.f = json.load(open(self.filename, 'r'))

        # User features
        self.follower = None
        self.following = None
        self.app_source = None
        self.location = None
        self.description = None
        self.protected = None
        self.verified = None
        self.followers_count = None
        self.friends_count = None
        self.listed_count = None
        self.statuses_count = None
        self.creation_time = None
        self.geo_enable = None
        self.utc_offset = None
        self.time_zone = None
        self.language = None
        self.default_profile = None
        self.default_profile_image = None
        # self.following = None
        self.follow_request_sent = None
        self.notifications = None
        self.geo = None

        # Tweet feature
        self.tweet_with_hashtags = 0
        self.tweets_with_mentions = 0
        self.tweets_with_urls = 0
        self.tweets_with_special_chars = 0
        self.num_tweet = 0
        self.retweet_count = 0
        self.tweet_text = []
        self.retweet_text = []
        self.duplicate_tweets = 0
        self.duplicate_urls = 0
        self.duplicate_domains = 0
        self.duplicate_mentions = 0
        self.duplicate_hashtags = 0
        self.num_tweets = 0
        self.num_hashtags = 0
        self.num_urls = 0
        self.num_mentions = 0
        self.duplicate_urls = 0
        self.duplicate_domains = 0
        self.duplicate_mentions = 0
        self.duplicate_hashtags = 0
        self.num_tweets_hashtags = 0
        self.num_tweets_urls = 0
        self.num_tweets_mentions = 0
        self.num_total_urls = 0
        self.num_total_domains = 0
        self.num_total_mentions = 0
        self.num_total_hashtags = 0
        self.tweets = []
        self.tweet_timings = []
        self.app_sources = []
        self.retweet_sources = []
        self.urls = []
        self.domains = []
        self.mentions = []
        self.hashtags = []

        self.mean_iat = None
        self.min_iat = None
        self.max_iat = None
        self.std_iat = None
        self.entropy_iat = None
        self.burstiness = None

        try:
            load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
            user = os.getenv('DATABASE_USER')
            password = os.getenv('DATABASE_PASSWORD')
            conn_string = "dbname='twitter' user=" + user + " password = " + password
            self.con = psycopg2.connect(conn_string)
            self.cur = self.con.cursor()
        except (psycopg2.DatabaseError) as e:
            print 'Error %s' % e
            sys.exit(1)
        self.cur.execute('SELECT user_info_json FROM followers WHERE user_id = (%s)', (self.user_id,))
        f = self.cur.fetchone()
        if f and f[0]:
            self.user_info = ast.literal_eval(f[0])
        else:
            print "No info in db for user " + self.user_id
            self.user_info = None

    def user_features(self):
        self.follower = (self.user_info['followers_count'])
        self.following = (self.user_info['friends_count'])
        self.location = self.user_info['location']
        self.description = self.user_info['description']
        self.protected = self.user_info['protected']
        self.verified = self.user_info['verified']
        self.followers_count = self.user_info['followers_count']
        self.friends_count = self.user_info['friends_count']
        self.listed_count = self.user_info['listed_count']
        self.statuses_count = self.user_info['statuses_count']
        self.creation_time = self.user_info['created_at']
        self.geo_enable = self.user_info['geo_enabled']
        self.utc_offset = self.user_info['utc_offset']
        self.time_zone = self.user_info['time_zone']
        self.language = self.user_info['lang']
        self.default_profile = self.user_info['default_profile']
        self.default_profile_image = self.user_info['default_profile_image']
        # self.following = self.user_info['user']['following']
        self.follow_request_sent = self.user_info['follow_request_sent']
        self.notifications = self.user_info['notifications']

    def collect_tweets(self):
        """
        # of hashtags per tweet
        # of tweets with hashtags
        # mentions per tweet
        # of tweets with mentions
        URLs per tweet
        Tweets with URLs
        # special characters per tweet
        # Tweets with special characters
        Retweets by user
        Inter-tweet content similarity: Bag of Words w/ Jaccard and cosine similarity
        Duplicate tweets
        Duplicate URLs ratio (1-unique URLs/total URLs)
        Duplicate Domains Ratio (1-unique domains/total domains)
        Duplicate Mentions Ratio (1-unique mentions/ total mentions)
        Duplicate hashtags ratio (1-unique hashtags/total hashtags)
        """
        for twt in self.f:
            tweet = Tweet()
            tweet.get_features(twt)
            self.tweets.append(tweet)
            self.tweet_timings.append(tweet.date)
            self.tweet_text.append(tweet.html_text)
            self.app_sources.append(tweet.source)
            self.retweet_sources.append(tweet.rts)
            for url in tweet.urls:
                self.urls.append(url['expanded_url'])
                self.domains.append(url['display_url'].split('/')[0])
            for mention in tweet.mentions:
                self.mentions.append(mention['id'])
            for hashtag in tweet.hashtags:
                self.hashtags.append(hashtag['text'])

    def content_features(self):
        """
        # of hashtags per tweet
        # of tweets with hashtags
        # mentions per tweet
        # of tweets with mentions
        URLs per tweet
        Tweets with URLs
        # special characters per tweet
        # Tweets with special characters
        Retweets by user
        Inter-tweet content similarity: Bag of Words w/ Jaccard and cosine similarity
        Duplicate tweets
        Duplicate URLs ratio (1-unique URLs/total URLs)
        Duplicate Domains Ratio (1-unique domains/total domains)
        Duplicate Mentions Ratio (1-unique mentions/ total mentions)
        Duplicate hashtags ratio (1-unique hashtags/total hashtags)
        """
        self.num_tweets = len(self.tweets)
        self.num_tweets_hashtags = sum([1 if t.has_hashtag else 0 for t in self.tweets])
        self.num_tweets_urls = sum([1 if t.has_url else 0 for t in self.tweets])
        self.num_tweets_mentions = sum([1 if t.has_mention else 0 for t in self.tweets])
        self.num_total_urls = len(self.urls)
        self.num_total_domains = len(self.domains)
        self.num_total_mentions = len(self.mentions)
        self.num_total_hashtags = len(self.hashtags)
        self.duplicate_urls = 1 - float(len(set(self.urls)))/float(self.num_total_urls) if self.num_total_urls > 0 else 0
        self.duplicate_mentions = 1 - float(len(set(self.mentions)))/float(self.num_total_mentions) if self.num_total_mentions > 0 else 0
        self.duplicate_domains = 1 - float(len(set(self.domains)))/float(self.num_total_domains) if self.num_total_domains > 0 else 0
        self.duplicate_hashtags = 1 - float(len(set(self.hashtags)))/float(self.num_total_hashtags) if self.num_total_hashtags > 0 else 0

    def temporal_features(self):
        """
        # tweets per second
        Entropy of inter-tweet time distribution
        Predictability of tweet timing based on transfer energy approach
        Duration of longest session by user without any short (5 or 10 min) break
        Average number of tweets per day
        Percentage of unfollows compared to percentage of follows
        X2 test to see if tweets are drawn uniformly across seconds-of-minute and minute-of-hour distributions
        Pearson's Chi-Square Statistics of Uniformity: bin time stamps of user's tweets into histograms of W bins for
            second-of-minute, minute-of-hour, hour-of-day, and day-of-week
        Signal-to-Noise ratio (Ration of mean to standard deviation, min, max, and entropy of these values to detect
            abrupt changes in users' metadata (followers, followees, posts, etc)
        Burstiness: (std. dev - mean/ std. dev + mean) of time interval sequence
        Time-interval entropy: For time interval sequence Tj with length of N for user j, see sheet
        :return:
        """
        tweet_timings = [(t-datetime(1970, 1, 1)).total_seconds() for t in self.tweet_timings]
        tweet_timings.sort()
        iat = np.diff(tweet_timings)
        a = iat[:-1]
        b = iat[1:]
        c = a-b
        plt.scatter(range(0,198), c)
        plt.show()
        '''
        plt.scatter(iat[:-1], iat[1:])
        plt.suptitle('Lagged IAT')
        plt.show()
        '''
        self.mean_iat = np.mean(iat)
        self.min_iat = np.min(iat)
        self.max_iat = np.max(iat)
        self.std_iat = np.std(iat)
        self.burstiness = (self.std_iat - self.mean_iat) / (self.std_iat + self.mean_iat)
        seconds = [s.second for s in self.tweet_timings]
        minutes = [s.minute for s in self.tweet_timings]
        hours = [s.hour for s in self.tweet_timings]
        '''
        plt.scatter(seconds, minutes)
        plt.suptitle('Seconds-of-Minute')
        plt.show()
        plt.scatter(minutes, hours)
        plt.suptitle('Minutes-of-Hours')
        plt.show()
        '''
        #add day of week
        total_time = sum(tweet_timings)
        tweet_timing_probs = [t/total_time for t in tweet_timings]
        self.entropy_iat = -sum([p*math.log(p, 2) for p in tweet_timing_probs])



def get_files(file_path):
    folders = os.walk(file_path)
    file_names = []
    for f in folders:
        files = [fs for fs in f[2] if 'stream' in fs]
        if files:
            for i in range(0, len(files)):
                file_names.append((f[0], files[i]))
    return file_names

if __name__ == "__main__":
    files = get_files('/home/amanda/bigDisk/Twitter/Debot2/')
    for file_name in files:
        pt = ProcessTimeline(file_name)
        if not pt.user_info:
            continue
        # need to add in bulk lookup for users to add user
        #pt.user_features()
        pt.collect_tweets()
        print len(pt.f)
        if len(pt.f) == 200:
           # pt.content_features()
            pt.temporal_features()
