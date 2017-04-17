from __future__ import division
from datetime import datetime
import time
import numpy as np
from tweet import Tweet
import math
import matplotlib.pyplot as plt


class GetFeatures:
    """
    Code to extract a Twitter user's features
    """

    def __init__(self, user_id, user_info, timeline):
        self.user_id = user_id
        self.f = timeline
        self.user_info = user_info
        self.features = {}
        self.tweets = []
        self.tweet_timings = []
        self.app_sources = []
        self.retweet_sources = []
        self.urls = []
        self.domains = []
        self.mentions = []
        self.hashtags = []
        self.tweet_text = []
        self.url_distribution = []
        self.domain_distribution = []
        self.mention_distribution = []
        self.hashtag_distribution = []

    def set_date(self, date_str):
        """Convert string to datetime
        """
        time_struct = time.strptime(date_str, "%a %b %d %H:%M:%S +0000 %Y")#Tue Apr 26 08:57:55 +0000 2011
        self.date = datetime.fromtimestamp(time.mktime(time_struct))

    def user_features(self):
        # self.features['follower'] = (self.user_info['followers_count'])
        # self.features['following'] = (self.user_info['friends_count'])
        # self.features['location'] = self.user_info['location']
        # self.features['description'] = self.user_info['description']
        self.features['protected'] = self.user_info['protected']
        self.features['verified'] = self.user_info['verified']
        self.features['followers_count'] = self.user_info['followers_count']
        self.features['friends_count'] = self.user_info['friends_count']
        self.features['listed_count'] = self.user_info['listed_count']
        self.features['statuses_count'] = self.user_info['statuses_count']
        time_struct = time.strptime(self.user_info['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        account_date = datetime.fromtimestamp(time.mktime(time_struct))
        self.features['creation_time'] = (account_date - datetime(1970, 1, 1)).total_seconds()
        self.features['geo_enable'] = self.user_info['geo_enabled']
        # self.features['utc_offset'] = self.user_info['utc_offset']
        # self.features['time_zone'] = self.user_info['time_zone']
        #self.features['language'] = self.user_info['lang']
        self.features['default_profile'] = self.user_info['default_profile']
        self.features['default_profile_image'] = self.user_info['default_profile_image']
        # self.features['following = self.user_info['user']['following']
        # self.features['follow_request_sent'] = self.user_info['follow_request_sent']
        self.features['notifications'] = self.user_info['notifications']

    def collect_tweets(self):
        """

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
        Tweet length
        Inter-tweet content similarity: Bag of Words w/ Jaccard and cosine similarity
        Duplicate tweets
        Duplicate URLs ratio (1-unique URLs/total URLs)
        Duplicate Domains Ratio (1-unique domains/total domains)
        Duplicate Mentions Ratio (1-unique mentions/ total mentions)
        Duplicate hashtags ratio (1-unique hashtags/total hashtags)
        """
        #need to add in mean, min, max, std, etc for number of <> per tweet

        self.features['num_tweets'] = len(self.tweets)
        self.features['num_tweets_hashtags'] = sum([1 if t.has_hashtag else 0 for t in self.tweets])
        self.features['num_tweets_urls'] = sum([1 if t.has_url else 0 for t in self.tweets])
        self.features['num_tweets_mentions'] = sum([1 if t.has_mention else 0 for t in self.tweets])
        self.features['num_tweets_retweets'] = sum([1 if t.retweeted else 0 for t in self.tweets])
        self.features['num_total_urls'] = len(self.urls)
        self.features['num_total_domains'] = len(self.domains)
        self.features['num_total_mentions'] = len(self.mentions)
        self.features['num_total_hashtags'] = len(self.hashtags)
        self.features['num_total_retweets'] = len(self.retweet_sources)
        self.url_distribution = [t.num_urls for t in self.tweets]
        self.domain_distribution = [t.num_domains for t in self.tweets]
        self.mention_distribution = [t.num_mentions for t in self.tweets]
        self.hashtag_distribution = [t.num_hashtags for t in self.tweets]

        self.features['duplicate_urls'] = 1 - float(len(set(self.urls)))/float(self.features['num_total_urls']) if self.features['num_total_urls'] > 0 else 0
        self.features['duplicate_mentions'] = 1 - float(len(set(self.mentions)))/float(self.features['num_total_mentions']) if self.features['num_total_mentions'] > 0 else 0
        self.features['duplicate_domains'] = 1 - float(len(set(self.domains)))/float(self.features['num_total_domains']) if self.features['num_total_domains'] > 0 else 0
        self.features['duplicate_hashtags'] = 1 - float(len(set(self.hashtags)))/float(self.features['num_total_hashtags']) if self.features['num_total_hashtags'] > 0 else 0

        set_tweets = [set(t.html_text.split(' ')) for t in self.tweets]
        text_jaccard = [jaccard(a, b) for a, b in zip(set_tweets[:-1], set_tweets[1:])]
        self.features['mean_text_jaccard'] = np.mean(text_jaccard)
        self.features['min_text_jaccard'] = min(text_jaccard)
        self.features['max_text_jaccard'] = max(text_jaccard)
        self.features['std_text_jaccard'] = np.std(text_jaccard)

        special_characters = [get_special_characters(x.html_text) for x in self.tweets]
        self.features['mean_special_character'] = np.mean(special_characters)
        self.features['min_special_character'] = min(special_characters)
        self.features['max_special_character'] = max(special_characters)
        self.features['std_special_character'] = np.std(special_characters)

        tweet_lens = [len(x.html_text) for x in self.tweets]
        self.features['mean_tweet_length'] = np.mean(tweet_lens)
        self.features['min_tweet_length'] = min(tweet_lens)
        self.features['max_tweet_length'] = max(tweet_lens)
        self.features['std_tweet_length'] = np.std(tweet_lens)

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
        '''
        plt.scatter(range(0,198), c)
        plt.show()
        plt.scatter(iat[:-1], iat[1:])
        plt.suptitle('Lagged IAT')
        plt.show()
        '''
        self.features['mean_iat'] = np.mean(iat)
        self.features['min_iat'] = np.min(iat)
        self.features['max_iat'] = np.max(iat)
        self.features['std_iat'] = np.std(iat)
        self.features['burstiness'] = (self.features['std_iat'] - self.features['mean_iat']) / (self.features['std_iat'] + self.features['mean_iat'])
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
        self.features['entropy_iat'] = -sum([p*math.log(p, 2) for p in tweet_timing_probs])


def get_special_characters(str):
    return sum([0 if x.isalnum() else 1 for x in str])


def jaccard(a, b):
    try:
        return float(len(a & b))/float(len(a | b))
    except:
        return 0.0
