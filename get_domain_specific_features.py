
from __future__ import division
from datetime import datetime
import time
import numpy as np
from tweet import Tweet
import math
import matplotlib.pyplot as plt
from collections import Counter
from scipy.stats import chisquare


class GetFeatures:
    """
    Code to extract a Twitter user's features
    """

    def __init__(self, user_id, user_info, timeline):
        self.user_id = user_id
        self.f = timeline
        self.user_info = user_info
        self.user_features = {}
        self.temporal_features = {}
        self.content_features = {}
        self.network_features = {}
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

    def get_user_features(self):
        # self.features['follower'] = (self.user_info['followers_count'])
        # self.features['following'] = (self.user_info['friends_count'])
        self.user_features['location'] = self.user_info['location']
        # self.features['description'] = self.user_info['description']
        self.user_features['protected'] = self.user_info['protected']
        self.user_features['verified'] = self.user_info['verified']
        self.user_features['listed_count'] = self.user_info['listed_count']
        self.user_features['statuses_count'] = self.user_info['statuses_count']
        time_struct = time.strptime(self.user_info['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        account_date = datetime.fromtimestamp(time.mktime(time_struct))
        self.user_features['creation_time'] = (account_date - datetime(1970, 1, 1)).total_seconds()
        self.user_features['geo_enable'] = self.user_info['geo_enabled']
        # self.features['utc_offset'] = self.user_info['utc_offset']
        self.user_features['time_zone'] = self.user_info['time_zone']
        self.user_features['language'] = self.user_info['lang']
        self.user_features['default_profile'] = self.user_info['default_profile']
        self.user_features['default_profile_image'] = self.user_info['default_profile_image']
        # self.features['following = self.user_info['user']['following']
        # self.features['follow_request_sent'] = self.user_info['follow_request_sent']
        self.user_features['notifications'] = self.user_info['notifications']

        self.network_features['followers_count'] = self.user_info['followers_count']
        self.network_features['friends_count'] = self.user_info['friends_count']
        tmp_dict = dict()
        for key, value in self.user_features.iteritems():
            if not value:
                tmp_dict[key] = 'not_given'
            else:
                tmp_dict[key] = value
        self.user_features = tmp_dict

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

    def get_content_features(self):
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
        # Including mentions as a network feature since they involve another user
        self.content_features['num_tweets'] = len(self.tweets)
        self.content_features['num_tweets_hashtags'] = sum([1 if t.has_hashtag else 0 for t in self.tweets])
        self.content_features['num_tweets_urls'] = sum([1 if t.has_url else 0 for t in self.tweets])
        self.network_features['num_tweets_mentions'] = sum([1 if t.has_mention else 0 for t in self.tweets])
        self.content_features['num_tweets_retweets'] = sum([1 if t.retweeted else 0 for t in self.tweets])
        self.content_features['num_total_urls'] = len(self.urls)
        self.content_features['num_total_domains'] = len(self.domains)
        self.network_features['num_total_mentions'] = len(self.mentions)
        self.content_features['num_total_hashtags'] = len(self.hashtags)
        self.content_features['num_total_retweets'] = len(self.retweet_sources)
        self.url_distribution = [t.num_urls for t in self.tweets]
        self.domain_distribution = [t.num_domains for t in self.tweets]
        self.mention_distribution = [t.num_mentions for t in self.tweets]
        self.hashtag_distribution = [t.num_hashtags for t in self.tweets]

        self.content_features['duplicate_urls'] = 1 - float(len(set(self.urls)))/float(self.content_features['num_total_urls']) if self.content_features['num_total_urls'] > 0 else 0
        self.network_features['duplicate_mentions'] = 1 - float(len(set(self.mentions)))/float(self.network_features['num_total_mentions']) if self.network_features['num_total_mentions'] > 0 else 0
        self.content_features['duplicate_domains'] = 1 - float(len(set(self.domains)))/float(self.content_features['num_total_domains']) if self.content_features['num_total_domains'] > 0 else 0
        self.content_features['duplicate_hashtags'] = 1 - float(len(set(self.hashtags)))/float(self.content_features['num_total_hashtags']) if self.content_features['num_total_hashtags'] > 0 else 0

        set_tweets = [set(t.html_text.split(' ')) for t in self.tweets]
        text_jaccard = [jaccard(a, b) for a, b in zip(set_tweets[:-1], set_tweets[1:])]
        self.content_features['mean_text_jaccard'] = np.mean(text_jaccard)
        self.content_features['min_text_jaccard'] = min(text_jaccard)
        self.content_features['max_text_jaccard'] = max(text_jaccard)
        self.content_features['std_text_jaccard'] = np.std(text_jaccard)

        special_characters = [get_special_characters(x.html_text) for x in self.tweets]
        self.content_features['mean_special_character'] = np.mean(special_characters)
        self.content_features['min_special_character'] = min(special_characters)
        self.content_features['max_special_character'] = max(special_characters)
        self.content_features['std_special_character'] = np.std(special_characters)

        tweet_lens = [len(x.html_text) for x in self.tweets]
        self.content_features['mean_tweet_length'] = np.mean(tweet_lens)
        self.content_features['min_tweet_length'] = min(tweet_lens)
        self.content_features['max_tweet_length'] = max(tweet_lens)
        self.content_features['std_tweet_length'] = np.std(tweet_lens)

    def get_temporal_features(self):
        """
        # tweets per second
        Entropy of inter-tweet time distribution
        Duration of longest session by user without any short (5 or 10 min) break
        Average number of tweets per day
        Burstiness: (std. dev - mean/ std. dev + mean) of time interval sequence
        X2 test to see if tweets are drawn uniformly across seconds-of-minute, minute-of-hour, and hour-of-day distributions

        Signal-to-Noise ratio (Ration of mean to standard deviation, min, max, and entropy of these values to detect
            abrupt changes in users' metadata (followers, followees, posts, etc)
        Predictability of tweet timing based on transfer energy approach
        Percentage of unfollows compared to percentage of follows
        :return:
        """
        tweet_timings = [(t-datetime(1970, 1, 1)).total_seconds() for t in self.tweet_timings]
        tweet_timings.sort()
        iat = np.diff(tweet_timings)
        break_time = [0 if i <= 600 else 1 for i in iat]
        self.temporal_features['longest_session'] = ncount(break_time)
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
        self.temporal_features['mean_iat'] = np.mean(iat)
        self.temporal_features['min_iat'] = np.min(iat)
        self.temporal_features['max_iat'] = np.max(iat)
        self.temporal_features['std_iat'] = np.std(iat)
        self.temporal_features['burstiness'] = (self.temporal_features['std_iat'] - self.temporal_features['mean_iat']) \
                                               / (self.temporal_features['std_iat'] + self.temporal_features['mean_iat'])
        all_seconds = xrange(0, 60)
        seconds = [s.second for s in self.tweet_timings]
        missing_seconds = set(all_seconds) - set(seconds)
        seconds = dict(Counter(sorted(seconds)))
        seconds_freqs = []
        for i in range(60):
            if i in missing_seconds:
                seconds_freqs.append(0)
            else:
                seconds_freqs.append(seconds[i])
        self.temporal_features['x2_secs'] = chisquare(seconds_freqs)[1]

        all_mins = xrange(0, 60)
        mins = [s.minute for s in self.tweet_timings]
        missing_mins = set(all_mins) - set(mins)
        mins = dict(Counter(sorted(mins)))
        mins_freqs = []
        for i in range(60):
            if i in missing_mins:
                mins_freqs.append(0)
            else:
                mins_freqs.append(mins[i])
        self.temporal_features['x2_mins'] = chisquare(mins_freqs)[1]

        all_hours = xrange(0, 24)
        hours = [s.hour for s in self.tweet_timings]
        missing_hours = set(all_hours) - set(hours)
        hours = dict(Counter(sorted(hours)))
        hours_freqs = []
        for i in range(24):
            if i in missing_hours:
                hours_freqs.append(0)
            else:
                hours_freqs.append(hours[i])
        self.temporal_features['x2_hours'] = chisquare(hours_freqs)[1]

        days = Counter([(s.year, s.month, s.day) for s in self.tweet_timings]).values()
        self.temporal_features['avg_tweets_per_day'] = np.mean(days)
        self.temporal_features['max_tweets_per_day'] = max(days)
        self.temporal_features['min_tweets_per_day'] = min(days)
        self.temporal_features['std_tweets_per_day'] = np.std(days)

        '''
        plt.scatter(seconds, minutes)
        plt.suptitle('Seconds-of-Minute')
        plt.show()
        plt.scatter(minutes, hours)
        plt.suptitle('Minutes-of-Hours')
        plt.show()
        '''
        total_time = sum(tweet_timings)
        tweet_timing_probs = [t/total_time for t in tweet_timings]
        self.temporal_features['entropy_iat'] = -sum([p*math.log(p, 2) for p in tweet_timing_probs])


def get_special_characters(str):
    return sum([0 if x.isalnum() else 1 for x in str])


def ncount(L):
    count = 1
    max_count = 0
    for i in range(0, len(L)):
        if L[i] == 0:
            count += 1
        else:
            count = 1
        if max_count < count:
            max_count = count
    return max_count


def jaccard(a, b):
    try:
        return float(len(a & b))/float(len(a | b))
    except:
        return 0.0
