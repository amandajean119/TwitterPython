import time
from datetime import datetime
import re


class Tweet:
    """Store the tweet info
    """
    def __init__(self):
        self.id = None
        self.username = None
        self.url = None
        self.user_avatar_url = None
        self.tweet_url = None
        self.profile_url = None
        self.html_text = None
        self.retweeted = None
        self.retweet_user = None
        self.date = None
        self.num_hashtags = 0
        self.has_hashtag = None
        self.hashtags = None
        self.num_mentions = 0
        self.has_mention = None
        self.mentions = None
        self.num_urls = 0
        self.has_url = None
        self.urls = None
        self.tweet_length = 0
        self.source = None
        self.rts = None

    def set_date(self, date_str):
        """Convert string to datetime
        """
        time_struct = time.strptime(date_str, "%a %b %d %H:%M:%S +0000 %Y")#Tue Apr 26 08:57:55 +0000 2011
        self.date = datetime.fromtimestamp(time.mktime(time_struct))

    def set_text(self, plain_text):
        """convert plain text into html text with http, user and hashtag links
        """

        re_http = re.compile(r"(http://[^ ]+)")
        self.html_text = re_http.sub(r'\1', plain_text)

        re_https = re.compile(r"(https://[^ ]+)")
        self.html_text = re_https.sub(r'\1', self.html_text)


        re_user = re.compile(r'@[0-9a-zA-Z+_]*',re.IGNORECASE)
        for iterator in re_user.finditer(self.html_text):
            a_username = iterator.group(0)
            username = a_username.replace('@','')
            # Make sure this is actually what we want, and it is necessary
            link = '' + username + ''
            self.html_text = self.html_text.replace(a_username, link)


        re_hash = re.compile(r'#[0-9a-zA-Z+_]*',re.IGNORECASE)
        for iterator in re_hash.finditer(self.html_text):
            h_tag = iterator.group(0)
            link_tag = h_tag.replace('#','%23')
            link = '' + h_tag + ''
            self.html_text = self.html_text.replace(h_tag + " ", link + " ")
            #check last tag
            offset = len(self.html_text) - len(h_tag)
            index = self.html_text.find(h_tag, offset)
            if index >= 0:
                self.html_text = self.html_text[:index] + " " + link

    def set_profile_url(self):
        """Create the url profile
        """
        if self.retweeted:
            self.profile_url = "http://www.twitter.com/%s" % self.retweet_user
        else:
            self.profile_url = "http://www.twitter.com/%s" % self.username

    def set_tweet_url(self):
        """Create the url of the tweet
        """
        self.tweet_url = "http://www.twitter.com/%s/status/%s" % (self.username, self.id)

    def get_features(self, js_tweet):
        """
        # of hashtags per tweet
        # of tweets with hashtags
        # mentions per tweet
        # of tweets with mentions
        URLs per tweet
        Tweets with URLs
        # special characters per tweet
        # Tweets with special characters
        Tweet length
        Tweets ending with punctuation, hashtag, or link
        Tweets geo-enabled?
        """
        self.id = js_tweet['id']
        self.username = js_tweet['user']['screen_name']
        try:
            self.retweet_user = js_tweet['retweeted_status']['user']['screen_name']
            self.retweeted = True
        except:
            self.retweeted = False
        self.set_date(js_tweet['created_at'])
        #tweet.id, tweet.username must exist
        self.set_tweet_url()
        #convert plain text to html text
        text = js_tweet['text'].encode('utf8')
        self.set_text(text)
        #tweet.id, tweet.username must exist
        self.set_profile_url()
        if self.retweeted:
            self.user_avatar_url = js_tweet['retweeted_status']['user']['profile_image_url']
        else:
            self.user_avatar_url = js_tweet['user']['profile_image_url']
        if js_tweet.get('source'):
            self.source = js_tweet['source']
        if js_tweet.get('retweeted_status'):
            self.rts = js_tweet['retweeted_status']['user']['id']
        self.num_hashtags = len(js_tweet['entities']['hashtags'])
        self.has_hashtag = True if js_tweet['entities']['hashtags'] else False
        self.hashtags = js_tweet['entities']['hashtags']
        self.num_mentions = len(js_tweet['entities']['user_mentions'])
        self.has_mention = True if js_tweet['entities']['user_mentions'] else False
        self.mentions = js_tweet['entities']['user_mentions']
        self.num_urls = len(js_tweet['entities']['urls'])
        self.has_url = True if js_tweet['entities']['urls'] else False
        self.urls = js_tweet['entities']['urls']
        self.tweet_length = len(self.html_text)
        self.date = datetime.strptime(str(js_tweet['created_at']), '%a %b %d %H:%M:%S +0000 %Y')
