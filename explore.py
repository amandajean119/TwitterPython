# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import sys
from datetime import *
import traceback

# ___________________________________________________________________

class StdOutListener(StreamListener):
    def __init__(self, listen_time, api=None):
        super(StdOutListener, self).__init__()
        self.prev_time = datetime.now()
        self.next_time = datetime.now()
        output_file_name = 'tmp_followers_stream.txt'
        #should this be write or append??
        self.output_file = open(output_file_name, 'w')
        self.listen_time = listen_time  # in seconds

    def on_data(self, data):
        self.next_time = datetime.now()
        if (self.next_time - self.prev_time).total_seconds() < self.listen_time:
            try:
                self.output_file.write(str(data))
                return True
            except:
                print "Exception writing data"
                return True
        else:
            return False

    def on_error(self, status):
        if status == 420:
            print 'ERROR: ' + str(status)
            time.sleep(600)
        else:
            print 'ERROR: ' + str(status)



class Explore:

    def __init__(self, followers, listen_time=600):
        self.access_token = "781174251621494785-vekK5v518ddfH7I0zBOESdWXRgQz63n"
        self.access_token_secret = "fhVmLgCvVEwzjk28KFLsqPwivs7OlepaEpggtee1WDxqD"
        self.consumer_key = "tlUFi9tJGX1NxIA7JWBET2f4K"
        self.consumer_secret = "4uFHkxjmLyn2mAdLYOCtD1VekrHtg34qYk16kxn0bnSOGnIpxT"
        '''
        access_token = "3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd"
        access_token_secret = "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0"
        consumer_key = "IrZza7bDaRKGQUc5ZZM2EiCsG"
        consumer_secret = "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"
        '''
        self.users = followers
        self.l = StdOutListener(listen_time)
        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)

    def get_stream(self):
        while len(self.users) > 5000:
            try:
                print "Listening to 5000 users"
                self.stream = Stream(self.auth, self.l)
                self.stream.filter(follow=self.users[0:4999])
                self.users = self.users[5000:]
            except:
                print "Exited first loop with exception"
                traceback.print_exc(file=sys.stdout)
                pass
        else:
            try:
                "Listening to last set of users"
                self.stream = Stream(self.auth, self.l)
                self.stream.filter(follow=self.users)
            except:
                print "Exited last round with exception"
                traceback.print_exc(file=sys.stdout)
                sys.exit()
