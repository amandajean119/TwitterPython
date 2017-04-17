# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import sys
from datetime import *
import traceback


# ________________________________read file and write each line in an array

def load_from_file(f):
    filename = open(f, 'r')
    contents = filename.read()
    filename.close()
    items = [name for name in contents.split('\n') if name]
    return items


# ___________________________________________________________________

class StdOutListener(StreamListener):
    def __init__(self, api=None):
        super(StdOutListener, self).__init__()
        self.prev_time = datetime.now()
        self.next_time = datetime.now()
        output_file_name = 'followers_stream5.txt'
        self.output_file = open(output_file_name, 'a')
        self.listen_time = 43200  # in seconds

    def on_data(self, data):
        self.next_time = datetime.now()
        if (self.next_time - self.prev_time).total_seconds() < self.listen_time:
            try:
                # x = json.loads(data)
                # output_file.write(str(x['created_at']) + "," + str(x['user']['screen_name']) + "," + str(x['user']['id'])+ "\n")
                self.output_file.write(str(data))
                return True
            except:
                return True
        else:
            return False

    def on_error(self, status):
        print status


class CollectStreams:

    def __init__(self):
        # Variables that contains the user credentials to access Twitter API

        # ==================Add Twitter Authentication key==================

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
        # inputs from terminal
        self.input_file_name = sys.argv[1]
        self.users = load_from_file(self.input_file_name)

        loc = sys.argv[1].rfind('/')
        loc2 = sys.argv[1][:loc].rfind('/')
        log_file_name = sys.argv[1][:loc2 + 1] + "log.txt"
        self.log_file = open(log_file_name, 'a')
        self.l = StdOutListener()
        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)

    def run_stream(self):
        self.log_file.write("Start Listening: " + str(datetime.now()) + '\n')

        while len(self.users) > 5000:
            try:
                self.stream = Stream(self.auth, self.l)
                self.stream.filter(follow=self.users[0:4999])
                self.users = self.users[5000:]
            except:
                pass
        else:
            try:
                self.stream = Stream(self.auth, self.l)
                self.stream.filter(follow=self.users)
            except:
                traceback.print_exc(file=sys.stdout)
                sys.exit()
        print "Listening Finished!"
        self.log_file.write("Listening Finished!: " + str(datetime.now()) + '\n')

cs = CollectStreams()
cs.run_stream()