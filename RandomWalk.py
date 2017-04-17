import logging
import query
import explore
import json
import extract
import hop


class RandomWalk:

    def __init__(self):
        logging.basicConfig()
        self.tokens = ["3049000188-5uO7lnBazaRcGTDiWzQNP6cYmTX5LeM4TFeIzWd",
                       "i3ZqDFWkr7tkXsdI1PYoQALvE6rtSWaXVPjuHxdFRpTK0", "IrZza7bDaRKGQUc5ZZM2EiCsG",
                       "hYFK0tUtxGBHpwvrsvZ6NLYTTGqCo5rMjBfmAPEtQ3nEm2OmJR"]
        self.bot_id = None
        self.followers = None
        self.followers_stream = None

    def query_twitter(self):
        """
        Gets the followers of the current bot
        :param bot_id: bot_id to get followers of
        :return: up to 5000 of the most recent followers of the current bot
        """
        # Maybe expand so can take in screen name or bot id
        q = query.Query(self.tokens)
        self.followers = q.query_api(self.bot_id)

    def explore_followers(self):
        """
        Gets streaming information about the current list of followers
        :param followers:
        :return:
        """
        e = explore.Explore(self.followers, listen_time=600)
        e.get_stream()

    def process_stream(self):
        """
        Processed the followers' 10 minute tweet streams
        :return:
        """
        f = json.load(open('tmp_followers_stream.txt', 'r'))
        e = extract.Extract(f)
        self.followers_stream = e.process_stream()

    def hop_nodes(self):
        """
        Decide which node to explore based on processed stream
        :return:
        """
        h = hop.Hop(self.followers_stream)
        self.bot_id = h.pick_node()

