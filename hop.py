class Hop:
    def __init__(self, followers_stream):
        self.followers_stream = followers_stream
        self.scores = {}

    def pick_node(self):
        for key, value in self.followers_stream:
            self.scores[key] = self.evaluate_bot_score(value)
        # Does this return the key or the value? Need the key
        # Eventually we may want to return top k ones, or have multiple methods to evaluate different aspects of the
        # stream and get consensus, or have method to compare different followers to each other,
        # like similarity of tweets or usernames
        return max(self.scores, key=lambda user: self.scores[user])

    def evaluate_bot_score(self, stream):
        """

        :return: score of how bot-like a user's Twitter stream is
        """
        # magic formula that says bot or not
        return 0
