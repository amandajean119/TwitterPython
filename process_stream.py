import json
'''
for i in range(2, 7):
    filename = 'followers_stream/followers_stream' + str(i)
    with open(filename + '.txt', 'r') as f:
        with open(filename + '.json', 'w') as output:
            output.write('[')
            head = next(f)
            output.write(head)
            for line in f:
                output.write(',\n')
                output.write(line.strip())
            output.write(']')

print "created all intermediate files"
for i in range(2, 3):
    filename = 'followers_stream/followers_stream' + str(i)
    data = json.load(open(filename + '.json', 'r'))
    json.dump(data, open(filename + '2.json', 'w'), indent=4, separators=(',', ': '))
    print "did file"
'''

tweets_data_path = 'first_cliques_stream.txt'

tweets_data = []
keys = set()
with open(tweets_data_path, 'r') as f:
    for line in f:
        try:
            tweet = json.loads(line)
            '''
            print "text"
            print tweet["text"]
            print "deeper"
            print tweet["retweeted_status"]["extended_tweet"]["full_text"]
            '''
            for key, value in tweet.iteritems():
                keys.add(key)
                tweets_data.append(tweet)
        except:
            continue

    for key in keys:
        print key
