import json
import os

user_stream = {}

def process_stream(file_names):
    for file_path, file_name in file_names:
        with open(file_path + '/' + file_name, 'r') as f:
            for line in f:
                try:
                    js = json.loads(line)
                    if js.get('delete') or js.get('scrub_geo'):
                        continue
                    try:
                        id = js['id']
                        if id in user_stream:
                            user_stream[id].append(js)
                        else:
                            user_stream[id] = [js]
                    except Exception as e:
                        print "Parsing error"
                        print e
                        print js
                        continue
                except Exception as e:
                    print "Line error"
                    print e
                    print line
                    continue
    for key, value in user_stream.iteritems():
        json.dump(value, open('followers_stream/stream/' + str(key) + '_stream.json', 'w'))
        print "Wrote file " + str(key)

def get_files(file_path):
    folders = os.walk(file_path)
    file_names = []
    for f in folders:
        files = [fs for fs in f[2] if 'stream' in fs]
        if files:
            for i in range(0, len(files)):
                file_names.append((f[0], files[i]))
    return file_names

process_stream(get_files('/home/amanda/bigDisk/Twitter/followers_stream/stream/'))