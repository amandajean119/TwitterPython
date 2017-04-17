import os
import psycopg2
from dotenv import load_dotenv
import sys
import ast
from get_features import GetFeatures
import pandas as pd
from get_user_info import *


def get_bot_features(users_file, output):
    folders = os.walk('/home/amanda/bigDisk/Twitter/Debot2/stream/')
    try:
        load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
        username = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASSWORD')
        conn_string = "dbname='twitter' user=" + username + " password = " + password
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
    except psycopg2.DatabaseError as e:
        print 'Error: ' + e
        traceback.print_exc()
        sys.exit(1)
    with open(users_file, 'r') as f:
        user_ids = [line.strip() for line in f]
    file_names = []
    features = []
    # only one folder so can put i check inside inner loop
    for f in folders:
        files = [fs for fs in f[2] if 'stream.json' in fs]
        if files:
            for i in range(0, len(files)):
                if files[i].split('_')[0] in user_ids:
                    file_names.append((f[0], files[i]))
                    print i

                if i >= 50000:
                    break


    print "got filenames"
    i = 0
    for file_path, file_name in file_names:

        if i >=9000:
            break

        try:
            f = json.load(open(file_path +'/' + file_name, 'r'))
        except:
            print file_path
            print file_name
            continue
        if len(f) > 150:
            i += 1
            user_id = file_name.split('_')[0]
            cur.execute('SELECT user_info_json FROM followers WHERE user_id = %s', (user_id,))
            record = cur.fetchone()
            if record:
                if record[0]:
                    user_info = ast.literal_eval(record[0])
                else:
                    continue
                gf = GetFeatures(user_id, user_info, f)
                gf.user_features()
                gf.collect_tweets()
                gf.content_features()
                gf.temporal_features()
                features.append(gf.features)
    pd.DataFrame(features).to_csv(output)
    print "dumped file"

get_bot_features('stream/random_bots.txt', 'bot_features.csv')
get_bot_features('random_streams/random_users.txt', 'user_features.csv')
#get_bot_features('clique_expansion/common_core.csv', 'clique_expansion/common_core_features.csv')