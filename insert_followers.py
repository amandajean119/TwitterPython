import os
import psycopg2
import json
from dotenv import load_dotenv
import sys
import traceback


class InsertFollowers:

    def __init__(self):
        self.files = []

    def get_files(self, file_path):
        folders = os.walk(file_path)
        file_list = ['/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_7',
                     '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_4',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_8',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_3',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_2',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_5',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_9',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_1',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_3/Round_6',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_8/Round_4',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_8/Round_3',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_8/Round_2',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_8/Round_5',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_8/Round_1',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_7',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_4',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_8',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_3',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_2',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_5',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_1',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_7/Round_6',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_7',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_4',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_8',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_3',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_2',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_5',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_9',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_1',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_10/Round_6',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_7',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_4',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_8',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_3',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_2',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_5',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_9',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_1',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_1/Round_6',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_7',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_4',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_8',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_3',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_2',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_5',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_9',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_1',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_9/Round_6',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_4/Round_7',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_4/Round_8',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_4/Round_2',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_4/Round_9',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_4/Round_1',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_5/Round_7',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_5/Round_4',
                    '/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/Night_5/Round_8']
        for f in folders:
            files = [fs for fs in f[2] if 'bot' in fs]
            if files:
                if f[0] not in file_list:
                    for i in range(0, len(files)):
                        self.files.append((f[0], files[i]))

    def insert_into_db(self):
        try:
            load_dotenv('creds/.env')
            user = os.getenv('DATABASE_USER')
            password = os.getenv('DATABASE_PASSWORD')
            conn_string = "dbname='twitter' user=" + user + " password = " + password
            con = psycopg2.connect(conn_string)
            cur = con.cursor()
        except (psycopg2.DatabaseError) as e:
            print 'Error %s' % e
            sys.exit(1)
        for file_path, file_name in self.files:
            print file_path, file_name
            try:
                data = json.load(open(file_path + '/' + file_name, 'r'))
                for screen_name, friends_followers in data.iteritems():
                    # cur.execute('SELECT * FROM name_id where screen_name = (%s);', (screen_name,))
                    # count = cur.fetchone()[0]
                    cur.execute('UPDATE name_id SET followers = (%s) WHERE screen_name = (%s)', (str(friends_followers[0]), screen_name))
                    #cur.execute('UPDATE name_id SET friends = (%s) WHERE screen_name = (%s)', (str(friends_followers[1]), screen_name))
                    con.commit()
            except:
                traceback.print_exc()
                print file_path + '/' + file_name
                continue


i_f = InsertFollowers()
i_f.get_files('/home/amanda/bigDisk/Twitter/Weeks/w36_29_Aug/')
i_f.insert_into_db()
#i_f.get_files('/home/amanda/bigDisk/Twitter/Weeks/w35_19_Aug/')
#i_f.insert_into_db()
