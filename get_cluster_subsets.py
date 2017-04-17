import os
import pandas as pd
import psycopg2
import json
from dotenv import load_dotenv
import sys
import traceback

class GetClusterSubsets:
    def __init__(self):
        self.files = []
        self.clusters = set()
        self.file_path = '/home/amanda/bigDisk/Twitter/Weeks/'
        self.cluster_dates = {}

    def get_files(self):
        folders = os.walk(self.file_path)
        for f in folders:
            files = [fs for fs in f[2] if 'clstrs' in fs]
            if files:
                for i in range(0, len(files)):
                    self.files.append((f[0], files[i]))

    def find_followers(self):
        for file_path, file_name in self.files:
            with open(file_path + '/' + file_name, 'r') as f:
                for line in f:
                    users = set()
                    for user in line.strip().split(','):
                        users.add(user)
                    if 12 < len(users) < 20:
                        self.clusters.update(users)
        with open('mid_size_clusters.txt', 'w') as f:
            for cluster in self.clusters:
                f.write(cluster + '\n')

    def get_dates(self):
        non_susp = set()
        for file_path, file_name in self.files:
            with open(file_path + '/' + file_name, 'r') as f:
                for line in f:
                    users = set()
                    for user in line.strip().split(','):
                        users.add(user)
                    if 12 < len(users) < 20:
                        self.cluster_dates[frozenset(users)] = file_path.split('/')[6].split('_')[0].lstrip('w')
        with open('crawler/twitterCrawler/twitter/spiders/mid_size_non_suspended.txt', 'r') as f:
            for line in f:
                non_susp.add(line.strip())
        with open('mid_size_non_suspended_dates.txt', 'w') as output:
            for key, value in self.cluster_dates.iteritems():
                num_members = len(key)
                num_missing = len(key - non_susp)
                if num_missing < 4:
                    non_susp_members = key & non_susp
                    for item in non_susp_members:
                        output.write(item + ';')
                    output.write(',' + value + ',' + str(num_missing) + '\n')


def create_date_cluster_file():
    try:
        load_dotenv('../creds/.env')
        user = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASSWORD')
        conn_string = "dbname='twitter' user=" + user + " password = " + password
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
    except (psycopg2.DatabaseError) as e:
        print 'Error %s' % e
        sys.exit(1)
    actual_user = set()
    with open('subset_clusters.csv', 'r') as f:
        for line in f:
            line = line.strip(',').split(',')
            actual_user.update(set(line))
    user_stats = []
    with open('cluster_stats.csv', 'r') as stats:
        next(stats)
        for line in stats:
            line = line.strip().split(',')
            users = set([l for l in line[0].strip().rstrip(';').split(';') if l in actual_user])
            vals = line[1:]
            user_stats.append((users, vals))

    with open('mid_size_cluster_dates.txt', 'r') as f:
        with open('subset_user_date_ratio.csv', 'w') as output:
            output.write('week,cluster,users,num_edges,possible_edges,ratio\n')
            for line in f:
                line = line.strip().split(',')
                date = line[-1]
                users = set(line[:-1])
                vals = filter(lambda u_s: len(u_s[0] - users) < 2, user_stats)
                if vals:
                    users = vals[0][0]
                    vals = vals[0][1]
                    cluster_names = {}
                    for user in users:
                        cur.execute('SELECT cluster FROM name_id WHERE screen_name = %s;', (user,))
                        f = cur.fetchone()
                        if f:
                            clusters = [c for c in f[0].split(',') if c.find(date) != -1]
                            for cluster in clusters:
                                if cluster in cluster_names:
                                    cluster_names[cluster] += 1
                                else:
                                    cluster_names[cluster] = 1
                    cluster = max(cluster_names, key=cluster_names.get)
                    output.write(date + ',' + cluster + ',' + ';'.join(users) + ',' + ','.join(vals) + '\n')

# gcs = GetClusterSubsets()
# gcs.get_files()
# gcs.get_dates()
# gcs.find_followers()
create_date_cluster_file()