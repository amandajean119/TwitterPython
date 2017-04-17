import psycopg2
import json
from dotenv import load_dotenv
import sys
import os
import traceback


def get_filenames(file_path):
    folders = os.walk(file_path)
    cluster_files = set()
    for f in folders:
        files = [fs for fs in f[2] if 'clstrs' in fs]
        if files:
            for i in range(0, len(files)):
                cluster_files.add((f[0], files[i]))
    return cluster_files


def update_db():
    try:
        load_dotenv('/home/amanda/bigDisk/Twitter/creds/.env')
        user = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASSWORD')
        conn_string = "dbname='twitter' user=" + user + " password = " + password
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
    except psycopg2.DatabaseError as e:
        print 'Error %s' % e
        traceback.print_exc()
        sys.exit(1)
    users = {}
    files = get_filenames('/home/amanda/bigDisk/Twitter/Weeks/')
    for file_path, file_name in files:
        with open(file_path + '/' + file_name, 'r') as f:
            cluster_num = 0
            for line in f:
                for user in line.strip().split(','):
                    cluster_name = '_'.join(file_path.split('/')[6:]) + '_' + str(cluster_num)
                    if user not in users:
                        users[user] = {cluster_name}
                    else:
                        users[user].add(cluster_name)
                cluster_num += 1
    for user, cluster in users.iteritems():
        cluster = ','.join(list(cluster))
        cur.execute('UPDATE name_id SET cluster = (%s) WHERE screen_name = (%s)', (cluster, user))
        con.commit()

update_db()