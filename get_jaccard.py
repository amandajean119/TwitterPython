import psycopg2
import json
from dotenv import load_dotenv
import sys
import os
import traceback
import ast
from itertools import combinations


def collect_clusters():
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
    cur.execute("SELECT followers, cluster, screen_name, user_id FROM name_id WHERE followers IS NOT NULL AND cluster IS NOT NULL and cluster like '%w32_15_Jul_Night_7_Round_7%';")
    records = cur.fetchall()
    cluster_info = {}
    for record in records:
        followers = ast.literal_eval(record[0])
        clusters = record[1].split(',')
        for cluster in clusters:
            # if cluster.split('_')[0] == 'w35' or cluster.split('_')[0] == 'w36':
            if cluster in cluster_info:
                cluster_info[cluster].append(followers)
            else:
                cluster_info[cluster] = [followers]
    '''
    with open('cluster_followers.csv', 'w') as f:
        for cluster_name, followers in cluster_info.iteritems():
            f.write(cluster_name + ',')
            for follower_set in followers:
                for follower in follower_set:
                    f.write(str(follower) + ';')
                f.write(',')
            f.write('\n')
            '''
    return cluster_info


def collect_followers_for_subset():
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
    with open('../cluster_friendships/subset_users.csv', 'r') as f:
        for line in f:
            line = line.split(',')
            if line[0] in users:
                users[line[0]].append(line[1].strip())
            else:
                users[line[0]] = [line[1].strip()]
    data = json.load(open('bots_followers_friends_clusters.json', 'r'))
    cluster_info = {}
    for screen_name, followers in data.iteritems():
        if followers == [[], []] or followers == []:
            print "User " + screen_name + " has no followers"
            continue
        cur.execute('SELECT cluster FROM name_id WHERE screen_name = %s;', (screen_name,))
        try:
            clusters = cur.fetchone()[0].split(',')
            cluster_numbers = users[screen_name]
            for cluster in clusters:
                if cluster.split('_')[0][1:] in cluster_numbers:
                    if cluster in cluster_info:
                        cluster_info[cluster].append(followers)
                    else:
                        cluster_info[cluster] = [followers]
        except:
            print "Error with clusters for: " + screen_name
            continue
    '''
    with open('cluster_followers_subset.csv', 'w') as f:
        for cluster_name, followers in cluster_info.iteritems():
            f.write(cluster_name + ',')
            for follower_set in followers:
                for follower in follower_set:
                    f.write(str(follower) + ';')
                f.write(',')
            f.write('\n')
            '''
    return cluster_info


def jaccard((a, b)):
    a = set(a)
    b = set(b)
    try:
        return float(len(a & b))/float(len(a | b))
    except:
        print "error"


def calculate_jaccard_distances():
    cluster_info = collect_followers_for_subset()
    # cluster_info = get_clusters_from_file()
    with open('jaccard_per_cluster_subset.csv', 'w') as f:
        for cluster_name, followers in cluster_info.iteritems():
            jaccards = []
            if followers and len(followers) > 5:
                f.write(cluster_name + ',')
                combos = list(combinations(followers, 2))
                for combo in combos:
                    j = jaccard(combo)
                    jaccards.append(j)
                    f.write(str(j) + ',')
                avg_jaccard = sum(jaccards)/float(len(jaccards))
                f.write(str(avg_jaccard) + '\n')


def get_clusters_from_file():
    cluster_info = {}
    with open('cluster_followers_subset.csv', 'r') as f:
        for line in f:
            line = line.split(',')
            if line[1]:
                tmp = []
                for followers in line[1].split(','):
                    tmp.append(list(ast.literal_eval(followers.replace(';', ','))))
                cluster_info[line[0]] = tmp
    return cluster_info


def fix_set():
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
    cur.execute("SELECT cluster, user_id FROM name_id WHERE cluster IS NOT NULL AND cluster LIKE '%set%';")
    records = cur.fetchall()
    for record in records:
        user_id = record[1]
        clusters = record[0].lstrip('set(').rstrip(')')
        clusters = str(ast.literal_eval(clusters))
        cur.execute('UPDATE name_id SET cluster = (%s) WHERE user_id = (%s);', (clusters, user_id))
        con.commit()

#fix_set()
#collect_followers_for_subset()
calculate_jaccard_distances()
