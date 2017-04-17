import json
import ast
import pandas as pd
import math

def process_json():
    j = json.load(open('bots_friendships.json', 'r'))

    with open('bot_friendships.csv', 'w') as f:
        for key, value in j.iteritems():
            try:
                f1 = value['relationship']['source']['following']
                f2 = value['relationship']['source']['followed_by']
                f.write(key + ';' + str(f1) + ';' + str(f2) + '\n')
            except TypeError:
                f.write(key + ';None;None\n')
                print key
                print value

def characterize_cluster():
    clusters = {}
    users = {}
    weeks = {}
    with open('subset_clusters_full.txt', 'r') as cluster_file:
        header = next(cluster_file)
        for line in cluster_file:
            line = line.split(',')
            l = line[0].strip(';').split(';')
            clusters[frozenset(l)] = {}
            weeks[frozenset(l)] = line[1]

    with open('bot_friendships.csv', 'r') as follows:
        for line in follows:
            line = line.split(';')
            user1 = ast.literal_eval(line[0])[0]
            user2 = ast.literal_eval(line[0])[1]
            if ast.literal_eval(line[1]):
                if user1 in users:
                    users[user1].append(user2)
                else:
                    users[user1] = [user2]
            if ast.literal_eval(line[2].strip()):
                if user2 in users:
                    users[user2].append(user1)
                else:
                    users[user2] = [user1]
    for cluster in clusters:
        for user in cluster:
            if user in users:
                clusters[cluster][user] = users[user]
    cluster_week = {}
    with open('subset_users.csv', 'w') as output:
        with open('subset_edges.csv', 'w') as edges:
            output.write('ID,Cluster\n')
            edges.write('Source,Target\n')
            for key, value in clusters.iteritems():
                cluster_week[weeks[key]] = {str(list(key)): value}
                for user in key:
                    output.write(user + ',' + weeks[key] + '\n')
                for source, targets in value.iteritems():
                    for target in targets:
                        edges.write(source + ',' + target + '\n')
    with open('bot_clusters_follow_info.json', 'w') as f:
        json.dump(cluster_week, f, indent=4, sort_keys=True, separators=(',', ': '))

def get_users():
    users = set()
    with open('subset_clusters.txt', 'r') as f:
        with open('subset_users.csv', 'w') as output:
            for line in f:
                line = line.strip(';').split(';')
                for user in line:
                    users.add(user)
            for user in users:
                output.write(user + '\n')
def nCr(n,r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)

def cluster_stats():
    clusters = {}
    weeks = {}
    users = {}
    actual_user = set()
    with open('subset_clusters.csv', 'r') as f:
        for line in f:
            line = line.strip(',').split(',')
            actual_user.update(set(line))
    with open('subset_clusters_full.txt', 'r') as cluster_file:
        header = next(cluster_file)
        for line in cluster_file:
            line = line.split(',')
            l = [l for l in line[0].strip(';').split(';') if l in actual_user]
            clusters[frozenset(l)] = 0
            weeks[frozenset(l)] = line[1]
    with open('bot_friendships.csv', 'r') as follows:
        for line in follows:
            line = line.split(';')
            user1 = ast.literal_eval(line[0])[0]
            user2 = ast.literal_eval(line[0])[1]
            if ast.literal_eval(line[1]):
                if user1 in users:
                    users[user1].append(user2)
                else:
                    users[user1] = [user2]
            if ast.literal_eval(line[2].strip()):
                if user2 in users:
                    users[user2].append(user1)
                else:
                    users[user2] = [user1]
    with open('cluster_stats.csv', 'w') as output:
        output.write('users,num_edges,possible_edges,ratio\n')
        for cluster in clusters:
            for user in cluster:
                output.write(user + ';')
                if user in users:
                    for follower in users[user]:
                        if follower in cluster:
                            clusters[cluster] += 1
            num_users = len(cluster)*(len(cluster) - 1)
            ratio = float(clusters[cluster]) / float(num_users)
            output.write(',' + str(clusters[cluster]) + ',' + str(num_users) + ',' + str(ratio) + '\n')

process_json()
characterize_cluster()
#get_users()
cluster_stats()