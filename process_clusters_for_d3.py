import numpy as np
import json
from random import randint

edges = {}
with open('subset_edges.csv', 'r') as f:
    header = next(f)
    for line in f:
        line = line.strip().split(',')
        user1 = line[0]
        user2 = line[1]
        if user1 in edges:
            edges[user1].add(user2)
        else:
            edges[user1] = set([user2])

user_dict = {}
with open("subset_users.csv", 'r') as f:
    # need to avoid header
    header = next(f)
    for line in f:
        line = line.strip().split(',')
        if line[0] in user_dict:
            user_dict[line[0]] = str(100)
        else:
            user_dict[line[0]] = line[1]
user_indices = dict(enumerate(user_dict.keys()))
user_indices = {u: i for i, u in user_indices.iteritems()}
e = set(edges.keys())
for vals in edges.values():
    for val in vals:
        e.add(val)

json_dict = {}
links_list = []
nodes_list = []
for user, group_number in user_dict.iteritems():
    tmp_dict = {"group": group_number, "name": user, "id": user_indices[user]}
    nodes_list.append(tmp_dict)
for user1, followers in edges.iteritems():
    for user2 in followers:
        tmp_dict = {"source": user_indices[user1], "target": user_indices[user2]}
        links_list.append(tmp_dict)
json_dict["links"] = links_list
json_dict["nodes"] = nodes_list
json.dump(json_dict, open('twitter_bots.json', 'w'), indent=4)
