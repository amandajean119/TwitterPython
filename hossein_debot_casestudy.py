from __future__ import division
import json
import sys
from datetime import *
import numpy as np

def save_to_file (l , f):
	thefile = open (f , 'w')
	for item in l:
		thefile.write("%s\n" % item)
		
def load_from_file (f):
	filename = open(f, 'r')
	contents = filename.read()
	filename.close()
	items = [name for name in contents.split('\n') if name]		
	return items

def get_stat(filenames):
	fin = open('good_user.txt','r')
	wanted = []
	for line in fin:
		wanted.append(line.rstrip())
	print (wanted[0])
	c=0
	d=0
	out1=0
	out2=0
	out3=0
	out4=0
	ids=[]
	apps=[]
	RT_sources=[]
	m_follower=[]
	m_following=[]
	m_twt_count=[]
	texts=[]
	user_dict={}
	start_time = None
	end_time = None	
	created_at_temp = None
	for filename in filenames:
		flag = True
		print filename
		f = open(filename,'r')
		for line in f:
			if(len(line) <= 1000):#deletion
				if line.find('{\"delete\"')==0:
					x = json.loads(line)
					user_id = str(x['delete']['status']['user_id'])
					if user_id in wanted:
						if not user_id in user_dict:
							tt = [0 for i in range(6)]
							tt[2] = 1
							user_dict[user_id] = tt
						else:
							tt = user_dict[user_id]
							tt[2] = tt[2]+1
							user_dict[user_id] = tt
						d = d+1
			else:#normal	 
				c = c + 1
				x = json.loads(line)
				user_id =  str(x['user']['id'])
				created_at_temp = datetime.strptime(str(x['created_at']), '%a %b %d %H:%M:%S +0000 %Y')
				if flag:
					start_time = created_at_temp
					flag = False
				if user_id in wanted:
					twt_id = (x['id_str'])
					twt = x['text']#.encode('utf8')
					
					created_at = datetime.strftime(created_at_temp,"%m/%d/%y")
					url = (x['user']['screen_name']).encode('utf8')
					text = x['text'].encode('utf8')
					loc = text.find('http')
					if loc>0:
						text = text[0:loc]
					if not text in texts:
						texts.append(text)
					
					follower = (x['user']['followers_count'])
					following = (x['user']['friends_count'])			
					app_source = x['source']
					location = x['user']['location']
					description = x['user']['description']
					protected = x['user']['protected']
					verified = x['user']['verified']
					followers_count = x['user']['followers_count']
					friends_count = x['user']['friends_count']
					listed_count = x['user']['listed_count']
					statuses_count = x['user']['statuses_count']
					creation_time = x['user']['created_at']
					geo_enable = x['user']['geo_enabled']
					utc_offset = x['user']['utc_offset']
					time_zone = x['user']['time_zone']
					language = x['user']['lang']
					default_profile = x['user']['default_profile']
					default_profile_image =x['user']['default_profile_image'] 
					#following = x['user']['following']
					follow_request_sent = x['user']['follow_request_sent']
					notifications = x['user']['notifications']
					geo = x['geo']
					
					
					if not app_source in apps:
						apps.append(app_source)
					if twt.find('RT @')==0:
						out1=out1+1
						if x.get('retweeted_status'):
							RT_S = x['retweeted_status']['user']['id']
							if not RT_S in RT_sources:
								RT_sources.append(RT_S)
					if twt.find('http')>=0:
						out2=out2+1
					if verified=='true':
						out3=out3+1
					
					if not user_id in ids:
						ids.append(user_id)
						m_follower.append(int(str(follower)))
						m_following.append(int(str(following)))
						m_twt_count.append(int(str(statuses_count)))
					
					
					if not user_id in user_dict:
						tt = [0 for i in range(6)]
						tt[0] = 1
						if twt.find('http')>=0:
							tt[1] = 1
						tt[3] = statuses_count
						tt[4] = follower
						tt[5] = following	
						user_dict[user_id] = tt
					else:
						tt = user_dict[user_id]
						tt[0] = tt[0]+1
						if twt.find('http')>=0:
							tt[1] = tt[1] + 1
						tt[3] = statuses_count
						tt[4] = follower
						tt[5] = following	
						user_dict[user_id] = tt
					
		end_time = created_at_temp
		delta = end_time - start_time
		print 'duration: ' + str(((end_time - start_time).total_seconds() / 3600)) + ' hours'
	#save_to_file(m_follower , 'follower_' + filename)
	#save_to_file(m_following , 'following_' + filename)
	#save_to_file(m_twt_count , 'twt_count_' + filename)			
	#m_follower = np.array(m_follower)
	#m_following = np.array(m_following)
	#m_twt_count = np.array(m_twt_count)
	print '#users: ' + str(len(ids))
	print '#apps: ' + str(len(apps))
	print 'deletions: ' + str(d)
	print 'deletions/user: ' + str(d/len(ids))
	print 'tweets: ' + str(c)
	print 'tweets/user: ' + str(c/len(ids))
	print 'retweets: ' + (str(100*out1/c))[0:2] + '%'
	if len(RT_sources)>0:
		print '#RT per source: ' + (str(out1/len(RT_sources)))
	print 'HTTP: ' + (str(100*out2/c))[0:2] + '%'
	print 'UNIQUE TW: ' + (str(len(texts))) + '  ,  ' + str(1-(len(texts)/c))
	#print 'follower: ' + str(int(np.mean(m_follower))) + ' , ' + str(int(np.std(m_follower)))
	#print 'following: ' + str(int(np.mean(m_following))) + ' , ' + str(int(np.std(m_following)))
	#print 'tweet count: ' + str(int(np.mean(m_twt_count))) + ' , ' + str(int(np.std(m_twt_count)))
	
	
	
	fout = open('classification_data_good.csv' , 'w')
	for k,v in user_dict.items():
		if k in wanted:
			fout.write(str(k)+',')
			for xx in v:
				fout.write(str(xx)+',')
			fout.write('\n')	
	

if __name__ == "__main__":
	prefix=[]
	for i in range(len(sys.argv)-1):
		prefix.append(sys.argv[i+1])
	get_stat(prefix)		
		
	
