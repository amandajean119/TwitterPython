import botornot

def load_from_file (f):
	filename = open(f, 'r')
	contents = filename.read()
	filename.close()
	items = [name for name in contents.split('\n') if name]                
	return items
#unmdscs1
twitter_app_auth = {
    'consumer_key': '',
    'consumer_secret': '',
    'access_token': '',
    'access_token_secret': '',
  }
bon = botornot.BotOrNot(**twitter_app_auth)

f_out = open('score.txt','w')
names = load_from_file('names.txt')

for i in range (len(names)):
	print i
	req = '@' + names[i]  	
	score = bon.check_account(req)
	print score
	f_out.write(str(score) + '\n')
