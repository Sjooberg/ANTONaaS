from flask import Flask, render_template
from tasks import make_celery
import os
import sys
import swiftclient
from collections import Counter
try:
	import json
except ImportError:
	import simplejson as json


	
# Containers name to retreive documents from (do not forget to source g.. first)

container_name = 'tweets'


# Setup connection to swift client containers
config = {'user':os.environ['OS_USERNAME'],
          'key':os.environ['OS_PASSWORD'],
          'tenant_name':os.environ['OS_TENANT_NAME'],
          'authurl':os.environ['OS_AUTH_URL']}
conn = swiftclient.Connection(auth_version=3, **config)          


# Create flask app
app = Flask(__name__)
app.config['CELERY_BROKER_URL']='amqp://guest@localhost//'
app.config['CELERY_BACKEND']='rpc://'


# Create cellery worker
celery = make_celery(app)


# Method done by the flask app
@app.route('/twitterCount',methods=["GET"])
def twitterCount():
	tweetRetrieveAndCount.delay()
	return "End flask route"


# Task that is beeing done by the celery workers
@celery.task(name= 'celery_ex.tweetRetrieveAndCount')
def tweetRetrieveAndCount():	
	pronomen = {'han': 0, 'hon': 0 , 'hen': 0, 'den': 0, 'det': 0, 'denna': 0, 'denne': 0}
	for data in conn.get_container(container_name)[1]:
		obj_tuple = conn.get_object(container_name, data['name'])	
		currentLine = 0
		for line in obj_tuple[1].splitlines():	
			currentLine += 1
			if currentLine % 2 == 0:
				continue
			try:
				tweet = json.loads(line)
				if 'retweeted_status' not in tweet:
					countsWord = Counter(tweet['text'].lower().split())
					for key in pronomen:
						if key in countsWord:
							pronomen[key] += countsWord[key]
			except ValueError:
				print(ValueError)
			except:
				continue
	print(pronomen)
	with open("result", 'w') as result:
		json.dump(pronomen, result)
	return "End celery tweetRetrieve"

if __name__ == '__main__':
	app.run(host = "0.0.0.0", debug=True)
