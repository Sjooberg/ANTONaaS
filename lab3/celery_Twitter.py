from __future__ import division
from flask import Flask, render_template
from tasks import make_celery
import os
import sys
import swiftclient
from collections import Counter
import pygal
import ast
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
	tweetCount.delay()
	return "End flask route"

#Create the Graph
@app.route('/graph', methods=["GET"])
def graph():#
	if os.path.isfile("result"):
		pronoun_graph = pygal.Bar(width=1200, height=600,
                          explicit_size=True)
		with open("result", 'r') as result_json:
			try:
				result_dict = ast.literal_eval(result_json.read())
				total_count  = sum(result_dict.values())
				# Add actual data for making graph
				for key in result_dict:
					pronoun_graph.add(key, [(result_dict[key]/total_count)*100])
			except Exception, e:
				return(str(e))
		# Giving the chart (pygal object) info
		pronoun_graph.y_title = "The relative number of pronouns[%]"
		pronoun_graph.title = "The number of pronouns"
		pronoun_graph_data = pronoun_graph.render_data_uri()
		return render_template("graphing.html", pronoun_graph_data = pronoun_graph_data)
	else:
        	return "No results found, run twitterCount\n"


# Task that is beeing done by the celery workers
@celery.task(name= 'celery_ex.tweetCount')
def tweetCount():	
	pronoun = {'han': 0, 'hon': 0 , 'hen': 0, 'den': 0, 'det': 0, 'denna': 0, 'denne': 0}
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
					for key in pronoun:
						if key in countsWord:
							pronoun[key] += countsWord[key]
			except ValueError:
				print(ValueError)
			except:
				continue
	print("The total number of each pronoun is:")
	print(pronoun)
	with open("result", 'w') as result:
		json.dump(pronoun, result)
	return "Ending tweetCount"

if __name__ == '__main__':
	app.run(host = "0.0.0.0", debug=True)
