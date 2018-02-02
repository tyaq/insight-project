# jsonify creates a json representation of the response
from flask import jsonify
import urllib

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-35-153-150-183.compute-1.amazonaws.com'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('hypespace')

@app.route('/')
@app.route('/index')
def index():
  return "Hello, World!"

@app.route('/api/status')
def get_status():
       stmt = "SELECT * FROM status"
       print(stmt)
       response = session.execute(stmt,parameters=[id])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"deviceID": x.deviceid.replace('\"', ''), "door-open": x.dooropen, "defrosted": x.defrosted, "efficiency": x.efficiency} for x in response_list]
       return jsonify(status=jsonresponse)

@app.route('/api/status/<id>')
def get_door(id):
       id='"'+id+'"'
       stmt = "SELECT * FROM status WHERE deviceID=%s"
       print(stmt % id)
       response = session.execute(stmt,parameters=[id])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"deviceID": x.deviceid.replace('\"', ''), "door-open": x.dooropen, "defrosted": x.defrosted, "efficiency": x.efficiency} for x in response_list]
       return jsonify(status=jsonresponse)
