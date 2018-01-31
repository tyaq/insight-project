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

@app.route('/api/<id>/door')
def get_door(id):
       print(id)
       stmt = "SELECT * FROM doorStatus WHERE deviceID=\"%s\""
       response = session.execute(stmt,parameters=[id])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"DeviceID": x.deviceid.decode('string_escape'), "door-open": x.dooropen} for x in response_list]
       return jsonify(doorStatus=jsonresponse)

@app.route('/api/<id>/defrost')
def get_defrost(id):
       print(id)
       stmt = "SELECT * FROM defrostStatus WHERE deviceID=\"%s\""
       response = session.execute(stmt,parameters=[id])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"DeviceID": x.deviceid.decode('string_escape'), "defrosted": x.defrosted} for x in response_list]
       return jsonify(doorStatus=jsonresponse)
