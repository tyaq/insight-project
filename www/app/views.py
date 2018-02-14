# jsonify creates a json representation of the response
from flask import jsonify
import urllib

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

cluster = Cluster(['ec2-35-171-196-68.compute-1.amazonaws.com'])

# Setting up connections to cassandra
session = cluster.connect('hypespace')

@app.route('/')
@app.route('/index')
def index():
  return app.send_static_file('freeboard/index.html')

@app.route('/api/v1/status')
def get_status():
       stmt = 'SELECT * FROM status'
       print(stmt)
       response = session.execute(stmt)
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{'deviceID': x.deviceid.replace('\"', ''), 'door-open': x.dooropen, 'defrosted': x.defrosted, 'efficiency': x.efficiency} for x in response_list]
       return jsonify(status=jsonresponse)

@app.route('/api/v1/status/<id>')
def get_door(id):
       id='"'+id+'"'
       stmt = 'SELECT * FROM status WHERE deviceID=%s LIMIT 1'
       print(stmt % id)
       response = session.execute(stmt,parameters=[id])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{'deviceID': x.deviceid.replace('\"', ''), 'door-open': x.dooropen, 'defrosted': x.defrosted, 'efficiency': x.efficiency} for x in response_list]
       return jsonify(status=jsonresponse)

@app.route('/api/v1/timeline/<id>')
def get_timeline(id):
       id='"'+id+'"'
       stmt = 'SELECT * FROM timeline WHERE deviceID=%s LIMIT 1'
       print(stmt % id)
       response = session.execute(stmt,parameters=[id])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{'deviceID': x.deviceid.replace('\"', ''), 'time_stamp': x.time_stamp, 'sensorName1': x.sensorname1, 'sensorValue1': x.sensorvalue1, 'sensorName2': x.sensorname2, 'sensorValue2': x.sensorvalue2,} for x in response_list]
       return jsonify(timeline=jsonresponse)

