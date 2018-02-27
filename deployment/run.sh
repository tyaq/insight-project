#!/bin/bash

echo "Starting Scrat Job..."
# Turn on web server
peg sshcmd-node web-server 1 "cd ~/scrat/www \
&& sudo nohup -E python tornadoapp.py &"

# Turn on flink job
peg sshcmd-node flink-cluster 1 "cd ~/scrat/proccesing/flinkStream \
&& mvn clean package -Pbuild-jar \
&& $FLINK_HOME/bin/flink run -c org.insight.flinkStream.sensorStream target/sensor-stream-0.1.jar"

# Turn on python producers
# Please change ip address (eg. 10.0.0.9) to your kafka's VPC ip
local hostnames=($(fetch_cluster_hostnames flink-cluster))
peg sshcmd-cluster producer-cluster "cd ~/scrat/data/ \
bash spawn_kafka_streams.sh ${hostnames[0]} 2 k1"

echo "Scrat is alive =)"