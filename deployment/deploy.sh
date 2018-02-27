#!/bin/bash

echo "Deploying Clusters..."
chmod +x ./producer-cluster/producer.sh
chmod +x ./flink-cluster/flink_kafka.sh
chmod +x ./cassandra-cluster/cassandra.sh
chmod +x ./web-server/server.sh

bash ./producer-cluster/producer.sh
bash ./flink-cluster/flink_kafka.sh
bash ./cassandra-cluster/cassandra.sh
bash ./web-server/server.sh
echo "Finished Deploying Clusters."