#!/bin/bash

PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=flink-cluster

peg up master.yml &
peg up workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka
peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} flink

peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get install git"
peg sshcmd-cluster ${CLUSTER_NAME} "git clone https://github.com/tyaq/scrat.git"

peg service ${CLUSTER_NAME} zookeeper start
peg service ${CLUSTER_NAME} kafka start
peg service ${CLUSTER_NAME} hadoop start
peg service ${CLUSTER_NAME} flink start

peg sshcmd-node ${CLUSTER_NAME} 1 "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic device_activity_stream --partitions 4 --replication-factor 2"