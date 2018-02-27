#!/bin/bash

PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=cassandra-cluster

peg up master.yml &
peg up workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} cassandra

peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get install git"
peg sshcmd-cluster ${CLUSTER_NAME} "git clone https://github.com/tyaq/scrat.git"

peg service ${CLUSTER_NAME} cassandra start

peg sshcmd-node ${CLUSTER_NAME} 1 "cqlsh -f ~/scrat/db/create_db.cql"