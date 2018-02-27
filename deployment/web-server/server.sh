#!/bin/bash

PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=web-server

peg up workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws

peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get update"
peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get install git"
peg sshcmd-cluster ${CLUSTER_NAME} "git clone https://github.com/tyaq/scrat.git"
peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get install nodejs"
peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get install npm"
peg sshcmd-cluster ${CLUSTER_NAME} "sudo pip install cassandra-driver"
peg sshcmd-cluster ${CLUSTER_NAME} "sudo pip install Flask"
peg sshcmd-cluster ${CLUSTER_NAME} "sudo npm install -g grunt-cli"

peg sshcmd-cluster ${CLUSTER_NAME} "cd scrat/www/app/static/freeboard && npm install && grunt"