#!/bin/bash

PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=producer-cluster

peg up workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws

peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get install git"
peg sshcmd-cluster ${CLUSTER_NAME} "git clone https://github.com/tyaq/scrat.git"
peg sshcmd-cluster ${CLUSTER_NAME} "pip install kafka-python"
