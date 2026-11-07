#!/usr/bin/env bash
# NODE 2 - stop Spark worker, HDFS DataNode, HDFS NameNode
set -uo pipefail
source "$(dirname "$0")/../scripts/env.sh"
"$SPARK_HOME/sbin/stop-worker.sh"
hdfs --daemon stop datanode
hdfs --daemon stop namenode
jps | grep -v Jps || true
