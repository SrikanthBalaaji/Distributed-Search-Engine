#!/usr/bin/env bash
# NODE 3 - stop Spark worker, Spark master, HDFS DataNode
set -uo pipefail
source "$(dirname "$0")/../scripts/env.sh"
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
"$SPARK_HOME/sbin/stop-history-server.sh" 2>/dev/null
hdfs --daemon stop datanode
jps | grep -v Jps || true
