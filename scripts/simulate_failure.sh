#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Fault-tolerance demo helper (run on Node 2 or Node 3 while a job runs).
#   bash scripts/simulate_failure.sh executor   # kill -9 this node's Spark executor JVM(s)
#   bash scripts/simulate_failure.sh worker     # kill -9 the Spark worker + its executors
#   bash scripts/simulate_failure.sh datanode   # stop this node's HDFS DataNode
#   bash scripts/simulate_failure.sh restore    # restart worker + datanode
# ---------------------------------------------------------------------
set -uo pipefail
source "$(dirname "$0")/env.sh" || exit 1

case "${1:-}" in
  executor)
    pids=$(pgrep -f CoarseGrainedExecutorBackend || true)
    [ -z "$pids" ] && { echo "no executor running on this node right now"; exit 1; }
    echo "[fail] kill -9 executor(s): $pids"; kill -9 $pids
    echo "Spark will mark the executor lost, re-run its tasks on other executors and"
    echo "recompute lost partitions from lineage / the last PageRank checkpoint." ;;
  worker)
    pids=$(pgrep -f 'org.apache.spark.deploy.worker.Worker|CoarseGrainedExecutorBackend' || true)
    [ -z "$pids" ] && { echo "no worker running"; exit 1; }
    echo "[fail] kill -9 worker + executors: $pids"; kill -9 $pids
    echo "The master removes this worker; the job continues on the remaining worker." ;;
  datanode)
    echo "[fail] stopping DataNode on $(hostname)"; hdfs --daemon stop datanode
    echo "With replication=2 every block still has a live replica on the other DataNode."
    echo "Check on node2: hdfs dfsadmin -report | grep -E 'Live|Dead'   (dead after ~1 min)" ;;
  restore)
    hdfs --daemon start datanode 2>/dev/null && echo "[restore] datanode started"
    if ! pgrep -f org.apache.spark.deploy.worker.Worker >/dev/null; then
      "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL" && echo "[restore] worker started"
    fi ;;
  *) echo "usage: $0 executor|worker|datanode|restore"; exit 1 ;;
esac
