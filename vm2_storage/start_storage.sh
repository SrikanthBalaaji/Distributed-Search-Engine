#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 2 - start HDFS NameNode + DataNode + Spark Worker
#   bash vm2_storage/start_storage.sh  [--no-worker]
# Daemons are started per-node (no ssh between VMs needed).
# ---------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/../scripts/env.sh"
[ -f "$HADOOP_CONF_DIR/hdfs-site.xml" ] || { echo "run scripts/render_configs.sh first"; exit 1; }
mkdir -p "$HDFS_DATA_ROOT/pids" "$DSE_ROOT/logs"

if pgrep -f 'hdfs.server.namenode.NameNode' >/dev/null; then echo "[hdfs] namenode already running"
else hdfs --daemon start namenode; fi
if pgrep -f 'hdfs.server.datanode.DataNode' >/dev/null; then echo "[hdfs] datanode already running"
else hdfs --daemon start datanode; fi

if [ "${1:-}" != "--no-worker" ]; then
  if pgrep -f org.apache.spark.deploy.worker.Worker >/dev/null; then echo "[spark] worker already running"
  else "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"; fi
fi
sleep 3
jps | grep -v Jps
echo
echo "  NameNode UI : http://${NODE2_IP}:${HDFS_NAMENODE_HTTP_PORT}"
echo "  Spark master: $SPARK_MASTER_URL (Node 3 must be running for the worker to register)"
