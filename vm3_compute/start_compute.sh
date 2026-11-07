#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 3 - start Spark Master + Spark Worker + HDFS DataNode
#   bash vm3_compute/start_compute.sh
# Start order across the cluster: Node 2 (NameNode) -> Node 3 (this) -> Node 2 worker
# (a worker started before the master simply retries until the master is up).
# ---------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/../scripts/env.sh"
dse_banner
[ "$NODE_ID" = "3" ] || echo "WARNING: NODE_ID=$NODE_ID but this script is meant for node 3"
[ -f "$SPARK_CONF_DIR/spark-env.sh" ] || { echo "run scripts/render_configs.sh first"; exit 1; }
mkdir -p "$HDFS_DATA_ROOT/datanode" "$HDFS_DATA_ROOT/pids" "$DSE_ROOT/logs"

if pgrep -f org.apache.spark.deploy.master.Master >/dev/null; then echo "[spark] master already running"
else "$SPARK_HOME/sbin/start-master.sh"; fi
sleep 3

if pgrep -f 'hdfs.server.datanode.DataNode' >/dev/null; then echo "[hdfs] datanode already running"
else hdfs --daemon start datanode; fi

if pgrep -f org.apache.spark.deploy.worker.Worker >/dev/null; then echo "[spark] worker already running"
else "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"; fi

sleep 3
echo; jps | grep -v Jps
cat <<EOF

  Spark master  : $SPARK_MASTER_URL
  Spark UI      : http://${NODE3_IP}:${SPARK_MASTER_WEBUI_PORT}   (should list 2 ALIVE workers)
  HDFS UI       : http://${NODE2_IP}:${HDFS_NAMENODE_HTTP_PORT}  -> Datanodes tab (2 live)
  History server: \$SPARK_HOME/sbin/start-history-server.sh  -> http://${NODE3_IP}:18080
EOF
