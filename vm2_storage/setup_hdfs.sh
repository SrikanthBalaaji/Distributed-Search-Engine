#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 2 - ONE-TIME HDFS setup: format the NameNode, start NameNode + DataNode,
# create the project directories.
#   bash vm2_storage/setup_hdfs.sh             # first time
#   bash vm2_storage/setup_hdfs.sh --reformat  # wipe & re-create (also wipe node 3's datanode dir!)
# ---------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../scripts/env.sh"
dse_banner
[ "$NODE_ID" = "2" ] || { echo "this script is for node 2 (NODE_ID=$NODE_ID)"; exit 1; }
[ -f "$HADOOP_CONF_DIR/hdfs-site.xml" ] || { echo "run scripts/render_configs.sh first"; exit 1; }

NN_DIR="$HDFS_DATA_ROOT/namenode"
if [ "${1:-}" = "--reformat" ]; then
  echo "[hdfs] --reformat: stopping daemons and wiping $HDFS_DATA_ROOT/{namenode,datanode}"
  hdfs --daemon stop datanode 2>/dev/null || true
  hdfs --daemon stop namenode 2>/dev/null || true
  rm -rf "$NN_DIR" "$HDFS_DATA_ROOT/datanode"
  echo "[hdfs] !! On NODE 3 also run:  rm -rf \$HDFS_DATA_ROOT/datanode  before starting its DataNode"
fi
mkdir -p "$NN_DIR" "$HDFS_DATA_ROOT/datanode" "$HDFS_DATA_ROOT/pids" "$DSE_ROOT/logs"

if [ ! -d "$NN_DIR/current" ]; then
  echo "[hdfs] formatting NameNode (clusterId=dse-cluster)"
  hdfs namenode -format -force -nonInteractive -clusterId dse-cluster
else
  echo "[hdfs] NameNode already formatted - skipping format"
fi

bash "$HERE/start_storage.sh" --no-worker

echo "[hdfs] waiting for safe mode to end..."
hdfs dfsadmin -safemode wait

echo "[hdfs] creating project directories"
for d in "$HDFS_RAW_DIR" "${HDFS_RAW_DIR}_toy" "$HDFS_WAREHOUSE_DIR" "$HDFS_SERVING_DIR" \
         "${HDFS_SERVING_DIR}_toy" "$HDFS_CHECKPOINT_DIR" "$HDFS_EVENTLOG_DIR"; do
  hdfs dfs -mkdir -p "$d"
  hdfs dfs -chmod 777 "$d"
done
hdfs dfs -ls /
echo
echo "[hdfs] done. Now start the DataNode on NODE 3 (bash vm3_compute/start_compute.sh)"
echo "       then check: hdfs dfsadmin -report   -> 'Live datanodes (2)'"
