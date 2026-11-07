#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 2 - show HDFS health and prove replication factor 2
#   bash vm2_storage/hdfs_report.sh
# ---------------------------------------------------------------------
set -uo pipefail
source "$(dirname "$0")/../scripts/env.sh"
echo "===== datanodes ====="
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|^Name:|^Hostname:|DFS Used%|Last contact"
echo; echo "===== project tree (replication column = 2nd field) ====="
hdfs dfs -ls -R "$HDFS_RAW_DIR" "$HDFS_SERVING_DIR" 2>/dev/null | head -40
echo; echo "===== block placement of raw inputs ====="
hdfs fsck "$HDFS_RAW_DIR" -files -blocks -locations 2>/dev/null | grep -E "^/|blk_|Average block replication|Under-replicated|Missing" | head -30
echo; echo "===== Iceberg warehouse size ====="
hdfs dfs -du -h "$HDFS_WAREHOUSE_DIR" 2>/dev/null
echo; echo "===== filesystem check ====="
hdfs fsck / 2>/dev/null | grep -E "Status|Total blocks|Default replication|Average block replication|Under-replicated|Corrupt"
