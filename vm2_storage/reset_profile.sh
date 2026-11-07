#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 2 - wipe ALL tables + exports of a profile to start the phases again.
#   DSE_PROFILE=toy bash vm2_storage/reset_profile.sh --yes     # toy only
#   bash vm2_storage/reset_profile.sh --yes                     # FULL dataset (careful!)
# Node 4 should also delete its local $SERVING_DIR/v* afterwards.
# ---------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/../scripts/env.sh"
[ "${1:-}" = "--yes" ] || { echo "This deletes namespace '$DSE_NAMESPACE' and $DSE_SERVING_DIR. Re-run with --yes"; exit 1; }

if [ "$DSE_LOCAL" = "1" ]; then
  ROOT="${DSE_FS_URI#file://}"
  rm -rf "$ROOT$HDFS_WAREHOUSE_DIR/$DSE_NAMESPACE" "$ROOT$DSE_SERVING_DIR" "$ROOT$DSE_RAW_DIR"
else
  hdfs dfs -rm -r -f -skipTrash "$HDFS_WAREHOUSE_DIR/$DSE_NAMESPACE" "$DSE_SERVING_DIR/*" "$DSE_RAW_DIR/*"
  hdfs dfs -mkdir -p "$DSE_SERVING_DIR" "$DSE_RAW_DIR"
  hdfs dfs -chmod 777 "$DSE_SERVING_DIR" "$DSE_RAW_DIR"
fi
echo "[reset] profile '$DSE_PROFILE' wiped (Iceberg namespace $DSE_NAMESPACE, $DSE_SERVING_DIR, $DSE_RAW_DIR)"
