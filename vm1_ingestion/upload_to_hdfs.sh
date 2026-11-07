#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 1 - upload a local input file to HDFS (replication factor 2).
#   bash vm1_ingestion/upload_to_hdfs.sh <local_file> <hdfs_dir>
# Uses the `hdfs` CLI (client only, no daemons) if installed, otherwise WebHDFS.
# ---------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/../scripts/env.sh"

LOCAL="$1"; DEST_DIR="$2"
NAME="$(basename "$LOCAL")"
[ -f "$LOCAL" ] || { echo "[upload] no such file: $LOCAL" >&2; exit 1; }

if [ "$DSE_LOCAL" = "1" ]; then
  TARGET="${DSE_FS_URI#file://}${DEST_DIR}"
  mkdir -p "$TARGET" && cp -f "$LOCAL" "$TARGET/$NAME"
  echo "[upload] (local mode) copied to $TARGET/$NAME"
  exit 0
fi

if command -v hdfs >/dev/null 2>&1 && [ -f "$HADOOP_CONF_DIR/core-site.xml" ]; then
  hdfs dfs -mkdir -p "$DEST_DIR"
  hdfs dfs -D dfs.replication="$HDFS_REPLICATION" -put -f "$LOCAL" "$DEST_DIR/$NAME"
  echo "[upload] hdfs dfs -put done:"
  hdfs dfs -stat "  replication=%r  size=%b bytes  blocksize=%o  %n" "$DEST_DIR/$NAME"
  echo "[upload] block locations:"
  hdfs fsck "$DEST_DIR/$NAME" -files -blocks -locations 2>/dev/null | grep -E "blk_|Replica" | sed 's/^/   /' | head -10 || true
else
  echo "[upload] hdfs CLI not available -> using WebHDFS ($WEBHDFS_URL)"
  python3 -m dse.webhdfs put "$LOCAL" "$DEST_DIR/$NAME"
fi
