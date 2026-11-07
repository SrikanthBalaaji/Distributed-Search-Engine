#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 4 - pull /serving/vN from HDFS (WebHDFS) into $SERVING_DIR, validate, activate.
#   bash vm4_search/pull_export.sh              # once
#   bash vm4_search/pull_export.sh --watch 30   # poll forever
# A running search service notices the CURRENT switch within a few seconds.
# ---------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../scripts/env.sh"
export SERVING_DIR WEBHDFS_URL DSE_SERVING_DIR
if [ "$DSE_LOCAL" = "1" ]; then
  export PULL_SOURCE_DIR="${DSE_FS_URI#file://}${DSE_SERVING_DIR}"
fi
PY="$HERE/.venv/bin/python"
[ -x "$PY" ] || PY="$(command -v python3)"
cd "$HERE"
exec "$PY" -m puller.pull_export "$@"
