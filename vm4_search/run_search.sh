#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 4 - start the FastAPI search service + UI
#   bash vm4_search/run_search.sh                 # serve + auto-pull new exports every 30 s
#   bash vm4_search/run_search.sh --no-auto-pull  # serve only (pull manually / via Node 3 notify)
#   DSE_PROFILE=toy bash vm4_search/run_search.sh # serve the toy profile
# UI: http://<NODE4_IP>:8000      API docs: http://<NODE4_IP>:8000/docs
# ---------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../scripts/env.sh"

export AUTO_PULL=1
[ "${1:-}" = "--no-auto-pull" ] && export AUTO_PULL=0
export SERVING_DIR WEBHDFS_URL DSE_SERVING_DIR ADMIN_TOKEN DSE_PROFILE
if [ "$DSE_LOCAL" = "1" ]; then
  export PULL_SOURCE_DIR="${DSE_FS_URI#file://}${DSE_SERVING_DIR}"
fi
mkdir -p "$SERVING_DIR"

PY="$HERE/.venv/bin/python"
[ -x "$PY" ] || PY="$(command -v python3)"
cd "$HERE"
echo "[node4] profile=$DSE_PROFILE  serving_dir=$SERVING_DIR  source=${PULL_SOURCE_DIR:-$WEBHDFS_URL$DSE_SERVING_DIR}  auto_pull=$AUTO_PULL"
echo "[node4] UI -> http://${THIS_NODE_IP}:${SEARCH_PORT}"
exec "$PY" -m uvicorn app.main:app --host 0.0.0.0 --port "$SEARCH_PORT"
