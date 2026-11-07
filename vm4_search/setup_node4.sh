#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 4 - one-time setup: python venv + dependencies + serving directory
#   bash vm4_search/setup_node4.sh
# ---------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../scripts/env.sh"

if ! python3 -c "import venv, ensurepip" 2>/dev/null; then
  echo "[node4] installing python3-venv (needs sudo)"
  sudo apt-get update -y && sudo apt-get install -y python3-venv python3-pip
fi
python3 -m venv "$HERE/.venv"
"$HERE/.venv/bin/pip" install --upgrade pip -q
"$HERE/.venv/bin/pip" install -r "$HERE/requirements.txt"

for d in "$SERVING_DIR" "${SERVING_DIR}_toy"; do
  if [ ! -d "$d" ]; then
    sudo mkdir -p "$d" && sudo chown "$USER":"$USER" "$d"
  fi
  echo "[node4] serving dir ready: $d"
done
echo
echo "[node4] done. Start the service with:  bash vm4_search/run_search.sh"
echo "        test WebHDFS reachability:   curl \"$WEBHDFS_URL/webhdfs/v1/?op=LISTSTATUS\""
