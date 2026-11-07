#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Spark 3.3.x (PySpark) only supports Python <= 3.10, but Ubuntu 24.04 ships 3.12.
# This installs python3.10 NEXT TO the system python (python3 stays 3.12).
# Run on Nodes 1, 2 and 3 (every node that runs a Spark driver or worker).
#   bash scripts/install_python310.sh
# Result: /usr/bin/python3.10 or /usr/local/bin/python3.10 -> same command name on
# every node, which is what spark.pyspark.python needs.
# ---------------------------------------------------------------------
set -euo pipefail

if command -v python3.10 >/dev/null 2>&1; then
  echo "[py310] already installed: $(python3.10 --version) at $(command -v python3.10)"
else
  echo "[py310] trying the deadsnakes PPA (needs sudo)..."
  if sudo apt-get update -y && sudo apt-get install -y software-properties-common \
     && sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt-get update -y \
     && sudo apt-get install -y python3.10; then
    echo "[py310] installed from deadsnakes"
  else
    echo "[py310] PPA failed -> installing with uv (standalone Python build)"
    if ! command -v uv >/dev/null 2>&1 && [ ! -x "$HOME/.local/bin/uv" ]; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
    fi
    UV="$(command -v uv || echo "$HOME/.local/bin/uv")"
    "$UV" python install 3.10
    sudo ln -sf "$("$UV" python find 3.10)" /usr/local/bin/python3.10
  fi
fi

python3.10 -c 'import sys; assert sys.version_info[:2] == (3, 10); print("[py310] OK:", sys.version.split()[0])'

ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/config/local.env"
if [ -f "$ENV_FILE" ]; then
  if grep -q '^PYSPARK_PYTHON=' "$ENV_FILE"; then
    sed -i 's/^PYSPARK_PYTHON=.*/PYSPARK_PYTHON=python3.10/' "$ENV_FILE"
  else
    echo "PYSPARK_PYTHON=python3.10" >> "$ENV_FILE"
  fi
  echo "[py310] config/local.env -> PYSPARK_PYTHON=python3.10"
  echo "[py310] now re-run:  bash scripts/render_configs.sh"
else
  echo "[py310] create config/local.env (cp config/local.env.example config/local.env); it already uses python3.10"
fi
