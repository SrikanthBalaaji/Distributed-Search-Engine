#!/usr/bin/env bash
# ---------------------------------------------------------------------
# spark-submit wrapper used by every node.
#   bash scripts/submit.sh <path/to/job.py> [job args...]
# - ships common/dse as a --py-files zip (executors need the tokenizer etc.)
# - cluster mode: uses config/generated/nodeN/spark/spark-defaults.conf
#   (master = Node 3, driver host = THIS node's ZeroTier IP, Iceberg catalog)
# - DSE_LOCAL=1: local[2] Spark + Iceberg warehouse on the local disk
# ---------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/env.sh"

if [ $# -lt 1 ]; then echo "usage: $0 <job.py> [args...]" >&2; exit 1; fi
JOB="$1"; shift

if [ -z "${SPARK_HOME:-}" ] || [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
  echo "[submit] SPARK_HOME ($SPARK_HOME) has no bin/spark-submit - fix config/local.env" >&2; exit 1
fi
if [ ! -f "$ICEBERG_JAR" ]; then
  echo "[submit] Iceberg jar missing: $ICEBERG_JAR  -> bash scripts/install_iceberg.sh" >&2; exit 1
fi

# ---- package the shared python code ----
mkdir -p "$DSE_ROOT/build"
ZIP="$DSE_ROOT/build/dse_common.zip"
rm -f "$ZIP"
( cd "$DSE_ROOT/common" && python3 - "$ZIP" <<'PY'
import sys, zipfile, pathlib
with zipfile.ZipFile(sys.argv[1], "w", zipfile.ZIP_DEFLATED) as z:
    for p in pathlib.Path("dse").rglob("*.py"):
        z.write(p)
PY
)

if [ "$DSE_LOCAL" = "1" ]; then
  mkdir -p "$DSE_ROOT/.local/fs" "$SPARK_CONF_DIR"
  ARGS=(
    --master "local[2]"
    --driver-memory 2g
    --jars "$ICEBERG_JAR"
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf "spark.sql.catalog.${DSE_CATALOG}=org.apache.iceberg.spark.SparkCatalog"
    --conf "spark.sql.catalog.${DSE_CATALOG}.type=hadoop"
    --conf "spark.sql.catalog.${DSE_CATALOG}.warehouse=${DSE_WAREHOUSE}"
    --conf spark.sql.shuffle.partitions=4
    --conf spark.default.parallelism=4
    --conf spark.ui.showConsoleProgress=false
    --conf spark.driver.host=127.0.0.1
    --conf spark.driver.bindAddress=127.0.0.1
    --conf spark.executorEnv.PYTHONHASHSEED=0
    --py-files "$ZIP"
  )
else
  PROPS="$SPARK_CONF_DIR/spark-defaults.conf"
  if [ ! -f "$PROPS" ]; then
    echo "[submit] $PROPS not found -> run: bash scripts/render_configs.sh" >&2; exit 1
  fi
  ARGS=( --properties-file "$PROPS" --py-files "$ZIP" )
fi

echo "[submit] $(basename "$JOB") on ${SPARK_MASTER_URL} (driver on node ${NODE_ID})"
exec "$SPARK_HOME/bin/spark-submit" "${ARGS[@]}" "$JOB" "$@"
