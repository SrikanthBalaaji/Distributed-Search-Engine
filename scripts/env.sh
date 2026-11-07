#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Shared environment for every script in this repo.
#     source scripts/env.sh            (normal cluster mode)
#     DSE_PROFILE=toy  source ...      (toy dataset -> separate tables/dirs)
#     DSE_LOCAL=1      source ...      (single-machine Spark local[*] test)
# ---------------------------------------------------------------------

DSE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DSE_ROOT

if [ ! -f "$DSE_ROOT/config/cluster.env" ]; then
  echo "[env] missing config/cluster.env" >&2; return 1 2>/dev/null || exit 1
fi

set -a
# shellcheck disable=SC1091
source "$DSE_ROOT/config/cluster.env"
if [ -f "$DSE_ROOT/config/local.env" ]; then
  # shellcheck disable=SC1091
  source "$DSE_ROOT/config/local.env"
fi
set +a

DSE_LOCAL="${DSE_LOCAL:-0}"
DSE_PROFILE="${DSE_PROFILE:-full}"
export DSE_LOCAL DSE_PROFILE

if [ "$DSE_LOCAL" = "1" ]; then
  NODE_ID="${NODE_ID:-3}"
fi
if [ -z "${NODE_ID:-}" ]; then
  echo "[env] NODE_ID not set. Run: cp config/local.env.example config/local.env && nano config/local.env" >&2
  return 1 2>/dev/null || exit 1
fi
export NODE_ID

# ---- profile: toy data gets its own namespace + HDFS dirs -----------
if [ "$DSE_PROFILE" = "toy" ]; then
  ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE}_toy"
  HDFS_RAW_DIR="${HDFS_RAW_DIR}_toy"
  HDFS_SERVING_DIR="${HDFS_SERVING_DIR}_toy"
  SERVING_DIR="${SERVING_DIR:-/serving}_toy"
fi

# ---- derived values --------------------------------------------------
_ip_var="NODE${NODE_ID}_IP";     THIS_NODE_IP="${!_ip_var}"
_host_var="NODE${NODE_ID}_HOST"; THIS_NODE_HOST="${!_host_var}"
export THIS_NODE_IP THIS_NODE_HOST

export HDFS_URI="hdfs://${NODE2_HOST}:${HDFS_NAMENODE_PORT}"
export WEBHDFS_URL="http://${NODE2_HOST}:${HDFS_NAMENODE_HTTP_PORT}"
export SPARK_MASTER_URL="spark://${NODE3_IP}:${SPARK_MASTER_PORT}"
export SEARCH_URL="http://${NODE4_IP}:${SEARCH_PORT}"
export ICEBERG_JAR="${ICEBERG_JAR:-/opt/iceberg/iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar}"
export HDFS_DATA_ROOT="${HDFS_DATA_ROOT:-$HOME/dse-hdfs}"
export SERVING_DIR="${SERVING_DIR:-/serving}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-1g}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"

# Generated (per-node) Hadoop/Spark configs - our own conf dirs, so the
# configs from earlier lab assignments in $HADOOP_HOME/etc are untouched.
export DSE_GEN_DIR="$DSE_ROOT/config/generated/node${NODE_ID}"
export HADOOP_CONF_DIR="$DSE_GEN_DIR/hadoop"
export SPARK_CONF_DIR="$DSE_GEN_DIR/spark"

# One HDFS identity for every VM -> no permission surprises between users
export HADOOP_USER_NAME="${HDFS_USER}"
export PYTHONHASHSEED=0

# ---- settings read by the Python jobs (common/dse/config.py) ---------
export DSE_CATALOG="$ICEBERG_CATALOG"
export DSE_NAMESPACE="$ICEBERG_NAMESPACE"
export DSE_RAW_DIR="$HDFS_RAW_DIR"
export DSE_SERVING_DIR="$HDFS_SERVING_DIR"
export DSE_CHECKPOINT_DIR="$HDFS_CHECKPOINT_DIR"
export DSE_SHUFFLE_PARTITIONS="$SPARK_SHUFFLE_PARTITIONS"
if [ "$DSE_LOCAL" = "1" ]; then
  export DSE_FS_URI="file://$DSE_ROOT/.local/fs"
  export SPARK_MASTER_URL="local[2]"
  export THIS_NODE_IP=127.0.0.1
  export WEBHDFS_URL=""
  export SERVING_DIR="$DSE_ROOT/.local/serving"
  if [ "$DSE_PROFILE" = "toy" ]; then export SERVING_DIR="$DSE_ROOT/.local/serving_toy"; fi
  export HADOOP_CONF_DIR=""
  export SPARK_CONF_DIR="$DSE_ROOT/.local/spark-conf"
else
  export DSE_FS_URI="$HDFS_URI"
fi
export DSE_WAREHOUSE="${DSE_FS_URI}${HDFS_WAREHOUSE_DIR}"

# ---- PATH / PYTHONPATH ----------------------------------------------
[ -n "${JAVA_HOME:-}" ]   && [ -d "$JAVA_HOME/bin" ]   && PATH="$JAVA_HOME/bin:$PATH"
[ -n "${HADOOP_HOME:-}" ] && [ -d "$HADOOP_HOME/bin" ] && PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
[ -n "${SPARK_HOME:-}" ]  && [ -d "$SPARK_HOME/bin" ]  && PATH="$SPARK_HOME/bin:$PATH"
export PATH
export PYTHONPATH="$DSE_ROOT/common${PYTHONPATH:+:$PYTHONPATH}"

dse_banner() {
  echo "------------------------------------------------------------"
  echo " DSE node ${NODE_ID} (${THIS_NODE_HOST} / ${THIS_NODE_IP})  profile=${DSE_PROFILE}  local=${DSE_LOCAL}"
  echo " HDFS=${DSE_FS_URI}  Spark=${SPARK_MASTER_URL}"
  echo " Iceberg=${DSE_CATALOG}.${DSE_NAMESPACE}  warehouse=${DSE_WAREHOUSE}"
  echo "------------------------------------------------------------"
}
