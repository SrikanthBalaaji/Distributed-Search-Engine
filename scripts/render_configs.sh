#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Render Hadoop + Spark configs for THIS node into config/generated/nodeN/
#   bash scripts/render_configs.sh
# Your existing $HADOOP_HOME/etc/hadoop and $SPARK_HOME/conf are NOT touched:
# env.sh points HADOOP_CONF_DIR / SPARK_CONF_DIR at the generated dirs.
# Re-run whenever config/cluster.env or config/local.env changes.
# ---------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/env.sh"
dse_banner

if [ "$NODE_ID" = "4" ]; then
  echo "[render] Node 4 runs only Python (FastAPI + WebHDFS puller); no Hadoop/Spark config needed."
  exit 0
fi

TPL="$DSE_ROOT/config/templates"
mkdir -p "$HADOOP_CONF_DIR" "$SPARK_CONF_DIR" "$DSE_ROOT/logs/spark" "$DSE_ROOT/logs/hadoop"

render() {  # render <template> <output>   (${VAR} substitution from the environment)
  python3 - "$1" "$2" <<'PY'
import os, string, sys
src, dst = sys.argv[1], sys.argv[2]
try:
    out = string.Template(open(src).read()).substitute(os.environ)
except KeyError as e:
    sys.exit(f"[render] variable {e} used in {src} is not set (check config/*.env)")
open(dst, "w").write(out)
print(f"[render] {dst}")
PY
}

# ---- Hadoop: start from the installed defaults, then overlay ours ----
if [ -n "${HADOOP_HOME:-}" ] && [ -d "$HADOOP_HOME/etc/hadoop" ]; then
  cp -n "$HADOOP_HOME"/etc/hadoop/* "$HADOOP_CONF_DIR"/ 2>/dev/null || true
else
  echo "[render] WARNING: HADOOP_HOME ($HADOOP_HOME) not found - only writing site files"
fi
render "$TPL/hadoop/core-site.xml.tmpl" "$HADOOP_CONF_DIR/core-site.xml"
render "$TPL/hadoop/hdfs-site.xml.tmpl" "$HADOOP_CONF_DIR/hdfs-site.xml"
render "$TPL/hadoop/workers.tmpl"       "$HADOOP_CONF_DIR/workers"
{
  echo ""
  echo "# ---- added by DSE render_configs.sh ----"
  echo "export JAVA_HOME=\"$JAVA_HOME\""
  echo "export HADOOP_LOG_DIR=\"$DSE_ROOT/logs/hadoop\""
  echo "export HADOOP_PID_DIR=\"$HDFS_DATA_ROOT/pids\""
} >> "$HADOOP_CONF_DIR/hadoop-env.sh"
# keep hadoop-env.sh from growing on every re-render
awk '!(/^# ---- added by DSE/ || /^export (JAVA_HOME|HADOOP_LOG_DIR|HADOOP_PID_DIR)=/) || !seen[$0]++' \
  "$HADOOP_CONF_DIR/hadoop-env.sh" > "$HADOOP_CONF_DIR/hadoop-env.sh.tmp" && \
  mv "$HADOOP_CONF_DIR/hadoop-env.sh.tmp" "$HADOOP_CONF_DIR/hadoop-env.sh"

# ---- Spark ----
render "$TPL/spark/spark-defaults.conf.tmpl" "$SPARK_CONF_DIR/spark-defaults.conf"
render "$TPL/spark/spark-env.sh.tmpl"        "$SPARK_CONF_DIR/spark-env.sh"
chmod +x "$SPARK_CONF_DIR/spark-env.sh"
if [ -n "${SPARK_HOME:-}" ] && [ -f "$SPARK_HOME/conf/log4j2.properties.template" ]; then
  sed 's/^rootLogger.level = info/rootLogger.level = warn/' \
    "$SPARK_HOME/conf/log4j2.properties.template" > "$SPARK_CONF_DIR/log4j2.properties"
fi

mkdir -p "$HDFS_DATA_ROOT"
echo
echo "[render] done. HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "[render]       SPARK_CONF_DIR=$SPARK_CONF_DIR"
if [ ! -f "$ICEBERG_JAR" ]; then
  echo "[render] NOTE: Iceberg jar not found at $ICEBERG_JAR -> run: bash scripts/install_iceberg.sh"
fi
