#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Health check from ANY node: ZeroTier reachability, service ports,
# software versions, HDFS + Spark status.
#   bash scripts/check_cluster.sh
# ---------------------------------------------------------------------
source "$(dirname "$0")/env.sh" || exit 1
dse_banner

ok()   { printf "  \e[32m[ OK ]\e[0m %s\n" "$*"; }
bad()  { printf "  \e[31m[FAIL]\e[0m %s\n" "$*"; }
info() { printf "  [info] %s\n" "$*"; }

port_open() { timeout 3 bash -c "</dev/tcp/$1/$2" 2>/dev/null; }

echo "== ZeroTier reachability"
for n in 1 2 3 4; do
  ipv="NODE${n}_IP"; hv="NODE${n}_HOST"
  if ping -c1 -W2 "${!ipv}" >/dev/null 2>&1; then ok "node$n ${!hv} (${!ipv}) reachable"
  else bad "node$n ${!hv} (${!ipv}) NOT reachable (zerotier-cli listnetworks?)"; fi
  r="$(getent hosts "${!hv}" | awk '{print $1}')"
  [ "$r" = "${!ipv}" ] && ok "  ${!hv} resolves to ${!ipv}" || bad "  ${!hv} resolves to '${r}' (run scripts/setup_hosts.sh)"
done

echo "== Service ports"
chk() { if port_open "$1" "$2"; then ok "$3 ($1:$2)"; else bad "$3 ($1:$2) closed"; fi; }
chk "$NODE2_IP" "$HDFS_NAMENODE_PORT"      "HDFS NameNode RPC   (node2)"
chk "$NODE2_IP" "$HDFS_NAMENODE_HTTP_PORT" "HDFS NameNode UI    (node2)"
chk "$NODE2_IP" 9866                       "HDFS DataNode       (node2)"
chk "$NODE3_IP" 9866                       "HDFS DataNode       (node3)"
chk "$NODE3_IP" "$SPARK_MASTER_PORT"       "Spark master        (node3)"
chk "$NODE3_IP" "$SPARK_MASTER_WEBUI_PORT" "Spark master UI     (node3)"
chk "$NODE4_IP" "$SEARCH_PORT"             "Search API          (node4)"

echo "== Local software"
command -v java >/dev/null && info "java: $(java -version 2>&1 | head -1)" || info "java: not found"
command -v python3 >/dev/null && info "python3: $(python3 --version 2>&1)"
[ -x "${SPARK_HOME:-/nonexistent}/bin/spark-submit" ] && info "spark: $("$SPARK_HOME/bin/spark-submit" --version 2>&1 | grep -m1 -o 'version [0-9.]*')" || info "spark: not installed (fine on node 4)"
command -v hdfs >/dev/null && info "hadoop: $(hadoop version 2>/dev/null | head -1)" || info "hadoop: not installed (fine on node 4)"
[ -f "$ICEBERG_JAR" ] && info "iceberg jar: $ICEBERG_JAR" || info "iceberg jar: missing (needed on nodes 1-3)"
command -v jps >/dev/null && info "JVMs here: $(jps 2>/dev/null | awk '$2!="Jps"{print $2}' | xargs)"

if command -v hdfs >/dev/null && [ -f "$HADOOP_CONF_DIR/core-site.xml" ]; then
  echo "== HDFS"
  hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|Configured Capacity|DFS Used%|^Name:" | sed 's/^/  /' \
    || bad "hdfs dfsadmin -report failed"
fi

echo "== Spark master"
if command -v curl >/dev/null; then
  curl -s "http://${NODE3_IP}:${SPARK_MASTER_WEBUI_PORT}/json/" 2>/dev/null | python3 -c '
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    print("  [FAIL] master UI not reachable"); sys.exit()
print("  status=%s workers=%d cores=%d memory=%dMB" % (d.get("status"), len(d.get("workers",[])), d.get("cores",0), d.get("memory",0)))
for w in d.get("workers",[]):
    print("   - worker %s state=%s cores=%s" % (w.get("host"), w.get("state"), w.get("cores")))
'
fi
