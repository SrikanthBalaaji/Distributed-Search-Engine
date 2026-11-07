#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Add dse-node1..4 -> ZeroTier IP aliases to /etc/hosts (run on ALL 4 VMs).
#   bash scripts/setup_hosts.sh
# Idempotent: replaces the block between the markers on every run.
# HDFS DataNodes advertise these names, and WebHDFS redirects to them,
# so every VM (including Node 4) must be able to resolve them.
# ---------------------------------------------------------------------
set -euo pipefail
DSE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
set -a; source "$DSE_ROOT/config/cluster.env"; set +a

BEGIN="# >>> dse-cluster (managed by distributed-search-engine) >>>"
END="# <<< dse-cluster <<<"
BLOCK="$BEGIN
${NODE1_IP}  ${NODE1_HOST}
${NODE2_IP}  ${NODE2_HOST}
${NODE3_IP}  ${NODE3_HOST}
${NODE4_IP}  ${NODE4_HOST}
$END"

TMP="$(mktemp)"
sed "/^${BEGIN//\//\\/}\$/,/^${END//\//\\/}\$/d" /etc/hosts > "$TMP"
printf '%s\n' "$BLOCK" >> "$TMP"
sudo cp /etc/hosts "/etc/hosts.bak.$(date +%Y%m%d%H%M%S)"
sudo cp "$TMP" /etc/hosts
rm -f "$TMP"

echo "[hosts] /etc/hosts now contains:"
grep -A5 "dse-cluster (managed" /etc/hosts
if grep -Eq "^127\.[0-9.]+\s+.*\b(${NODE1_HOST}|${NODE2_HOST}|${NODE3_HOST}|${NODE4_HOST})\b" /etc/hosts; then
  echo "[hosts] WARNING: a dse-nodeX alias is also mapped to 127.x - remove that line, HDFS will break." >&2
fi
for h in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST" "$NODE4_HOST"; do
  printf "  %-10s -> %s\n" "$h" "$(getent hosts "$h" | awk '{print $1}')"
done
