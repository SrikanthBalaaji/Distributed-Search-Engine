#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 3 - Spark fault-tolerance demo (deliverable #4).
#   bash vm3_compute/fault_tolerance_demo.sh
#
# Runs PageRank slowly (sleep between iterations). While it runs, a teammate on
# NODE 2 kills that node's executor / worker:
#     bash scripts/simulate_failure.sh executor      (or: worker)
# Expected: the driver logs "Lost executor ... on <node2>", failed tasks are retried
# on the Node 3 worker, lost RDD partitions are recomputed from lineage / the last
# HDFS checkpoint, and PageRank still finishes with sum(PR)=1.
# Afterwards on Node 2:   bash scripts/simulate_failure.sh restore
# ---------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../scripts/env.sh"
DELAY="${1:-12}"
cat <<EOF
================================================================
 FAULT TOLERANCE DEMO - PageRank with ${DELAY}s pause per iteration
 1. Open the Spark UI: http://${NODE3_IP}:${SPARK_MASTER_WEBUI_PORT}  (2 workers ALIVE)
 2. After ~3 iterations, on NODE 2 run:
        bash scripts/simulate_failure.sh executor     # or 'worker'
 3. Watch this log + the 'Executors' tab of the app UI (http://${NODE3_IP}:4040)
 4. Restore on Node 2:  bash scripts/simulate_failure.sh restore
================================================================
EOF
sleep 3
bash "$DSE_ROOT/scripts/submit.sh" "$HERE/jobs/pagerank_job.py" --iterations 12 \
  --checkpoint-every 3 --iteration-delay "$DELAY" 2>&1 | \
  grep -E --line-buffered "iter |Lost executor|lost|Removed|Resubmitted|Retry|TaskSetManager.*(Lost|failed)|converged|PAGERANK|top 10|^\s+[0-9]\.[0-9]+"
echo
echo "HDFS side: stop a DataNode (bash scripts/simulate_failure.sh datanode on node 3) and run"
echo "  hdfs fsck /dse/raw -files -blocks -locations   -> every block still readable (RF=2)"
