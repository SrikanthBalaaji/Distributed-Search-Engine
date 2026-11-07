#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 1 - run one ingestion phase end-to-end:
#   local precheck  ->  upload to HDFS  ->  spark-submit ingest_job to the Node 3 master
#
#   bash vm1_ingestion/run_phase.sh 0                 # Phase 0: data/full/pages.jsonl
#   bash vm1_ingestion/run_phase.sh 1                 # Phase 1: data/full/changes.jsonl
#   DSE_PROFILE=toy bash vm1_ingestion/run_phase.sh 0 # toy dataset (separate tables)
#   bash vm1_ingestion/run_phase.sh 2 --file data/demo/bad_update.jsonl
#   options: --dry-run (validate + diff only)   --skip-upload (file already in HDFS)
# ---------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../scripts/env.sh"

PHASE="${1:-}"; shift || true
[[ "$PHASE" =~ ^[0-9]+$ ]] || { sed -n '2,13p' "$0"; exit 1; }

FILE=""; DRY=""; SKIP_UPLOAD=0
while [ $# -gt 0 ]; do
  case "$1" in
    --file) FILE="$2"; shift 2 ;;
    --dry-run) DRY="--dry-run"; shift ;;
    --skip-upload) SKIP_UPLOAD=1; shift ;;
    *) echo "unknown option $1"; exit 1 ;;
  esac
done

if [ -z "$FILE" ]; then
  if [ "$DSE_PROFILE" = "toy" ]; then
    case "$PHASE" in 0) FILE="$DSE_ROOT/data/toy/phase0_toy_pages.jsonl" ;;
                     1) FILE="$DSE_ROOT/data/toy/phase1_toy_changes.jsonl" ;; esac
  else
    case "$PHASE" in 0) FILE="$DSE_ROOT/data/full/pages.jsonl" ;;
                     1) FILE="$DSE_ROOT/data/full/changes.jsonl" ;; esac
  fi
fi
[ -n "$FILE" ] || { echo "phase $PHASE needs --file <path>"; exit 1; }

dse_banner
HDFS_DIR="${DSE_RAW_DIR}/phase${PHASE}"
HDFS_FILE="${DSE_FS_URI}${HDFS_DIR}/$(basename "$FILE")"

echo "### [1/3] local validation"
python3 "$HERE/precheck.py" "$FILE" --phase "$PHASE"

if [ "$SKIP_UPLOAD" = "0" ]; then
  echo; echo "### [2/3] upload to HDFS  ->  $HDFS_DIR"
  bash "$HERE/upload_to_hdfs.sh" "$FILE" "$HDFS_DIR"
fi

echo; echo "### [3/3] Spark ingestion job (master: $SPARK_MASTER_URL)"
mkdir -p "$DSE_ROOT/logs"
LOG="$DSE_ROOT/logs/ingest_phase${PHASE}_$(date +%Y%m%d_%H%M%S).log"
bash "$DSE_ROOT/scripts/submit.sh" "$HERE/jobs/ingest_job.py" --phase "$PHASE" --input "$HDFS_FILE" $DRY 2>&1 | tee "$LOG"
echo "log: $LOG"
