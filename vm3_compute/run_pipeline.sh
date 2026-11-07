#!/usr/bin/env bash
# ---------------------------------------------------------------------
# NODE 3 - run the compute pipeline after every ingestion phase:
#     inverted index  ->  PageRank (>=10 iterations)  ->  validated Parquet export /serving/vN
#     -> notify Node 4 to pull + activate the new version
#
#   bash vm3_compute/run_pipeline.sh                    # incremental index update
#   bash vm3_compute/run_pipeline.sh --full-index       # rebuild index from scratch
#   bash vm3_compute/run_pipeline.sh --iterations 30    # PageRank max iterations
#   DSE_PROFILE=toy bash vm3_compute/run_pipeline.sh    # toy tables
#   options: --skip-index --skip-pagerank --skip-export --no-notify
# ---------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../scripts/env.sh"

INDEX_ARGS=(); PR_ARGS=(); DO_INDEX=1; DO_PR=1; DO_EXPORT=1; NOTIFY=1
while [ $# -gt 0 ]; do
  case "$1" in
    --full-index) INDEX_ARGS+=(--full); shift ;;
    --iterations) PR_ARGS+=(--iterations "$2"); shift 2 ;;
    --iteration-delay) PR_ARGS+=(--iteration-delay "$2"); shift 2 ;;
    --skip-index) DO_INDEX=0; shift ;;
    --skip-pagerank) DO_PR=0; shift ;;
    --skip-export) DO_EXPORT=0; shift ;;
    --no-notify) NOTIFY=0; shift ;;
    *) echo "unknown option $1"; exit 1 ;;
  esac
done

dse_banner
mkdir -p "$DSE_ROOT/logs"
TS="$(date +%Y%m%d_%H%M%S)"
run() {  # run <name> <job.py> [args]
  local name="$1"; shift
  local log="$DSE_ROOT/logs/${name}_${TS}.log"
  echo; echo "### ${name}  (log: $log)"
  local t0=$SECONDS
  if ! bash "$DSE_ROOT/scripts/submit.sh" "$@" 2>&1 | tee "$log" | { grep -v -E " (INFO|WARN) |^\s*$" || true; }; then
    echo "!!! ${name} FAILED - see $log. Node 4 keeps serving the previous version." >&2
    exit 1
  fi
  echo "### ${name} finished in $((SECONDS - t0))s"
}

[ "$DO_INDEX" = 1 ]  && run index    "$HERE/jobs/index_job.py"    "${INDEX_ARGS[@]+"${INDEX_ARGS[@]}"}"
[ "$DO_PR" = 1 ]     && run pagerank "$HERE/jobs/pagerank_job.py" "${PR_ARGS[@]+"${PR_ARGS[@]}"}"
[ "$DO_EXPORT" = 1 ] && run export   "$HERE/jobs/export_job.py"

if [ "$DO_EXPORT" = 1 ] && [ "$NOTIFY" = 1 ] && [ "$DSE_LOCAL" != "1" ]; then
  echo; echo "### notifying Node 4 ($SEARCH_URL)"
  if curl -fsS -m 120 -X POST "$SEARCH_URL/api/admin/pull?profile=${DSE_PROFILE}" \
        -H "X-Admin-Token: ${ADMIN_TOKEN:-}" ; then
    echo; echo "Node 4 pulled + activated the new version."
  else
    echo "Node 4 not reachable - it will pick the export up on its next poll (or run vm4_search/pull_export.sh)."
  fi
fi
