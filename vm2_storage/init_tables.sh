#!/usr/bin/env bash
# NODE 2 - create Iceberg namespace + tables on HDFS (safe to re-run)
#   bash vm2_storage/init_tables.sh          (DSE_PROFILE=toy for the toy namespace)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
bash "$HERE/../scripts/submit.sh" "$HERE/jobs/init_tables.py" 2>&1 | { grep -v -E " (INFO|WARN) " || true; }
