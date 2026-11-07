#!/usr/bin/env bash
# NODE 2 - Iceberg admin CLI (snapshots, time travel, rollback, maintenance)
#   bash vm2_storage/iceberg_admin.sh tables
#   bash vm2_storage/iceberg_admin.sh snapshots pages
#   bash vm2_storage/iceberg_admin.sh doc 94
#   bash vm2_storage/iceberg_admin.sh rollback pages <snapshot_id>
#   bash vm2_storage/iceberg_admin.sh --help
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
bash "$HERE/../scripts/submit.sh" "$HERE/jobs/iceberg_admin.py" "$@" 2>&1 | { grep -v -E " (INFO|WARN) |^\[submit\]" || true; }
