"""Node 4 settings (environment variables; vm4_search/run_search.sh exports them from config/*.env)."""
import os
import sys

# make common/dse importable when running from vm4_search/
_COMMON = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "common"))
if _COMMON not in sys.path:
    sys.path.insert(0, _COMMON)


def _bool(name, default=False):
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


PROFILE = os.environ.get("DSE_PROFILE", "full")                   # full | toy
SERVING_DIR = os.environ.get("SERVING_DIR", "/serving")          # local /serving/vN/
CURRENT_FILE = "CURRENT"                                          # SERVING_DIR/CURRENT -> "v3"

# ranking (spec): Final = 0.7 * BM25/max_BM25 + 0.3 * PR/max_PR
BM25_K1 = float(os.environ.get("BM25_K1", 1.2))
BM25_B = float(os.environ.get("BM25_B", 0.75))
W_BM25 = float(os.environ.get("W_BM25", 0.7))
W_PAGERANK = float(os.environ.get("W_PAGERANK", 0.3))
# max_PageRank taken over the matching documents ("candidates") or the whole corpus ("global")
PR_NORM = os.environ.get("PR_NORM", "candidates")

WATCH_INTERVAL = float(os.environ.get("WATCH_INTERVAL", 5))      # seconds between CURRENT checks
MAX_CACHED_VERSIONS = int(os.environ.get("MAX_CACHED_VERSIONS", 3))
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

# background puller (WebHDFS) inside the API process
AUTO_PULL = _bool("AUTO_PULL", False)
PULL_INTERVAL = float(os.environ.get("PULL_INTERVAL", 30))
WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "")
HDFS_SERVING_DIR = os.environ.get("DSE_SERVING_DIR", os.environ.get("HDFS_SERVING_DIR", "/serving"))
SOURCE_DIR = os.environ.get("PULL_SOURCE_DIR", "")                # local-mode alternative to WebHDFS

FRONTEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
