"""Central settings. Values come from environment variables exported by scripts/env.sh,
so every node / job agrees on table names, HDFS paths and ranking parameters."""
import os


def _env(name, default):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


# ---- Iceberg ----------------------------------------------------------
CATALOG = _env("DSE_CATALOG", "iceberg_catalog")
NAMESPACE = _env("DSE_NAMESPACE", "dse")

# ---- File system (hdfs://dse-node2:9000 in cluster mode, file://... in local mode)
FS_URI = _env("DSE_FS_URI", "hdfs://dse-node2:9000").rstrip("/")
RAW_DIR = _env("DSE_RAW_DIR", "/dse/raw")
SERVING_DIR = _env("DSE_SERVING_DIR", "/serving")
CHECKPOINT_DIR = _env("DSE_CHECKPOINT_DIR", "/dse/checkpoints")
SHUFFLE_PARTITIONS = int(_env("DSE_SHUFFLE_PARTITIONS", "8"))

# ---- Ranking configuration (fixed by the project spec) ----------------
BM25_K1 = 1.2
BM25_B = 0.75
PAGERANK_DAMPING = 0.85
PAGERANK_MIN_ITERATIONS = 10
W_BM25 = 0.7
W_PAGERANK = 0.3

# ---- Export ------------------------------------------------------------
BODY_MAX_CHARS = 20000  # cleaned text kept per doc for snippets on Node 4

# ---- Table names ---------------------------------------------------------
PAGES = "pages"
POSTINGS = "postings"
PAGERANK = "pagerank"
REJECTED = "rejected_records"
INGEST_LOG = "ingest_log"
PIPELINE_LOG = "pipeline_log"
ALL_TABLES = (PAGES, POSTINGS, PAGERANK, REJECTED, INGEST_LOG, PIPELINE_LOG)

# Iceberg table property that records which pages snapshot a derived table was built from
SOURCE_SNAPSHOT_PROP = "dse.source-pages-snapshot"


def table(name):
    """Fully-qualified Iceberg table name, e.g. iceberg_catalog.dse.pages"""
    return "%s.%s.%s" % (CATALOG, NAMESPACE, name)


def fs_path(path):
    """Absolute URI on the cluster FS for an absolute path like /serving/v3"""
    if "://" in path:
        return path
    return FS_URI + "/" + path.lstrip("/")
