"""Shared code for the Distributed Search Engine (shipped to Spark executors as a zip).

Modules
-------
config      settings read from the environment (scripts/env.sh exports them)
textproc    wikitext cleaning + tokenizer (used by BOTH Spark indexing and the FastAPI query side)
validation  record parsing/validation rules for pages.jsonl / changes.jsonl
tables      Iceberg table DDL + snapshot / property helpers  (needs pyspark)
fsutil      Hadoop FileSystem helpers through the Spark JVM    (needs pyspark)
webhdfs     tiny dependency-free WebHDFS client (Node 1 upload fallback, Node 4 puller)
"""

__version__ = "1.0.0"
