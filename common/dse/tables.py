"""Iceberg table definitions and helpers (Spark side).

Tables (namespace `dse`, Hadoop catalog, warehouse on HDFS /warehouse - Node 2 owns it):

  pages             one row per document id (live docs AND delete tombstones)
  postings          inverted index postings  (term, doc_id, tf)
  pagerank          PageRank score per live doc
  rejected_records  every rejected / superseded input line (audit of dirty data)
  ingest_log        one row per ingestion run
  pipeline_log      one row per index / pagerank / export run
"""
import json
from datetime import datetime

from dse import config as C

COW = (
    "'format-version'='2', 'write.update.mode'='copy-on-write', "
    "'write.delete.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'"
)

DDL = {
    C.PAGES: """
        CREATE TABLE IF NOT EXISTS {t} (
            doc_id        STRING  COMMENT 'document id (primary key)',
            url           STRING,
            title         STRING,
            text          STRING  COMMENT 'raw wikitext; NULL for tombstones',
            raw_links     ARRAY<STRING> COMMENT 'links as received (validated, may dangle)',
            links         ARRAY<STRING> COMMENT 'links to docs that currently exist',
            version       BIGINT  COMMENT 'record version (explicit or phase number)',
            is_deleted    BOOLEAN COMMENT 'tombstone - keeps the version so older records cannot resurrect it',
            content_hash  STRING,
            phase         INT,
            batch_id      STRING,
            source_file   STRING,
            ingested_at   TIMESTAMP
        ) USING iceberg TBLPROPERTIES ({cow})""",
    C.POSTINGS: """
        CREATE TABLE IF NOT EXISTS {t} (
            term    STRING,
            doc_id  STRING,
            tf      INT
        ) USING iceberg TBLPROPERTIES ({cow})""",
    C.PAGERANK: """
        CREATE TABLE IF NOT EXISTS {t} (
            doc_id      STRING,
            rank        DOUBLE,
            out_degree  INT,
            in_degree   INT
        ) USING iceberg TBLPROPERTIES ({cow})""",
    C.REJECTED: """
        CREATE TABLE IF NOT EXISTS {t} (
            batch_id     STRING,
            phase        INT,
            source_file  STRING,
            line_no      BIGINT,
            doc_id       STRING,
            reason       STRING,
            raw_preview  STRING,
            rejected_at  TIMESTAMP
        ) USING iceberg TBLPROPERTIES ({cow})""",
    C.INGEST_LOG: """
        CREATE TABLE IF NOT EXISTS {t} (
            batch_id          STRING,
            phase             INT,
            source_file       STRING,
            status            STRING,
            total_lines       BIGINT,
            accepted          BIGINT,
            rejected          BIGINT,
            duplicates        BIGINT,
            upserts           BIGINT,
            deletes           BIGINT,
            inserted_docs     BIGINT,
            updated_docs      BIGINT,
            deleted_docs      BIGINT,
            links_repaired    BIGINT,
            dangling_removed  BIGINT,
            live_docs         BIGINT,
            pages_snapshot_id BIGINT,
            started_at        TIMESTAMP,
            finished_at       TIMESTAMP
        ) USING iceberg TBLPROPERTIES ({cow})""",
    C.PIPELINE_LOG: """
        CREATE TABLE IF NOT EXISTS {t} (
            stage              STRING,
            status             STRING,
            pages_snapshot_id  BIGINT,
            output_snapshot_id BIGINT,
            details            STRING,
            started_at         TIMESTAMP,
            finished_at        TIMESTAMP
        ) USING iceberg TBLPROPERTIES ({cow})""",
}


def ensure_tables(spark):
    """Create the namespace and all tables if they don't exist (idempotent)."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS %s.%s" % (C.CATALOG, C.NAMESPACE))
    for name, ddl in DDL.items():
        spark.sql(ddl.format(t=C.table(name), cow=COW))


def table_exists(spark, name):
    try:
        spark.table(C.table(name)).schema  # noqa: B018
        return True
    except Exception:  # AnalysisException
        return False


def current_snapshot_id(spark, name):
    """Snapshot id of the table's current state (correct after rollbacks), or None."""
    t = C.table(name)
    try:
        rows = spark.sql("SELECT snapshot_id FROM %s.refs WHERE name = 'main'" % t).collect()
        if rows:
            return int(rows[0][0])
        return None
    except Exception:
        rows = spark.sql(
            "SELECT snapshot_id FROM %s.history ORDER BY made_current_at DESC LIMIT 1" % t
        ).collect()
        return int(rows[0][0]) if rows else None


def read_at(spark, name, snapshot_id):
    """DataFrame of the table as of a snapshot (Iceberg time travel)."""
    return spark.sql("SELECT * FROM %s VERSION AS OF %d" % (C.table(name), int(snapshot_id)))


def get_property(spark, name, key):
    for row in spark.sql("SHOW TBLPROPERTIES %s" % C.table(name)).collect():
        if row[0] == key:
            return row[1]
    return None


def set_properties(spark, name, props):
    items = ", ".join("'%s'='%s'" % (k, str(v).replace("'", "")) for k, v in props.items())
    spark.sql("ALTER TABLE %s SET TBLPROPERTIES (%s)" % (C.table(name), items))


def log_pipeline(spark, stage, status, pages_snapshot_id, output_snapshot_id, details, started_at):
    from pyspark.sql.types import (LongType, StringType, StructField, StructType, TimestampType)

    schema = StructType([
        StructField("stage", StringType()), StructField("status", StringType()),
        StructField("pages_snapshot_id", LongType()), StructField("output_snapshot_id", LongType()),
        StructField("details", StringType()), StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
    ])
    row = (stage, status,
           int(pages_snapshot_id) if pages_snapshot_id is not None else None,
           int(output_snapshot_id) if output_snapshot_id is not None else None,
           json.dumps(details, default=str), started_at, datetime.now())
    spark.createDataFrame([row], schema).writeTo(C.table(C.PIPELINE_LOG)).append()
