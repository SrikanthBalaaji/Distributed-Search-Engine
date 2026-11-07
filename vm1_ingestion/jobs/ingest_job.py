"""
NODE 1 - Ingestion Spark job  (driver runs on Node 1, executors on Nodes 2 + 3)

    bash scripts/submit.sh vm1_ingestion/jobs/ingest_job.py \
         --phase 0 --input hdfs://dse-node2:9000/dse/raw/phase0/pages.jsonl

Steps
  1. read the raw JSONL from HDFS, validate every line on the executors
     (malformed JSON, missing required fields, bad op/url/links/version)
  2. deduplicate by doc_id inside the batch: highest version wins, ties -> earliest line
  3. merge with the CURRENT Iceberg `pages` table (pinned snapshot):
        incoming record replaces stored one only if its version is strictly higher
        DELETE -> tombstone row (is_deleted=true) so lower versions can't resurrect it
  4. recompute `links` for every live doc = raw_links that point to docs that exist
     (removes dangling links, incl. links to docs deleted in this phase)
  5. MERGE only the rows that actually changed -> one new Iceberg snapshot
     (re-ingesting the same phase changes nothing => no new snapshot = idempotent)
  6. write rejected/superseded lines to `rejected_records`, a summary row to `ingest_log`
"""
import argparse
import hashlib
import os
import sys
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, LongType, StringType, StructField, StructType)

from dse import config as C
from dse.sparkutil import banner, get_spark
from dse.tables import current_snapshot_id, ensure_tables, read_at
from dse.validation import parse_record, to_row

PARSED_SCHEMA = StructType([
    StructField("line_no", LongType()),
    StructField("doc_id", StringType()),
    StructField("url", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("raw_links", ArrayType(StringType())),
    StructField("op", StringType()),
    StructField("version", LongType()),
    StructField("reject_reason", StringType()),
    StructField("raw_preview", StringType()),
])

PAGE_COLUMNS = ["doc_id", "url", "title", "text", "raw_links", "links", "version",
                "is_deleted", "content_hash", "phase", "batch_id", "source_file", "ingested_at"]

def empty_links():
    return F.array().cast(ArrayType(StringType()))


def parse_args():
    p = argparse.ArgumentParser(description="DSE ingestion job (Node 1)")
    p.add_argument("--input", required=True, help="HDFS path of pages.jsonl / changes.jsonl")
    p.add_argument("--phase", type=int, required=True, help="0 = base corpus, 1 = change set, ...")
    p.add_argument("--dry-run", action="store_true", help="validate + diff, but don't commit")
    return p.parse_args()


def _line_hash(line):
    return int(hashlib.sha1(line.encode("utf-8")).hexdigest()[:15], 16)


def main():
    args = parse_args()
    started = datetime.now()
    spark = get_spark("dse-ingest-phase%d" % args.phase)
    sc = spark.sparkContext
    ensure_tables(spark)
    T_PAGES = C.table(C.PAGES)
    phase = args.phase
    source = args.input

    banner("NODE 1 :: INGESTION  phase=%d" % phase, input=source, table=T_PAGES)

    # ------------------------------------------------------------------
    # 1. parse + validate on the executors
    # ------------------------------------------------------------------
    raw = sc.textFile(source)
    indexed = raw.zipWithIndex().cache()
    total_lines = indexed.count()
    if total_lines == 0:
        raise SystemExit("input file is empty: %s" % source)
    content_sig = indexed.map(lambda t: _line_hash(t[0])).sum() % (1 << 60)
    batch_id = "p%d-%d-%x" % (phase, total_lines, content_sig)

    parsed_rdd = indexed.map(lambda t: to_row(parse_record(t[0], t[1] + 1, phase)))
    parsed = spark.createDataFrame(parsed_rdd, PARSED_SCHEMA).cache()

    accepted = parsed.filter(F.col("reject_reason").isNull())
    rejected = parsed.filter(F.col("reject_reason").isNotNull())

    # ------------------------------------------------------------------
    # 2. dedup inside the batch: highest version wins, tie -> earliest line
    # ------------------------------------------------------------------
    w = Window.partitionBy("doc_id").orderBy(F.col("version").desc(), F.col("line_no").asc())
    ranked = accepted.withColumn("_rn", F.row_number().over(w))
    winners = ranked.filter("_rn = 1").drop("_rn")
    superseded = ranked.filter("_rn > 1").drop("_rn")

    winners = winners.withColumn(
        "content_hash",
        F.when(F.col("op") == "DELETE", F.lit("__deleted__")).otherwise(
            F.sha2(F.concat_ws("\u0001", "url", "title", "text",
                               F.concat_ws("\u0002", "raw_links")), 256)))

    # ------------------------------------------------------------------
    # 3. merge with the current table state (pinned snapshot -> consistent re-reads)
    # ------------------------------------------------------------------
    base_sid = current_snapshot_id(spark, C.PAGES)
    if base_sid is None:
        current = spark.createDataFrame([], spark.table(T_PAGES).schema)
    else:
        current = read_at(spark, C.PAGES, base_sid)

    c = current.select([F.col(x).alias("c_" + x) for x in current.columns])
    b = winners.select([F.col(x).alias("b_" + x) for x in winners.columns])
    j = c.join(b, F.col("c_doc_id") == F.col("b_doc_id"), "full_outer")

    take_b = F.col("b_doc_id").isNotNull() & (
        F.col("c_doc_id").isNull() | (F.col("b_version") > F.col("c_version")))
    b_del = F.col("b_op") == "DELETE"
    now = F.current_timestamp()

    def pick(b_expr, c_col):
        return F.when(take_b, b_expr).otherwise(F.col(c_col))

    state = j.select(
        F.coalesce("b_doc_id", "c_doc_id").alias("doc_id"),
        pick(F.when(b_del, F.coalesce("b_url", "c_url")).otherwise(F.col("b_url")), "c_url").alias("url"),
        pick(F.when(b_del, F.coalesce("b_title", "c_title")).otherwise(F.col("b_title")), "c_title").alias("title"),
        pick(F.when(b_del, F.lit(None).cast("string")).otherwise(F.col("b_text")), "c_text").alias("text"),
        pick(F.when(b_del, empty_links()).otherwise(F.col("b_raw_links")), "c_raw_links").alias("raw_links"),
        pick(F.col("b_version"), "c_version").cast("bigint").alias("version"),
        pick(b_del, "c_is_deleted").alias("is_deleted"),
        pick(F.col("b_content_hash"), "c_content_hash").alias("content_hash"),
        pick(F.lit(phase), "c_phase").cast("int").alias("phase"),
        pick(F.lit(batch_id), "c_batch_id").alias("batch_id"),
        pick(F.lit(source), "c_source_file").alias("source_file"),
        pick(now, "c_ingested_at").alias("ingested_at"),
        # previous values for change detection
        F.col("c_doc_id").isNotNull().alias("_c_exists"),
        F.col("c_version").alias("_c_version"),
        F.col("c_is_deleted").alias("_c_is_deleted"),
        F.col("c_content_hash").alias("_c_hash"),
        F.col("c_links").alias("_c_links"),
    ).cache()

    # ------------------------------------------------------------------
    # 4. dangling-link removal against the NEW set of live documents
    # ------------------------------------------------------------------
    live_ids = state.filter(~F.col("is_deleted")).select(F.col("doc_id").alias("dst"))
    exploded = (state.filter(~F.col("is_deleted"))
                .select("doc_id", F.posexplode("raw_links").alias("pos", "dst")))
    valid = exploded.join(F.broadcast(live_ids), "dst").filter(F.col("dst") != F.col("doc_id"))
    cleaned = (valid.groupBy("doc_id")
               .agg(F.sort_array(F.collect_list(F.struct("pos", "dst"))).alias("pairs"))
               .select("doc_id", F.expr("array_distinct(transform(pairs, p -> p.dst))").alias("links")))
    state = (state.join(cleaned, "doc_id", "left")
             .withColumn("links", F.coalesce(F.col("links"), empty_links())))

    # ------------------------------------------------------------------
    # 5. keep only rows that really changed
    # ------------------------------------------------------------------
    def differs(a, bcol):
        return ~F.col(a).eqNullSafe(F.col(bcol))

    links_changed = F.concat_ws("\u0001", "links") != F.coalesce(F.concat_ws("\u0001", "_c_links"), F.lit(""))
    changed_flag = (~F.col("_c_exists") | differs("version", "_c_version")
                    | differs("is_deleted", "_c_is_deleted") | differs("content_hash", "_c_hash")
                    | links_changed)
    changed = state.filter(changed_flag).cache()

    kinds = changed.select(
        F.when(~F.col("_c_exists") & ~F.col("is_deleted"), "inserted")
        .when(~F.col("_c_exists") & F.col("is_deleted"), "tombstone_only")
        .when(F.col("is_deleted") & ~F.col("_c_is_deleted"), "deleted")
        .when(F.col("is_deleted"), "tombstone_update")
        .when(F.col("_c_is_deleted"), "inserted")  # resurrected by a higher version
        .when(F.col("content_hash") != F.col("_c_hash"), "updated")
        .when(links_changed, "links_repaired")
        .otherwise("version_bump").alias("kind"))
    kind_counts = {r["kind"]: r["count"] for r in kinds.groupBy("kind").count().collect()}

    stats = {
        "total_lines": total_lines,
        "accepted": accepted.count(),
        "rejected": rejected.count(),
        "duplicates": superseded.count(),
        "upserts": winners.filter("op = 'UPSERT'").count(),
        "deletes": winners.filter("op = 'DELETE'").count(),
    }
    live = state.filter(~F.col("is_deleted"))
    link_totals = live.select(F.sum(F.size("raw_links")).alias("raw"), F.sum(F.size("links")).alias("ok")).first()
    dangling_removed = int((link_totals["raw"] or 0) - (link_totals["ok"] or 0))
    live_docs = live.count()
    n_changed = changed.count()

    print("\n--- validation ---------------------------------------------------")
    for k in ("total_lines", "accepted", "rejected", "duplicates", "upserts", "deletes"):
        print("  %-28s %d" % (k, stats[k]))
    print("  rejected by reason:")
    for r in rejected.groupBy("reject_reason").count().orderBy(F.desc("count")).collect():
        print("     %-25s %d" % (r["reject_reason"], r["count"]))
    print("\n--- change detection vs Iceberg snapshot %s ---" % base_sid)
    for k, v in sorted(kind_counts.items()):
        print("  %-28s %d" % (k, v))
    print("  %-28s %d" % ("rows to commit", n_changed))
    print("  %-28s %d" % ("dangling links removed", dangling_removed))
    print("  %-28s %d" % ("live documents after merge", live_docs))

    status = "DRY_RUN" if args.dry_run else ("APPLIED" if n_changed else "NOOP")
    new_sid = base_sid
    if not args.dry_run:
        # ---- 5b. commit page changes in ONE Iceberg MERGE ----
        if n_changed:
            changed.select(*PAGE_COLUMNS).createOrReplaceTempView("dse_staged_pages")
            spark.sql("""
                MERGE INTO {t} t
                USING dse_staged_pages s
                ON t.doc_id = s.doc_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """.format(t=T_PAGES))
            new_sid = current_snapshot_id(spark, C.PAGES)
        else:
            print("\n  nothing changed -> no new snapshot (idempotent re-run)")

        # ---- 6a. rejected + superseded lines (only ones not recorded before) ----
        rej = (rejected.select("line_no", "doc_id", F.col("reject_reason").alias("reason"), "raw_preview")
               .unionByName(superseded.select(
                   "line_no", "doc_id", F.lit("duplicate_superseded").alias("reason"),
                   F.lit(None).cast("string").alias("raw_preview"))))
        rej = rej.select(F.lit(batch_id).alias("batch_id"), F.lit(phase).cast("int").alias("phase"),
                         F.lit(source).alias("source_file"), "line_no", "doc_id", "reason",
                         "raw_preview", F.current_timestamp().alias("rejected_at"))
        existing = spark.table(C.table(C.REJECTED)).filter(F.col("batch_id") == batch_id).select("line_no")
        new_rej = rej.join(existing, "line_no", "left_anti").select(*rej.columns).cache()
        if new_rej.count():
            new_rej.writeTo(C.table(C.REJECTED)).append()

    # ---- 6b. ingest log ----
    log_row = [(batch_id, phase, source, status, stats["total_lines"], stats["accepted"],
                stats["rejected"], stats["duplicates"], stats["upserts"], stats["deletes"],
                kind_counts.get("inserted", 0), kind_counts.get("updated", 0),
                kind_counts.get("deleted", 0), kind_counts.get("links_repaired", 0),
                dangling_removed, live_docs, new_sid, started, datetime.now())]
    if not args.dry_run:
        log_schema = spark.table(C.table(C.INGEST_LOG)).schema
        spark.createDataFrame(log_row, log_schema).writeTo(C.table(C.INGEST_LOG)).append()

    banner("INGESTION %s" % status, batch_id=batch_id, pages_snapshot_before=base_sid,
           pages_snapshot_after=new_sid, live_docs=live_docs,
           next_step="Node 3: bash vm3_compute/run_pipeline.sh")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
