"""
NODE 3 - Distributed inverted index build / update  (driver on Node 3, tasks on Nodes 2 + 3)

    bash scripts/submit.sh vm3_compute/jobs/index_job.py [--full]

Map:     each executor cleans + tokenizes its share of documents and emits
         (term, doc_id, tf) postings                  (common/dse/textproc.py)
Shuffle: postings are hash-partitioned BY TERM, so every posting list for a term
         lives in one partition/file - a term-partitioned distributed inverted index
Store:   Iceberg table `postings` (term, doc_id, tf). df and doc length are derived
         from it at export time (df = #postings per term, length = sum(tf) per doc).

Incremental update: the table property `dse.source-pages-snapshot` remembers which
`pages` snapshot the index reflects. Next run diffs that snapshot against the current
one (Iceberg time travel) and only re-tokenizes added / changed / deleted documents.
"""
import argparse
import sys
from collections import Counter
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from dse import config as C
from dse.sparkutil import banner, get_spark
from dse.tables import (current_snapshot_id, ensure_tables, get_property, log_pipeline,
                        read_at, set_properties)
from dse.textproc import document_terms

POSTING_SCHEMA = StructType([
    StructField("term", StringType()),
    StructField("doc_id", StringType()),
    StructField("tf", IntegerType()),
])


def tokenize_partition(rows):
    """Executor side: one document -> its postings."""
    for r in rows:
        counts = Counter(document_terms(r["title"], r["text"]))
        for term, tf in counts.items():
            yield (term, r["doc_id"], int(tf))


def build_postings(spark, docs_df):
    rdd = docs_df.select("doc_id", "title", "text").rdd.mapPartitions(tokenize_partition)
    return (spark.createDataFrame(rdd, POSTING_SCHEMA)
            .repartition(C.SHUFFLE_PARTITIONS, "term")      # term-partitioned index
            .sortWithinPartitions("term", "doc_id"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="rebuild the whole index")
    args = ap.parse_args()
    started = datetime.now()

    spark = get_spark("dse-index")
    ensure_tables(spark)
    T_POST = C.table(C.POSTINGS)

    sid = current_snapshot_id(spark, C.PAGES)
    if sid is None:
        raise SystemExit("pages table is empty - run ingestion (Node 1) first")
    prev = get_property(spark, C.POSTINGS, C.SOURCE_SNAPSHOT_PROP)
    prev = int(prev) if prev else None
    banner("NODE 3 :: INVERTED INDEX", pages_snapshot=sid, indexed_snapshot=prev, table=T_POST)

    if prev == sid and not args.full:
        print("index already reflects pages snapshot %s - nothing to do" % sid)
        log_pipeline(spark, "index", "UP_TO_DATE", sid, current_snapshot_id(spark, C.POSTINGS), {}, started)
        spark.stop()
        return 0

    now_df = read_at(spark, C.PAGES, sid)
    mode = "full" if (args.full or prev is None) else "incremental"
    details = {"mode": mode}

    prev_df = None
    if mode == "incremental":
        try:
            prev_df = read_at(spark, C.PAGES, prev)
            prev_df.limit(1).collect()
        except Exception as e:  # snapshot expired / table rolled back past it
            print("previous snapshot %s not readable (%s) -> full rebuild" % (prev, str(e)[:120]))
            mode = details["mode"] = "full"

    if mode == "full":
        live = now_df.filter(~F.col("is_deleted"))
        postings = build_postings(spark, live)
        postings.writeTo(T_POST).overwrite(F.lit(True))
        details["docs_indexed"] = live.count()
    else:
        n = now_df.select("doc_id", F.col("content_hash").alias("h1"), F.col("is_deleted").alias("d1"))
        o = prev_df.select("doc_id", F.col("content_hash").alias("h0"), F.col("is_deleted").alias("d0"))
        diff = n.join(o, "doc_id", "full_outer").filter(
            ~F.col("h1").eqNullSafe(F.col("h0")) | ~F.col("d1").eqNullSafe(F.col("d0")))
        changed_ids = diff.select("doc_id").distinct().cache()
        n_changed = changed_ids.count()
        details["changed_docs"] = n_changed
        print("documents changed since snapshot %s: %d" % (prev, n_changed))
        if n_changed:
            changed_ids.createOrReplaceTempView("dse_changed_docs")
            # 1) drop old postings of every changed/deleted doc
            spark.sql("""MERGE INTO {t} t USING dse_changed_docs s ON t.doc_id = s.doc_id
                         WHEN MATCHED THEN DELETE""".format(t=T_POST))
            # 2) add postings for changed docs that are still live
            todo = now_df.join(changed_ids, "doc_id").filter(~F.col("is_deleted"))
            details["docs_indexed"] = todo.count()
            build_postings(spark, todo).writeTo(T_POST).append()

    # property is written LAST: if the job dies half-way, the next run redoes the diff
    set_properties(spark, C.POSTINGS, {C.SOURCE_SNAPSHOT_PROP: sid})
    post_sid = current_snapshot_id(spark, C.POSTINGS)

    post = spark.table(T_POST)
    stats = post.agg(F.count("*").alias("postings"), F.countDistinct("term").alias("terms"),
                     F.countDistinct("doc_id").alias("docs")).first()
    details.update(stats.asDict())
    print("\nindex mode=%s  postings=%d  terms=%d  docs_with_terms=%d"
          % (mode, stats["postings"], stats["terms"], stats["docs"]))
    print("top terms by document frequency:")
    for r in post.groupBy("term").agg(F.count("*").alias("df")).orderBy(F.desc("df"), "term").limit(10).collect():
        print("   %-20s df=%d" % (r["term"], r["df"]))

    log_pipeline(spark, "index", "OK", sid, post_sid, details, started)
    banner("INDEX DONE", pages_snapshot=sid, postings_snapshot=post_sid, mode=mode)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
