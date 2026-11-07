"""
NODE 3 - Export the validated index + document metadata as plain Parquet for serving.

    bash scripts/submit.sh vm3_compute/jobs/export_job.py

Output on HDFS (Node 4 pulls it over WebHDFS):

    /serving/vN/
        docs/part-*.parquet    doc_id, url, title, body, length, pagerank, out_degree,
                               in_degree, links, version, phase
        index/part-*.parquet   term, doc_id, tf, df, length       (inverted index)
        manifest.json          counts, snapshot ids, parameters, validation results, file list
        _SUCCESS               written LAST - Node 4 only accepts versions that have it

Consistency: postings and pagerank must have been built from the SAME pages snapshot;
the export reads pages AS OF that snapshot (Iceberg time travel), so concurrent
ingestion can't produce a mixed export. Everything is validated BEFORE publishing;
data is written to /serving/_tmp_vN_* and atomically renamed to /serving/vN.
If any check fails nothing is published and Node 4 keeps serving the previous version.
"""
import argparse
import json
import re
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from dse import config as C
from dse import fsutil
from dse.sparkutil import banner, get_spark
from dse.tables import (current_snapshot_id, ensure_tables, get_property, log_pipeline, read_at)
from dse.textproc import clean_wikitext


def body_text(text):
    return clean_wikitext(text or "")[: C.BODY_MAX_CHARS]


def next_version(spark):
    names = fsutil.list_names(spark, C.fs_path(C.SERVING_DIR))
    nums = [int(m.group(1)) for n in names for m in [re.match(r"^v(\d+)$", n)] if m]
    return (max(nums) + 1) if nums else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--allow-stale", action="store_true",
                    help="export even if pages has a newer snapshot than the index")
    args = ap.parse_args()
    started = datetime.now()

    spark = get_spark("dse-export")
    ensure_tables(spark)
    checks = []

    def check(name, ok, detail=""):
        checks.append({"check": name, "passed": bool(ok), "detail": str(detail)})
        print("  [%s] %-34s %s" % ("PASS" if ok else "FAIL", name, detail))

    post_src = get_property(spark, C.POSTINGS, C.SOURCE_SNAPSHOT_PROP)
    pr_src = get_property(spark, C.PAGERANK, C.SOURCE_SNAPSHOT_PROP)
    pages_now = current_snapshot_id(spark, C.PAGES)
    banner("NODE 3 :: EXPORT", postings_built_from=post_src, pagerank_built_from=pr_src,
           pages_current=pages_now)
    if not post_src or not pr_src:
        raise SystemExit("run index_job and pagerank_job before exporting")
    if post_src != pr_src:
        raise SystemExit("postings (%s) and pagerank (%s) were built from different pages snapshots "
                         "- re-run the pipeline" % (post_src, pr_src))
    sid = int(post_src)
    if pages_now != sid:
        msg = "pages has a newer snapshot (%s) than the index (%s)" % (pages_now, sid)
        if not args.allow_stale:
            raise SystemExit(msg + " - run index_job + pagerank_job first (or --allow-stale)")
        print("WARNING:", msg)

    post_sid = current_snapshot_id(spark, C.POSTINGS)
    pr_sid = current_snapshot_id(spark, C.PAGERANK)
    pages = read_at(spark, C.PAGES, sid)
    live = pages.filter("NOT is_deleted")
    postings = read_at(spark, C.POSTINGS, post_sid)
    pagerank = read_at(spark, C.PAGERANK, pr_sid)

    # ---------------- build serving tables ----------------
    doc_len = postings.groupBy("doc_id").agg(F.sum("tf").cast("int").alias("length"))
    term_df = postings.groupBy("term").agg(F.count("*").cast("int").alias("df"))
    body_udf = F.udf(body_text, StringType())

    docs = (live.join(doc_len, "doc_id", "left").join(pagerank, "doc_id", "left")
            .select("doc_id", "url", "title", body_udf("text").alias("body"),
                    F.coalesce("length", F.lit(0)).cast("int").alias("length"),
                    F.col("rank").alias("pagerank"),
                    F.coalesce("out_degree", F.lit(0)).cast("int").alias("out_degree"),
                    F.coalesce("in_degree", F.lit(0)).cast("int").alias("in_degree"),
                    "links", "version", "phase")
            .orderBy("doc_id")
            .cache())
    index = (postings.join(term_df, "term").join(doc_len, "doc_id")
             .select("term", "doc_id", "tf", "df", "length")
             .orderBy("term", "doc_id")
             .cache())

    # ---------------- validation (before anything is published) ----------------
    print("\nvalidation:")
    n_docs = docs.count()
    n_index = index.count()
    check("docs_not_empty", n_docs > 0, "%d docs" % n_docs)
    check("index_not_empty", n_index > 0, "%d postings" % n_index)
    n_distinct = docs.select("doc_id").distinct().count()
    check("doc_id_unique", n_distinct == n_docs, "%d distinct / %d" % (n_distinct, n_docs))
    bad_fields = docs.filter(F.col("doc_id").isNull() | F.col("url").isNull() | (F.trim("url") == "")
                             | F.col("title").isNull()).count()
    check("required_fields_present", bad_fields == 0, "%d bad rows" % bad_fields)
    ids = docs.select(F.col("doc_id").alias("dst"))
    dangling = (docs.select(F.explode("links").alias("dst")).join(ids, "dst", "left_anti").count())
    check("no_dangling_links", dangling == 0, "%d dangling" % dangling)
    missing_pr = docs.filter(F.col("pagerank").isNull()).count()
    check("pagerank_for_every_doc", missing_pr == 0, "%d missing" % missing_pr)
    pr_sum = docs.agg(F.sum("pagerank")).first()[0] or 0.0
    check("pagerank_sums_to_1", abs(pr_sum - 1.0) < 1e-3, "sum=%.6f" % pr_sum)
    orphan_postings = index.select("doc_id").distinct().join(
        docs.select("doc_id"), "doc_id", "left_anti").count()
    check("postings_reference_live_docs", orphan_postings == 0, "%d orphan doc ids" % orphan_postings)
    bad_counts = index.filter((F.col("tf") <= 0) | (F.col("df") <= 0) | (F.col("df") > n_docs)).count()
    check("tf_df_ranges", bad_counts == 0, "%d bad rows" % bad_counts)

    failed = [c for c in checks if not c["passed"]]
    if failed:
        log_pipeline(spark, "export", "FAILED_VALIDATION", sid, None, {"checks": checks}, started)
        print("\nEXPORT ABORTED - %d check(s) failed. Nothing published; Node 4 keeps the old version."
              % len(failed))
        spark.stop()
        return 2

    # ---------------- write to a temp dir, verify, atomically publish ----------------
    version_no = next_version(spark)
    version = "v%d" % version_no
    serving_root = C.fs_path(C.SERVING_DIR)
    tmp = "%s/_tmp_%s_%s" % (serving_root, version, started.strftime("%Y%m%d%H%M%S"))
    final = "%s/%s" % (serving_root, version)
    fsutil.mkdirs(spark, serving_root)

    docs.coalesce(1).write.mode("overwrite").parquet(tmp + "/docs")
    index.coalesce(1).write.mode("overwrite").parquet(tmp + "/index")
    rb_docs = spark.read.parquet(tmp + "/docs").count()
    rb_index = spark.read.parquet(tmp + "/index").count()
    check("readback_docs", rb_docs == n_docs, "%d rows" % rb_docs)
    check("readback_index", rb_index == n_index, "%d rows" % rb_index)
    if rb_docs != n_docs or rb_index != n_index:
        fsutil.delete(spark, tmp)
        log_pipeline(spark, "export", "FAILED_READBACK", sid, None, {"checks": checks}, started)
        return 3

    stats = docs.agg(F.avg("length").alias("avgdl"), F.sum("length").alias("tokens"),
                     F.sum("out_degree").alias("edges"), F.max("phase").alias("phase")).first()
    files = [{"path": p, "size": s} for p, s in fsutil.list_files(spark, tmp)
             if p.endswith(".parquet")]
    manifest = {
        "version": version, "version_number": version_no,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "producer": "spark-export-job",
        "phase": int(stats["phase"]) if stats["phase"] is not None else None,
        "pages_snapshot_id": sid, "postings_snapshot_id": post_sid, "pagerank_snapshot_id": pr_sid,
        "counts": {"docs": n_docs, "index_rows": n_index,
                   "terms": term_df.count(), "edges": int(stats["edges"] or 0)},
        "stats": {"avgdl": float(stats["avgdl"] or 0.0), "total_tokens": int(stats["tokens"] or 0)},
        "bm25": {"k1": C.BM25_K1, "b": C.BM25_B},
        "ranking": {"w_bm25": C.W_BM25, "w_pagerank": C.W_PAGERANK},
        "pagerank": {"damping": C.PAGERANK_DAMPING,
                     "iterations": int(get_property(spark, C.PAGERANK, "dse.iterations") or 0),
                     "final_delta": get_property(spark, C.PAGERANK, "dse.final-delta"),
                     "min_iterations": C.PAGERANK_MIN_ITERATIONS},
        "validation": checks,
        "files": files,
    }
    fsutil.write_text(spark, tmp + "/manifest.json", json.dumps(manifest, indent=2))
    if not fsutil.rename(spark, tmp, final):
        raise SystemExit("could not rename %s -> %s" % (tmp, final))
    fsutil.write_text(spark, final + "/_SUCCESS", "")
    fsutil.write_text(spark, serving_root + "/LATEST", version + "\n")

    log_pipeline(spark, "export", "OK", sid, None,
                 {"version": version, "docs": n_docs, "index_rows": n_index}, started)
    banner("EXPORT PUBLISHED", version=final, docs=n_docs, index_rows=n_index,
           phase=manifest["phase"], next_step="Node 4 pulls it (auto-poll or POST /api/admin/pull)")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
