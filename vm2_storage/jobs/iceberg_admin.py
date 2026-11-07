"""
NODE 2 - Iceberg lakehouse administration: snapshots, time travel, rollback, maintenance.

    bash vm2_storage/iceberg_admin.sh <command> [args]

  tables                              row counts + current snapshot of every table
  snapshots  <table>                  snapshot list with operation + record counts
  history    <table>                  which snapshot was current when (shows rollbacks)
  timetravel <table> <snapshot_id> [--where "doc_id='94'"] [--limit 20]
  doc        <doc_id>                 one document's state in EVERY pages snapshot
  diff       <snapshot_a> <snapshot_b> pages: added / updated / deleted docs between snapshots
  rollback   <table> <snapshot_id>    make an older snapshot current (recover a bad update)
  rollback-ts <table> "<yyyy-mm-dd hh:mm:ss>"
  expire     <table> [--retain-last 10]   remove old snapshots + unreferenced files
  compact    <table>                  rewrite small data files
  files      <table>                  data files of the current snapshot
  rejected   [--limit 20]             summary of rejected input records per batch
  ingest-log                          ingestion runs
  pipeline-log                        index / pagerank / export runs

<table> is a short name: pages, postings, pagerank, rejected_records, ingest_log, pipeline_log
"""
import argparse

from pyspark.sql import functions as F

from dse import config as C
from dse.sparkutil import banner, get_spark
from dse.tables import current_snapshot_id, ensure_tables, read_at


def short(t):
    return t.split(".")[-1]


def cmd_tables(spark, a):
    for name in C.ALL_TABLES:
        sid = current_snapshot_id(spark, name)
        n_snap = spark.sql("SELECT count(*) FROM %s.snapshots" % C.table(name)).first()[0]
        extra = ""
        if name == C.PAGES and sid:
            r = spark.table(C.table(name)).agg(
                F.sum(F.when(~F.col("is_deleted"), 1).otherwise(0)).alias("live"),
                F.sum(F.when(F.col("is_deleted"), 1).otherwise(0)).alias("tomb")).first()
            extra = "live=%s tombstones=%s" % (r["live"], r["tomb"])
        print("  %-18s rows=%-8d snapshots=%-3d current=%-20s %s"
              % (name, spark.table(C.table(name)).count(), n_snap, sid, extra))


def cmd_snapshots(spark, a):
    spark.sql("""
        SELECT committed_at, snapshot_id, parent_id, operation,
               summary['added-records']   AS added,
               summary['deleted-records'] AS deleted,
               summary['total-records']   AS total
        FROM {t}.snapshots ORDER BY committed_at""".format(t=C.table(a.table))).show(100, False)
    print("current snapshot:", current_snapshot_id(spark, a.table))


def cmd_history(spark, a):
    spark.sql("SELECT * FROM %s.history ORDER BY made_current_at" % C.table(a.table)).show(100, False)


def cmd_timetravel(spark, a):
    df = read_at(spark, a.table, a.snapshot_id)
    if a.where:
        df = df.filter(a.where)
    cols = [c for c in df.columns if c not in ("text", "raw_links")]
    print("rows in snapshot %s: %d" % (a.snapshot_id, df.count()))
    df.select(*cols).show(a.limit, truncate=60)


def cmd_doc(spark, a):
    snaps = spark.sql("SELECT committed_at, snapshot_id, operation FROM %s.snapshots ORDER BY committed_at"
                      % C.table(C.PAGES)).collect()
    for s in snaps:
        r = read_at(spark, C.PAGES, s["snapshot_id"]).filter(F.col("doc_id") == a.doc_id).collect()
        if not r:
            state = "(not present)"
        else:
            r = r[0]
            state = "v%s %s title=%r links=%s" % (
                r["version"], "DELETED" if r["is_deleted"] else "live", r["title"], list(r["links"] or []))
        print("  %s  %-20s %-10s %s" % (s["committed_at"], s["snapshot_id"], s["operation"], state))


def cmd_diff(spark, a):
    x = read_at(spark, C.PAGES, a.snapshot_a).select("doc_id", F.col("content_hash").alias("h_a"),
                                                     F.col("is_deleted").alias("d_a"), F.col("title").alias("title_a"))
    y = read_at(spark, C.PAGES, a.snapshot_b).select("doc_id", F.col("content_hash").alias("h_b"),
                                                     F.col("is_deleted").alias("d_b"), F.col("title").alias("title_b"))
    j = x.join(y, "doc_id", "full_outer")
    live_a = F.col("h_a").isNotNull() & ~F.coalesce(F.col("d_a"), F.lit(False))
    live_b = F.col("h_b").isNotNull() & ~F.coalesce(F.col("d_b"), F.lit(False))
    j = j.withColumn("change", F.when(~live_a & live_b, "added")
                     .when(live_a & ~live_b, "deleted")
                     .when(live_a & live_b & (F.col("h_a") != F.col("h_b")), "updated"))
    j = j.filter(F.col("change").isNotNull())
    j.groupBy("change").count().show()
    j.select("change", "doc_id", F.coalesce("title_b", "title_a").alias("title")).orderBy("change", "doc_id").show(30, False)


def cmd_rollback(spark, a):
    before = current_snapshot_id(spark, a.table)
    spark.sql("CALL %s.system.rollback_to_snapshot('%s.%s', %d)"
              % (C.CATALOG, C.NAMESPACE, a.table, int(a.snapshot_id))).show(truncate=False)
    print("current snapshot: %s -> %s" % (before, current_snapshot_id(spark, a.table)))
    if a.table == C.PAGES:
        print("NOTE: now re-run the Node 3 pipeline (index -> pagerank -> export) so search reflects it,")
        print("      or on Node 4 instantly re-activate an older serving version from the UI / API.")


def cmd_rollback_ts(spark, a):
    spark.sql("CALL %s.system.rollback_to_timestamp('%s.%s', TIMESTAMP '%s')"
              % (C.CATALOG, C.NAMESPACE, a.table, a.ts)).show(truncate=False)


def cmd_expire(spark, a):
    spark.sql("CALL %s.system.expire_snapshots(table => '%s.%s', older_than => current_timestamp(), "
              "retain_last => %d)" % (C.CATALOG, C.NAMESPACE, a.table, a.retain_last)).show(truncate=False)


def cmd_compact(spark, a):
    spark.sql("CALL %s.system.rewrite_data_files(table => '%s.%s')"
              % (C.CATALOG, C.NAMESPACE, a.table)).show(truncate=False)


def cmd_files(spark, a):
    spark.sql("SELECT file_path, record_count, file_size_in_bytes FROM %s.files" % C.table(a.table)).show(100, False)


def cmd_rejected(spark, a):
    t = spark.table(C.table(C.REJECTED))
    t.groupBy("batch_id", "phase", "reason").count().orderBy("phase", "batch_id", F.desc("count")).show(100, False)
    t.select("phase", "line_no", "doc_id", "reason", F.substring("raw_preview", 1, 70).alias("preview")) \
        .orderBy("phase", "line_no").show(a.limit, False)


def cmd_ingest_log(spark, a):
    spark.table(C.table(C.INGEST_LOG)).orderBy("started_at").drop("source_file").show(100, False)


def cmd_pipeline_log(spark, a):
    spark.table(C.table(C.PIPELINE_LOG)).orderBy("started_at").show(100, 80)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("tables")
    for n in ("snapshots", "history", "files", "compact"):
        sub.add_parser(n).add_argument("table")
    s = sub.add_parser("timetravel"); s.add_argument("table"); s.add_argument("snapshot_id", type=int)
    s.add_argument("--where"); s.add_argument("--limit", type=int, default=20)
    s = sub.add_parser("doc"); s.add_argument("doc_id")
    s = sub.add_parser("diff"); s.add_argument("snapshot_a", type=int); s.add_argument("snapshot_b", type=int)
    s = sub.add_parser("rollback"); s.add_argument("table"); s.add_argument("snapshot_id", type=int)
    s = sub.add_parser("rollback-ts"); s.add_argument("table"); s.add_argument("ts")
    s = sub.add_parser("expire"); s.add_argument("table"); s.add_argument("--retain-last", type=int, default=10)
    s = sub.add_parser("rejected"); s.add_argument("--limit", type=int, default=20)
    sub.add_parser("ingest-log")
    sub.add_parser("pipeline-log")
    a = p.parse_args()
    if hasattr(a, "table"):
        a.table = short(a.table)
        if a.table not in C.ALL_TABLES:
            p.error("unknown table %s (choose from %s)" % (a.table, ", ".join(C.ALL_TABLES)))

    spark = get_spark("dse-iceberg-admin-%s" % a.cmd)
    ensure_tables(spark)
    banner("NODE 2 :: ICEBERG ADMIN  %s" % a.cmd, namespace="%s.%s" % (C.CATALOG, C.NAMESPACE))
    {
        "tables": cmd_tables, "snapshots": cmd_snapshots, "history": cmd_history,
        "timetravel": cmd_timetravel, "doc": cmd_doc, "diff": cmd_diff, "rollback": cmd_rollback,
        "rollback-ts": cmd_rollback_ts, "expire": cmd_expire, "compact": cmd_compact,
        "files": cmd_files, "rejected": cmd_rejected, "ingest-log": cmd_ingest_log,
        "pipeline-log": cmd_pipeline_log,
    }[a.cmd](spark, a)
    spark.stop()


if __name__ == "__main__":
    main()
