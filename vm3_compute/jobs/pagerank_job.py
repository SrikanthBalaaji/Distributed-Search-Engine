"""
NODE 3 - Distributed PageRank  (driver on Node 3, tasks on the workers of Nodes 2 + 3)

    bash scripts/submit.sh vm3_compute/jobs/pagerank_job.py [--iterations 20] [--iteration-delay 15]

    PR(p) = (1-d)/N + d * ( sum_{q->p} PR(q)/out(q)  +  danglingMass/N ),   d = 0.85

* graph = live documents + their cleaned links (no dangling links, see ingestion)
* pages without out-links ("dangling nodes") spread their rank uniformly, so sum(PR)=1
* runs AT LEAST 10 iterations (spec), then stops early once the L1 change < --tol
* link table is hash-partitioned once and cached; ranks use the same partitioner,
  so the per-iteration join is a narrow (shuffle-free) dependency
* fault tolerance: ranks are checkpointed to HDFS every --checkpoint-every iterations;
  if an executor/worker dies, Spark recomputes lost partitions from lineage (short,
  thanks to the checkpoint) and re-runs failed tasks on the surviving worker.
  --iteration-delay N sleeps between iterations to give time to kill a worker in the demo.
"""
import argparse
import sys
import time
from datetime import datetime
from operator import add

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from dse import config as C
from dse.sparkutil import banner, get_spark
from dse.tables import (current_snapshot_id, ensure_tables, log_pipeline, read_at,
                        set_properties)

RANK_SCHEMA = StructType([
    StructField("doc_id", StringType()),
    StructField("rank", DoubleType()),
    StructField("out_degree", IntegerType()),
    StructField("in_degree", IntegerType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=20, help="max iterations (>= 10 enforced)")
    ap.add_argument("--tol", type=float, default=1e-8, help="L1 convergence threshold")
    ap.add_argument("--checkpoint-every", type=int, default=5)
    ap.add_argument("--iteration-delay", type=float, default=0.0, help="seconds (fault-tolerance demo)")
    args = ap.parse_args()
    max_iter = max(args.iterations, C.PAGERANK_MIN_ITERATIONS)
    d = C.PAGERANK_DAMPING
    started = datetime.now()

    spark = get_spark("dse-pagerank")
    sc = spark.sparkContext
    ensure_tables(spark)
    sid = current_snapshot_id(spark, C.PAGES)
    if sid is None:
        raise SystemExit("pages table is empty - run ingestion first")

    ckpt = C.fs_path("%s/pagerank/%s" % (C.CHECKPOINT_DIR, started.strftime("%Y%m%d_%H%M%S")))
    sc.setCheckpointDir(ckpt)
    P = C.SHUFFLE_PARTITIONS

    live = read_at(spark, C.PAGES, sid).filter("NOT is_deleted").select("doc_id", "links")
    links = (live.rdd.map(lambda r: (r["doc_id"], list(r["links"] or [])))
             .partitionBy(P).cache())
    N = links.count()
    if N == 0:
        raise SystemExit("no live documents")
    node_set_bc = sc.broadcast(set(links.keys().collect()))
    bad_edges = links.flatMap(lambda kv: [x for x in kv[1] if x not in node_set_bc.value]).count()
    dangling_bc = sc.broadcast(set(links.filter(lambda kv: len(kv[1]) == 0).keys().collect()))
    n_edges = links.map(lambda kv: len(kv[1])).sum()

    banner("NODE 3 :: PAGERANK", pages_snapshot=sid, nodes=N, edges=n_edges,
           dangling_nodes=len(dangling_bc.value), damping=d,
           iterations="min %d / max %d" % (C.PAGERANK_MIN_ITERATIONS, max_iter),
           checkpoint_dir=ckpt)
    if bad_edges:
        print("WARNING: %d edges point outside the live doc set (they are ignored)" % bad_edges)

    base = (1.0 - d) / N
    ranks = links.mapValues(lambda _: 1.0 / N).cache()
    ranks.count()
    delta, it = None, 0

    def emit(kv):
        _src, (outs, rank) = kv
        if outs:
            share = rank / len(outs)
            for dst in outs:
                yield (dst, share)

    for it in range(1, max_iter + 1):
        t0 = time.time()
        dangling_mass = ranks.filter(lambda kv: kv[0] in dangling_bc.value).values().sum()
        contribs = links.join(ranks).flatMap(emit).reduceByKey(add, P)
        new_ranks = links.leftOuterJoin(contribs).mapValues(
            lambda v, dm=dangling_mass: base + d * ((v[1] or 0.0) + dm / N))
        new_ranks.cache()
        if it % args.checkpoint_every == 0:
            new_ranks.checkpoint()
        delta = new_ranks.join(ranks).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum()
        total = new_ranks.values().sum()
        ranks.unpersist()
        ranks = new_ranks
        print("  iter %2d  L1-delta=%.3e  sum=%.6f  dangling=%.4f  %.1fs%s"
              % (it, delta, total, dangling_mass, time.time() - t0,
                 "  [checkpointed]" if it % args.checkpoint_every == 0 else ""), flush=True)
        if args.iteration_delay:
            time.sleep(args.iteration_delay)
        if it >= C.PAGERANK_MIN_ITERATIONS and delta < args.tol:
            print("  converged after %d iterations" % it)
            break

    in_deg = links.flatMap(lambda kv: [(x, 1) for x in kv[1]]).reduceByKey(add, P)
    result = (ranks.join(links.mapValues(len)).leftOuterJoin(in_deg)
              .map(lambda kv: (kv[0], float(kv[1][0][0]), int(kv[1][0][1]), int(kv[1][1] or 0))))
    df = spark.createDataFrame(result, RANK_SCHEMA)
    df.writeTo(C.table(C.PAGERANK)).overwrite(F.lit(True))
    set_properties(spark, C.PAGERANK, {C.SOURCE_SNAPSHOT_PROP: sid, "dse.iterations": it,
                                       "dse.final-delta": "%.3e" % delta, "dse.damping": d})
    pr_sid = current_snapshot_id(spark, C.PAGERANK)

    print("\ntop 10 documents by PageRank:")
    titles = read_at(spark, C.PAGES, sid).select("doc_id", "title")
    for r in df.join(titles, "doc_id").orderBy(df["rank"].desc()).limit(10).collect():
        print("   %.6f  in=%-4d %-8s %s" % (r["rank"], r["in_degree"], r["doc_id"], r["title"]))

    log_pipeline(spark, "pagerank", "OK", sid, pr_sid,
                 {"iterations": it, "final_delta": delta, "nodes": N, "edges": n_edges}, started)
    banner("PAGERANK DONE", iterations=it, final_delta="%.3e" % delta, pagerank_snapshot=pr_sid)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
