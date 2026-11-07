#!/usr/bin/env python3
"""
Pure-Python REFERENCE implementation of the whole pipeline (no Spark/HDFS/Iceberg).

Why it exists
  * Node 4 can build + test the search UI on a laptop before the cluster is up
  * cross-check: results of the distributed Spark jobs must match this (tools/compare_exports.py)

It applies the same rules as the Spark jobs (common/dse/*): validation, dedup by highest
version, tombstones, dangling-link removal, tokenizer, PageRank with dangling-mass
redistribution, and writes exports in the exact /serving/vN layout.

    python3 tools/reference_pipeline.py --out .local/ref_serving \
        data/full/pages.jsonl data/full/changes.jsonl
    # -> .local/ref_serving/v1 (after phase 0), v2 (after phase 1)
"""
import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "common"))
from dse import config as C  # noqa: E402
from dse.textproc import clean_wikitext, document_terms  # noqa: E402
from dse.validation import parse_record, pick_winners  # noqa: E402


def content_hash(r):
    s = "\u0001".join([r["url"] or "", r["title"] or "", r["text"] or "", "\u0002".join(r["raw_links"])])
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def apply_batch(state, path, phase):
    """state: doc_id -> row dict (live + tombstones). Returns stats dict."""
    accepted, rejected = [], []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            rec = parse_record(line.rstrip("\r\n"), i, phase)
            (rejected if rec["reject_reason"] else accepted).append(rec)
    winners, losers = pick_winners(accepted)
    changed = 0
    for doc_id, b in winners.items():
        cur = state.get(doc_id)
        if cur is not None and not b["version"] > cur["version"]:
            continue
        if b["op"] == "DELETE":
            state[doc_id] = {"doc_id": doc_id, "url": b["url"] or (cur or {}).get("url"),
                             "title": b["title"] or (cur or {}).get("title"), "text": None,
                             "raw_links": [], "version": b["version"], "is_deleted": True,
                             "content_hash": "__deleted__", "phase": phase}
        else:
            state[doc_id] = {"doc_id": doc_id, "url": b["url"], "title": b["title"], "text": b["text"],
                             "raw_links": b["raw_links"], "version": b["version"], "is_deleted": False,
                             "content_hash": content_hash(b), "phase": phase}
        changed += 1
    live = {d for d, r in state.items() if not r["is_deleted"]}
    dangling = 0
    for r in state.values():
        if r["is_deleted"]:
            r["links"] = []
            continue
        r["links"] = [x for x in r["raw_links"] if x in live and x != r["doc_id"]]
        dangling += len(r["raw_links"]) - len(r["links"])
    return {"accepted": len(accepted), "rejected": len(rejected), "duplicates": len(losers),
            "changed": changed, "live": len(live), "dangling_removed": dangling}


def pagerank(live_docs, d=C.PAGERANK_DAMPING, max_iter=20, tol=1e-8):
    ids = sorted(live_docs)
    n = len(ids)
    links = {i: live_docs[i]["links"] for i in ids}
    ranks = {i: 1.0 / n for i in ids}
    it, delta = 0, None
    for it in range(1, max(max_iter, C.PAGERANK_MIN_ITERATIONS) + 1):
        dm = sum(ranks[i] for i in ids if not links[i])
        contrib = dict.fromkeys(ids, 0.0)
        for i in ids:
            if links[i]:
                share = ranks[i] / len(links[i])
                for j in links[i]:
                    contrib[j] += share
        new = {i: (1 - d) / n + d * (contrib[i] + dm / n) for i in ids}
        delta = sum(abs(new[i] - ranks[i]) for i in ids)
        ranks = new
        if it >= C.PAGERANK_MIN_ITERATIONS and delta < tol:
            break
    return ranks, it, delta


def export(state, out_root, version_no, phase):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    live = {d: r for d, r in state.items() if not r["is_deleted"]}
    ranks, iters, delta = pagerank(live)
    in_deg = Counter(x for r in live.values() for x in r["links"])
    postings = []
    lengths = {}
    for d, r in live.items():
        tf = Counter(document_terms(r["title"], r["text"]))
        lengths[d] = sum(tf.values())
        postings.extend((t, d, c) for t, c in tf.items())
    df = Counter(t for t, _, _ in postings)

    docs = pd.DataFrame([{
        "doc_id": d, "url": r["url"], "title": r["title"],
        "body": clean_wikitext(r["text"])[: C.BODY_MAX_CHARS], "length": lengths.get(d, 0),
        "pagerank": ranks[d], "out_degree": len(r["links"]), "in_degree": in_deg.get(d, 0),
        "links": r["links"], "version": r["version"], "phase": r["phase"],
    } for d, r in sorted(live.items())])
    index = pd.DataFrame([{"term": t, "doc_id": d, "tf": c, "df": df[t], "length": lengths[d]}
                          for t, d, c in sorted(postings)])

    vdir = os.path.join(out_root, "v%d" % version_no)
    os.makedirs(os.path.join(vdir, "docs"), exist_ok=True)
    os.makedirs(os.path.join(vdir, "index"), exist_ok=True)
    docs_schema = pa.schema([("doc_id", pa.string()), ("url", pa.string()), ("title", pa.string()),
                             ("body", pa.string()), ("length", pa.int32()), ("pagerank", pa.float64()),
                             ("out_degree", pa.int32()), ("in_degree", pa.int32()),
                             ("links", pa.list_(pa.string())), ("version", pa.int64()), ("phase", pa.int32())])
    index_schema = pa.schema([("term", pa.string()), ("doc_id", pa.string()), ("tf", pa.int32()),
                              ("df", pa.int32()), ("length", pa.int32())])
    pq.write_table(pa.Table.from_pandas(docs, schema=docs_schema, preserve_index=False),
                   os.path.join(vdir, "docs", "part-00000.parquet"))
    pq.write_table(pa.Table.from_pandas(index, schema=index_schema, preserve_index=False),
                   os.path.join(vdir, "index", "part-00000.parquet"))
    files = []
    for sub in ("docs", "index"):
        p = os.path.join(sub, "part-00000.parquet")
        files.append({"path": p.replace(os.sep, "/"), "size": os.path.getsize(os.path.join(vdir, p))})
    manifest = {
        "version": "v%d" % version_no, "version_number": version_no,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "producer": "reference-pipeline", "phase": phase,
        "pages_snapshot_id": None, "postings_snapshot_id": None, "pagerank_snapshot_id": None,
        "counts": {"docs": len(docs), "index_rows": len(index), "terms": len(df),
                   "edges": int(docs["out_degree"].sum())},
        "stats": {"avgdl": float(docs["length"].mean()), "total_tokens": int(docs["length"].sum())},
        "bm25": {"k1": C.BM25_K1, "b": C.BM25_B},
        "ranking": {"w_bm25": C.W_BM25, "w_pagerank": C.W_PAGERANK},
        "pagerank": {"damping": C.PAGERANK_DAMPING, "iterations": iters, "final_delta": "%.3e" % delta,
                     "min_iterations": C.PAGERANK_MIN_ITERATIONS},
        "validation": [{"check": "reference_pipeline", "passed": True, "detail": "pure python"}],
        "files": files,
    }
    with open(os.path.join(vdir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    open(os.path.join(vdir, "_SUCCESS"), "w").close()
    return vdir, manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", help="phase files in order: phase0 [phase1 ...]")
    ap.add_argument("--out", default=os.path.join(ROOT, ".local", "ref_serving"))
    a = ap.parse_args()
    state = {}
    os.makedirs(a.out, exist_ok=True)
    for phase, path in enumerate(a.inputs):
        st = apply_batch(state, path, phase)
        vdir, m = export(state, a.out, phase + 1, phase)
        print("phase %d %-40s %s -> %s  (docs=%d, postings=%d, pr_iters=%s)"
              % (phase, os.path.basename(path), st, vdir, m["counts"]["docs"],
                 m["counts"]["index_rows"], m["pagerank"]["iterations"]))
    with open(os.path.join(a.out, "CURRENT"), "w") as f:
        f.write("v%d\n" % len(a.inputs))


if __name__ == "__main__":
    main()
