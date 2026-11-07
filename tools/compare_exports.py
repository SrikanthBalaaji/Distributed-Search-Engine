#!/usr/bin/env python3
"""
Compare two serving exports, e.g. the Spark export vs the reference pipeline:

    python3 tools/compare_exports.py /serving/v2 .local/ref_serving/v2

Checks: same doc ids, same links, same inverted index rows, PageRank within tolerance.
Exit code 0 if they match.
"""
import sys

import pyarrow.parquet as pq


def load(path):
    docs = pq.read_table(path + "/docs").to_pandas().set_index("doc_id").sort_index()
    idx = pq.read_table(path + "/index", columns=["term", "doc_id", "tf", "df", "length"]).to_pandas()
    idx = idx.sort_values(["term", "doc_id"]).reset_index(drop=True)
    return docs, idx


def main(a, b, tol=1e-6):
    da, ia = load(a)
    db, ib = load(b)
    ok = True

    def report(name, good, detail=""):
        nonlocal ok
        ok &= bool(good)
        print("  [%s] %-26s %s" % ("OK" if good else "DIFF", name, detail))

    print("comparing %s  vs  %s" % (a, b))
    sa, sb = set(da.index), set(db.index)
    report("doc ids", sa == sb, "%d vs %d (only A: %s, only B: %s)"
           % (len(sa), len(sb), sorted(sa - sb)[:5], sorted(sb - sa)[:5]))
    common = sorted(sa & sb)
    links_diff = [d for d in common if list(da.at[d, "links"]) != list(db.at[d, "links"])]
    report("links", not links_diff, "%d docs differ %s" % (len(links_diff), links_diff[:5]))
    len_diff = [d for d in common if int(da.at[d, "length"]) != int(db.at[d, "length"])]
    report("doc lengths", not len_diff, "%d docs differ %s" % (len(len_diff), len_diff[:5]))
    pr_err = (da.loc[common, "pagerank"] - db.loc[common, "pagerank"]).abs().max() if common else 0
    report("pagerank", pr_err < tol, "max abs diff %.2e" % pr_err)
    same_idx = len(ia) == len(ib) and (ia.values == ib.values).all()
    report("inverted index", same_idx, "%d vs %d rows" % (len(ia), len(ib)))
    top_a = list(da.sort_values("pagerank", ascending=False).index[:5])
    top_b = list(db.sort_values("pagerank", ascending=False).index[:5])
    report("top-5 pagerank", top_a == top_b, "%s vs %s" % (top_a, top_b))
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]))
