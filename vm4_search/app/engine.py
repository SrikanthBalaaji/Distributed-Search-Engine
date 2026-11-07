"""In-memory search index for one serving version + the ranking formula.

BM25 follows the provided bm25.py snippet exactly (k1=1.2, b=0.75):
    idf(t)      = ln(1 + (N - df + 0.5) / (df + 0.5))
    bm25(D, Q)  = sum_t idf(t) * tf*(k1+1) / (tf + k1*(1 - b + b*len(D)/avgdl))
    avgdl       = max(mean(doc length), 1)
but vectorised with numpy over posting lists instead of pandas row loops.

Final score (spec):
    final = 0.7 * bm25 / max_bm25  +  0.3 * pagerank / max_pagerank
where max_bm25 is over the documents matching the query, and max_pagerank over the same
matching set (PR_NORM=candidates, default) or over the whole corpus (PR_NORM=global).
Only documents matching at least one query term are returned.
"""
import json
import math
import os
import time

import numpy as np
import pyarrow.parquet as pq

from app import settings as S
from dse.textproc import TOKEN_RE, normalize_token, query_terms

SNIPPET_CHARS = 260


def js_safe_manifest(m):
    """Iceberg snapshot ids are 64-bit; JavaScript numbers would round them -> send as strings."""
    for k in ("pages_snapshot_id", "postings_snapshot_id", "pagerank_snapshot_id"):
        if m.get(k) is not None:
            m[k] = str(m[k])
    return m


class SearchIndex:
    def __init__(self):
        self.version = None
        self.path = None
        self.manifest = {}
        self.n_docs = 0
        self.loaded_at = None

    # ------------------------------------------------------------------ load
    @classmethod
    def load(cls, path):
        t0 = time.time()
        self = cls()
        self.path = path
        self.version = os.path.basename(os.path.normpath(path))
        with open(os.path.join(path, "manifest.json")) as f:
            self.manifest = js_safe_manifest(json.load(f))

        docs = pq.read_table(os.path.join(path, "docs")).to_pandas()
        docs = docs.sort_values("doc_id").reset_index(drop=True)
        self.doc_ids = docs["doc_id"].astype(str).to_numpy()
        self.pos = {d: i for i, d in enumerate(self.doc_ids)}
        self.urls = docs["url"].to_numpy()
        self.titles = docs["title"].to_numpy()
        self.bodies = docs["body"].fillna("").to_numpy()
        self.lengths = docs["length"].astype(np.float64).to_numpy()
        self.pagerank = docs["pagerank"].fillna(0.0).astype(np.float64).to_numpy()
        self.in_degree = docs["in_degree"].fillna(0).astype(int).to_numpy() if "in_degree" in docs else np.zeros(len(docs), int)
        self.out_degree = docs["out_degree"].fillna(0).astype(int).to_numpy() if "out_degree" in docs else np.zeros(len(docs), int)
        self.links = [list(x) if x is not None else [] for x in docs["links"]] if "links" in docs else [[] for _ in range(len(docs))]
        self.n_docs = len(docs)
        self.avgdl = max(float(self.lengths.mean()) if self.n_docs else 1.0, 1.0)
        self.max_pr_global = float(self.pagerank.max()) if self.n_docs else 0.0

        # inverted index: term -> slice of (doc_idx, tf) arrays sorted by term
        idx = pq.read_table(os.path.join(path, "index"), columns=["term", "doc_id", "tf", "df"]).to_pandas()
        idx["doc_idx"] = idx["doc_id"].astype(str).map(self.pos)
        idx = idx.dropna(subset=["doc_idx"]).sort_values(["term", "doc_idx"]).reset_index(drop=True)
        self.post_doc = idx["doc_idx"].astype(np.int32).to_numpy()
        self.post_tf = idx["tf"].astype(np.float64).to_numpy()
        terms = idx["term"].to_numpy()
        uniq, starts, counts = np.unique(terms, return_index=True, return_counts=True)
        df = idx["df"].to_numpy()
        self.term_slices = {t: (int(s), int(s + c), int(df[s])) for t, s, c in zip(uniq, starts, counts)}
        self.n_terms = len(uniq)
        self.n_postings = len(idx)

        # back-links for the document detail view
        self.backlinks = [[] for _ in range(self.n_docs)]
        for i, outs in enumerate(self.links):
            for d in outs:
                j = self.pos.get(d)
                if j is not None:
                    self.backlinks[j].append(i)
        self.loaded_at = time.strftime("%Y-%m-%d %H:%M:%S")
        self.load_seconds = round(time.time() - t0, 3)
        return self

    # ------------------------------------------------------------------ BM25
    def bm25(self, terms, k1=None, b=None):
        """Returns (scores[N], matched_mask[N, len(terms)])."""
        k1 = S.BM25_K1 if k1 is None else k1
        b = S.BM25_B if b is None else b
        scores = np.zeros(self.n_docs, dtype=np.float64)
        matched = np.zeros((self.n_docs, max(len(terms), 1)), dtype=bool)
        for j, t in enumerate(terms):
            sl = self.term_slices.get(t)
            if sl is None:
                continue
            s, e, df = sl
            idf = math.log(1.0 + (self.n_docs - df + 0.5) / (df + 0.5))
            docs = self.post_doc[s:e]
            tf = self.post_tf[s:e]
            dl = self.lengths[docs]
            scores[docs] += idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / self.avgdl)))
            matched[docs, j] = True
        return scores, matched

    # ------------------------------------------------------------------ search
    def search(self, query, k=10, offset=0):
        t0 = time.perf_counter()
        terms = query_terms(query)
        base = {"query": query, "terms": terms, "version": self.version,
                "phase": self.manifest.get("phase"), "total": 0, "results": []}
        if not terms:
            base["took_ms"] = round((time.perf_counter() - t0) * 1000, 2)
            return base

        scores, matched = self.bm25(terms)
        cand = np.nonzero(scores > 0)[0]
        if cand.size == 0:
            base["took_ms"] = round((time.perf_counter() - t0) * 1000, 2)
            base["unknown_terms"] = [t for t in terms if t not in self.term_slices]
            return base

        bm = scores[cand]
        pr = self.pagerank[cand]
        max_bm = float(bm.max())
        max_pr = float(pr.max()) if S.PR_NORM == "candidates" else self.max_pr_global
        bm_n = bm / max_bm if max_bm > 0 else np.zeros_like(bm)
        pr_n = pr / max_pr if max_pr > 0 else np.zeros_like(pr)
        final = S.W_BM25 * bm_n + S.W_PAGERANK * pr_n

        # sort: final desc, then bm25 desc, then doc_id asc (deterministic)
        order = sorted(range(cand.size), key=lambda i: (-final[i], -bm[i], self.doc_ids[cand[i]]))
        page = order[offset: offset + k]
        results = []
        for rank, i in enumerate(page, start=offset + 1):
            d = int(cand[i])
            snippet, parts = make_snippet(self.bodies[d], set(terms))
            results.append({
                "rank": rank,
                "doc_id": self.doc_ids[d],
                "title": self.titles[d],
                "url": self.urls[d],
                "snippet": snippet,
                "snippet_parts": parts,
                "score": round(float(final[i]), 6),
                "bm25": round(float(bm[i]), 6),
                "bm25_norm": round(float(bm_n[i]), 6),
                "pagerank": float(pr[i]),
                "pagerank_norm": round(float(pr_n[i]), 6),
                "matched_terms": [terms[j] for j in np.nonzero(matched[d])[0]],
                "in_degree": int(self.in_degree[d]),
                "out_degree": int(self.out_degree[d]),
            })
        base.update({
            "total": int(cand.size),
            "max_bm25": max_bm, "max_pagerank": max_pr,
            "unknown_terms": [t for t in terms if t not in self.term_slices],
            "results": results,
            "took_ms": round((time.perf_counter() - t0) * 1000, 2),
        })
        return base

    def all_scores(self, query):
        """doc_id -> final score for every matching doc (used by the compare view)."""
        r = self.search(query, k=self.n_docs)
        return {x["doc_id"]: x for x in r["results"]}

    # ------------------------------------------------------------------ docs / stats
    def doc(self, doc_id):
        i = self.pos.get(doc_id)
        if i is None:
            return None
        return {
            "doc_id": doc_id, "title": self.titles[i], "url": self.urls[i],
            "pagerank": float(self.pagerank[i]), "length": int(self.lengths[i]),
            "in_degree": int(self.in_degree[i]), "out_degree": int(self.out_degree[i]),
            "links": [{"doc_id": self.doc_ids[self.pos[d]], "title": self.titles[self.pos[d]]}
                      for d in self.links[i] if d in self.pos],
            "backlinks": [{"doc_id": self.doc_ids[j], "title": self.titles[j]} for j in self.backlinks[i]],
            "body": self.bodies[i][:2000],
            "version": self.version,
        }

    def stats(self):
        top = np.argsort(-self.pagerank)[:10]
        return {
            "version": self.version, "path": self.path, "loaded_at": self.loaded_at,
            "load_seconds": self.load_seconds,
            "docs": self.n_docs, "terms": self.n_terms, "postings": self.n_postings,
            "avgdl": round(self.avgdl, 3),
            "ranking": {"k1": S.BM25_K1, "b": S.BM25_B, "w_bm25": S.W_BM25,
                        "w_pagerank": S.W_PAGERANK, "pr_norm": S.PR_NORM},
            "top_pagerank": [{"doc_id": self.doc_ids[i], "title": self.titles[i],
                              "pagerank": float(self.pagerank[i]), "in_degree": int(self.in_degree[i])}
                             for i in top],
            "manifest": self.manifest,
        }


def make_snippet(body, term_set, width=SNIPPET_CHARS):
    """Query-dependent snippet -> (plain_text, [[segment, is_query_term], ...])."""
    if not body:
        return "", []
    first = None
    for m in TOKEN_RE.finditer(body):
        if normalize_token(m.group(0)) in term_set:
            first = m.start()
            break
    if first is None:
        start = 0
    else:
        start = max(0, first - width // 3)
        if start > 0:
            sp = body.rfind(" ", 0, start)
            start = sp + 1 if sp != -1 and start - sp < 30 else start
    end = min(len(body), start + width)
    if end < len(body):
        sp = body.rfind(" ", start, end)
        end = sp if sp > start + width // 2 else end
    text = body[start:end].replace("\n", " ")
    prefix = "… " if start > 0 else ""
    suffix = " …" if end < len(body) else ""
    # split into [text, is_hit] segments (safer for the browser than char offsets)
    parts, last = [], 0
    for m in TOKEN_RE.finditer(text):
        if normalize_token(m.group(0)) in term_set:
            if m.start() > last:
                parts.append([text[last:m.start()], False])
            parts.append([m.group(0), True])
            last = m.end()
    if last < len(text):
        parts.append([text[last:], False])
    if prefix:
        parts.insert(0, [prefix, False])
    if suffix:
        parts.append([suffix, False])
    return prefix + text + suffix, parts
