"""End-to-end (pure python): reference pipeline on the toy data -> serving export ->
SearchIndex. Also checks our vectorised BM25 against the provided bm25.py snippet."""
import os

import pyarrow.parquet as pq
import pytest

import reference_pipeline as ref
from app import settings as S
from app.bm25_reference import compute_bm25
from app.engine import SearchIndex
from app.store import validate_export_dir
from dse.textproc import query_terms

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOY0 = os.path.join(ROOT, "data", "toy", "phase0_toy_pages.jsonl")
TOY1 = os.path.join(ROOT, "data", "toy", "phase1_toy_changes.jsonl")


@pytest.fixture(scope="module")
def exports(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("serving"))
    state = {}
    s0 = ref.apply_batch(state, TOY0, 0)
    v1, _ = ref.export(state, out, 1, 0)
    s1 = ref.apply_batch(state, TOY1, 1)
    v2, _ = ref.export(state, out, 2, 1)
    return {"v1": v1, "v2": v2, "s0": s0, "s1": s1, "state": state}


def test_toy_phase0_rules(exports):
    s0 = exports["s0"]
    assert s0["rejected"] == 2          # empty + null rows
    assert s0["duplicates"] == 1        # second doc_id 2
    idx = SearchIndex.load(exports["v1"])
    assert sorted(idx.doc_ids) == ["1", "2", "3", "4"]
    assert idx.titles[idx.pos["2"]] == "Soil"  # first occurrence wins at equal version


def test_toy_phase1_delete_upsert_dangling(exports):
    idx = SearchIndex.load(exports["v2"])
    assert sorted(idx.doc_ids) == ["1", "3", "4", "5"]       # 2 deleted, 5 added
    assert idx.links[idx.pos["3"]] == ["1"]                  # link to deleted doc 2 stripped
    assert idx.links[idx.pos["1"]] == []                     # 1 -> 2 dangling after delete
    assert "forest" in idx.term_slices
    assert exports["state"]["2"]["is_deleted"] is True      # tombstone kept


def test_exports_validate(exports):
    for v in ("v1", "v2"):
        ok, errors, _ = validate_export_dir(exports[v])
        assert ok, errors


def test_bm25_matches_provided_snippet(exports):
    idx = SearchIndex.load(exports["v2"])
    docs_df = pq.read_table(exports["v2"] + "/docs").to_pandas()[["doc_id", "length"]]
    index_df = pq.read_table(exports["v2"] + "/index").to_pandas()
    for q in ("plant", "seed tree", "forest plants soil"):
        terms = query_terms(q)
        expected = compute_bm25(terms, docs_df, index_df, k1=1.2, b=0.75)
        got, _ = idx.bm25(terms)
        for doc_id, score in expected.items():
            assert got[idx.pos[doc_id]] == pytest.approx(score, rel=1e-9, abs=1e-12)


def test_final_score_formula(exports):
    idx = SearchIndex.load(exports["v2"])
    r = idx.search("plant", k=10)
    assert r["total"] == 3
    bm = {x["doc_id"]: x["bm25"] for x in r["results"]}
    pr = {x["doc_id"]: x["pagerank"] for x in r["results"]}
    for x in r["results"]:
        expect = S.W_BM25 * x["bm25"] / max(bm.values()) + S.W_PAGERANK * x["pagerank"] / max(pr.values())
        assert x["score"] == pytest.approx(expect, abs=1e-5)
    scores = [x["score"] for x in r["results"]]
    assert scores == sorted(scores, reverse=True)
    assert r["results"][0]["doc_id"] == "1"


def test_pagerank_properties(exports):
    idx = SearchIndex.load(exports["v1"])
    assert idx.pagerank.sum() == pytest.approx(1.0, abs=1e-9)
    assert idx.manifest["pagerank"]["iterations"] >= 10
    assert idx.doc_ids[idx.pagerank.argmax()] == "1"   # most linked-to page


def test_no_match_and_stopwords(exports):
    idx = SearchIndex.load(exports["v2"])
    assert idx.search("zzzz")["total"] == 0
    assert idx.search("the and of")["terms"] == []
