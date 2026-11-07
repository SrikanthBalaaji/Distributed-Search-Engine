import json

from dse.validation import parse_record, pick_winners


def rec(obj, line_no=1, phase=0):
    return parse_record(json.dumps(obj) if not isinstance(obj, str) else obj, line_no, phase)


GOOD = {"doc_id": "1", "url": "https://x.org/a", "title": "A", "text": "hello world", "links": ["2"]}


def test_good_record():
    r = rec(GOOD)
    assert r["reject_reason"] is None and r["op"] == "UPSERT" and r["version"] == 0
    assert r["raw_links"] == ["2"]


def test_malformed_and_missing():
    assert rec('{"doc_id": "malformed-1", "url":')["reject_reason"] == "malformed_json"
    assert rec("[1,2]")["reject_reason"] == "not_an_object"
    assert rec({**GOOD, "doc_id": ""})["reject_reason"] == "missing_doc_id"
    assert rec({**GOOD, "doc_id": None})["reject_reason"] == "missing_doc_id"
    assert rec({**GOOD, "text": "  "})["reject_reason"] == "missing_field:text"
    assert rec({**GOOD, "url": "ftp://x"})["reject_reason"] == "invalid_url"
    assert rec({**GOOD, "links": "2"})["reject_reason"] == "invalid_links"
    assert rec({**GOOD, "op": "MERGE"})["reject_reason"] == "invalid_op"
    assert rec({**GOOD, "version": "abc"})["reject_reason"] == "invalid_version"


def test_links_optional_and_cleaned():
    r = rec({k: v for k, v in GOOD.items() if k != "links"})
    assert r["reject_reason"] is None and r["raw_links"] == []
    r = rec({**GOOD, "links": ["2", "2", "1", "", None, 3]})
    assert r["raw_links"] == ["2", "3"]  # dup, self-link, blank, null removed; int -> str


def test_delete_needs_only_doc_id():
    r = rec({"doc_id": "7", "op": "DELETE"}, phase=1)
    assert r["reject_reason"] is None and r["op"] == "DELETE" and r["version"] == 1


def test_explicit_version_and_dedup():
    a = rec({**GOOD, "title": "old", "version": 1}, line_no=1)
    b = rec({**GOOD, "title": "new", "version": 3}, line_no=2)
    c = rec({**GOOD, "title": "dup", "version": 3}, line_no=5)
    winners, losers = pick_winners([a, b, c])
    assert winners["1"]["title"] == "new"  # highest version, earliest line on ties
    assert len(losers) == 2
