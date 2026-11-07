"""Record validation rules for pages.jsonl (Phase 0) and changes.jsonl (Phase 1+).

Used by:
  * vm1_ingestion/precheck.py      - local validation on the gateway before upload
  * vm1_ingestion/jobs/ingest_job  - inside Spark executors (authoritative)
  * tools/reference_pipeline.py    - pure-python reference implementation

Rules
-----
* each line must be a JSON object                      -> else malformed_json / not_an_object
* op: missing => UPSERT; INSERT/UPDATE/UPSERT => UPSERT; DELETE; anything else => invalid_op
* doc_id: required, non-empty string (ints are accepted and converted) -> missing_doc_id
* UPSERT needs non-empty url, title, text (strings)    -> missing_field:<name>
* url must start with http:// or https://               -> invalid_url
* links: optional (missing/null => []); must be a list  -> invalid_links
         elements converted to str, blanks/self-links/duplicates removed
* version: optional integer; if absent the PHASE number is used as the version.
  "Highest version wins": a record only replaces the stored document if its version is
  strictly higher. Within one file, ties are broken by the earliest line.
"""
import json

VALID_OPS = {"UPSERT": "UPSERT", "INSERT": "UPSERT", "UPDATE": "UPSERT", "DELETE": "DELETE"}
REQUIRED_UPSERT_FIELDS = ("url", "title", "text")
PREVIEW_CHARS = 300

# Field order of the tuple produced by to_row() - matches the Spark schema in ingest_job.py
ROW_FIELDS = (
    "line_no", "doc_id", "url", "title", "text", "raw_links",
    "op", "version", "reject_reason", "raw_preview",
)


def _as_id(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, str):
        v = value.strip()
        return v or None
    return None


def _as_text(value):
    if isinstance(value, str) and value.strip():
        return value
    return None


def _as_version(value):
    """Returns (ok, version_or_None)."""
    if value is None:
        return True, None
    if isinstance(value, bool):
        return False, None
    if isinstance(value, int):
        return True, value
    if isinstance(value, float) and value.is_integer():
        return True, int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return True, int(value.strip())
    return False, None


def _empty(line_no, reason, line, doc_id=None):
    return {
        "line_no": line_no, "doc_id": doc_id, "url": None, "title": None, "text": None,
        "raw_links": None, "op": None, "version": None,
        "reject_reason": reason, "raw_preview": (line or "")[:PREVIEW_CHARS],
    }


def parse_record(line, line_no, default_version):
    """Validate one JSONL line. Always returns a dict with ROW_FIELDS keys;
    reject_reason is None for accepted records."""
    if line is None or not line.strip():
        return _empty(line_no, "blank_line", line)
    try:
        obj = json.loads(line)
    except (ValueError, TypeError):
        return _empty(line_no, "malformed_json", line)
    if not isinstance(obj, dict):
        return _empty(line_no, "not_an_object", line)

    doc_id = _as_id(obj.get("doc_id"))
    raw_op = obj.get("op", "UPSERT")
    op = VALID_OPS.get(str(raw_op).strip().upper()) if raw_op is not None else "UPSERT"
    if op is None:
        return _empty(line_no, "invalid_op", line, doc_id)
    if doc_id is None:
        return _empty(line_no, "missing_doc_id", line)

    ok, version = _as_version(obj.get("version"))
    if not ok:
        return _empty(line_no, "invalid_version", line, doc_id)
    if version is None:
        version = int(default_version)

    url = _as_text(obj.get("url"))
    title = _as_text(obj.get("title"))
    text = _as_text(obj.get("text"))

    if op == "UPSERT":
        for name, val in (("url", url), ("title", title), ("text", text)):
            if val is None:
                return _empty(line_no, "missing_field:" + name, line, doc_id)
        url = url.strip()
        title = title.strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            return _empty(line_no, "invalid_url", line, doc_id)

    links = obj.get("links")
    if links is None:
        links = []
    if not isinstance(links, list):
        return _empty(line_no, "invalid_links", line, doc_id)
    clean, seen = [], set()
    for item in links:
        lid = _as_id(item)
        if lid is None or lid == doc_id or lid in seen:
            continue
        seen.add(lid)
        clean.append(lid)

    return {
        "line_no": line_no, "doc_id": doc_id,
        "url": url, "title": title, "text": text if op == "UPSERT" else None,
        "raw_links": clean if op == "UPSERT" else [],
        "op": op, "version": version, "reject_reason": None, "raw_preview": None,
    }


def to_row(rec):
    return tuple(rec[f] for f in ROW_FIELDS)


def pick_winners(records):
    """Pure-python dedup: highest version wins, ties -> earliest line.
    Returns (winners_by_id, superseded_records)."""
    best, losers = {}, []
    for r in records:
        cur = best.get(r["doc_id"])
        if cur is None:
            best[r["doc_id"]] = r
        elif (r["version"], -r["line_no"]) > (cur["version"], -cur["line_no"]):
            losers.append(cur)
            best[r["doc_id"]] = r
        else:
            losers.append(r)
    return best, losers
