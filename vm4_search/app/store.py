"""Versioned serving directory management on Node 4.

    SERVING_DIR/
        v1/  manifest.json  _SUCCESS  docs/*.parquet  index/*.parquet
        v2/  ...
        CURRENT            <- text file containing the active version, e.g. "v2"

* validate_export_dir(): a version is usable only if _SUCCESS + manifest exist, every file
  listed in the manifest is present with the right size, and Parquet row counts match.
* VersionStore keeps the active SearchIndex plus a small LRU of other versions (for the
  compare view), watches CURRENT and hot-swaps ONLY after the new version loaded fine -
  otherwise the previous version keeps serving (spec: "keep Phase 0 available if ... fails").
"""
import json
import logging
import os
import re
import threading
from collections import OrderedDict

import pyarrow.parquet as pq

from app import settings as S
from app.engine import SearchIndex

log = logging.getLogger("dse.store")
VERSION_RE = re.compile(r"^v(\d+)$")
REQUIRED_DOC_COLS = {"doc_id", "url", "title", "body", "length", "pagerank"}
REQUIRED_INDEX_COLS = {"term", "doc_id", "tf", "df", "length"}


def version_key(v):
    m = VERSION_RE.match(v)
    return int(m.group(1)) if m else -1


def _parquet_rows(directory):
    files = sorted(f for f in os.listdir(directory) if f.endswith(".parquet"))
    if not files:
        raise ValueError("no parquet files in %s" % directory)
    rows, cols = 0, None
    for f in files:
        meta = pq.ParquetFile(os.path.join(directory, f))
        rows += meta.metadata.num_rows
        cols = set(meta.schema_arrow.names)
    return rows, cols


def validate_export_dir(path):
    """Returns (ok: bool, errors: list[str], manifest: dict|None)."""
    errors = []
    if not os.path.isfile(os.path.join(path, "_SUCCESS")):
        errors.append("missing _SUCCESS marker (export not finished)")
    mpath = os.path.join(path, "manifest.json")
    if not os.path.isfile(mpath):
        return False, errors + ["missing manifest.json"], None
    try:
        with open(mpath) as f:
            manifest = json.load(f)
    except ValueError as e:
        return False, errors + ["manifest.json unreadable: %s" % e], None

    for item in manifest.get("files", []):
        fp = os.path.join(path, item["path"])
        if not os.path.isfile(fp):
            errors.append("missing file %s" % item["path"])
        elif os.path.getsize(fp) != int(item["size"]):
            errors.append("size mismatch %s (%d != %s)" % (item["path"], os.path.getsize(fp), item["size"]))
    if any(not v.get("passed", False) for v in manifest.get("validation", [])):
        errors.append("export reported failed validation checks")
    if errors:
        return False, errors, manifest

    counts = manifest.get("counts", {})
    for sub, key, required in (("docs", "docs", REQUIRED_DOC_COLS), ("index", "index_rows", REQUIRED_INDEX_COLS)):
        try:
            rows, cols = _parquet_rows(os.path.join(path, sub))
        except Exception as e:  # noqa: BLE001
            errors.append("%s unreadable: %s" % (sub, e))
            continue
        if rows != counts.get(key):
            errors.append("%s has %d rows, manifest says %s" % (sub, rows, counts.get(key)))
        missing = required - cols
        if missing:
            errors.append("%s missing columns %s" % (sub, sorted(missing)))
    return not errors, errors, manifest


def read_current(serving_dir=None):
    p = os.path.join(serving_dir or S.SERVING_DIR, S.CURRENT_FILE)
    try:
        with open(p) as f:
            v = f.read().strip()
        return v or None
    except OSError:
        return None


def write_current(version, serving_dir=None):
    """Atomic pointer switch (write temp file + os.replace)."""
    d = serving_dir or S.SERVING_DIR
    tmp = os.path.join(d, ".CURRENT.tmp")
    with open(tmp, "w") as f:
        f.write(version + "\n")
    os.replace(tmp, os.path.join(d, S.CURRENT_FILE))


def list_local_versions(serving_dir=None):
    d = serving_dir or S.SERVING_DIR
    if not os.path.isdir(d):
        return []
    return sorted((n for n in os.listdir(d) if VERSION_RE.match(n) and os.path.isdir(os.path.join(d, n))),
                  key=version_key)


class VersionStore:
    def __init__(self, serving_dir=None):
        self.serving_dir = serving_dir or S.SERVING_DIR
        self._lock = threading.RLock()
        self._cache = OrderedDict()           # version -> SearchIndex
        self.active = None                    # SearchIndex currently serving
        self.last_error = None
        self._stop = threading.Event()
        self._seen_current = None

    # ---------------- loading ----------------
    def _load(self, version):
        with self._lock:
            if version in self._cache:
                self._cache.move_to_end(version)
                return self._cache[version]
        path = os.path.join(self.serving_dir, version)
        ok, errors, _ = validate_export_dir(path)
        if not ok:
            raise ValueError("%s failed validation: %s" % (version, "; ".join(errors)))
        idx = SearchIndex.load(path)
        with self._lock:
            self._cache[version] = idx
            keep = {version, self.active.version if self.active else None}
            while len(self._cache) > S.MAX_CACHED_VERSIONS:
                victim = next((v for v in self._cache if v not in keep), None)  # oldest first
                if victim is None:
                    break
                del self._cache[victim]
        return idx

    def get(self, version=None):
        if not version or (self.active and version == self.active.version):
            if self.active is None:
                raise LookupError("no search version loaded yet - waiting for an export from Node 3")
            return self.active
        if not VERSION_RE.match(version):
            raise LookupError("bad version name %r" % version)
        return self._load(version)

    def activate(self, version, persist=True):
        """Load + validate a version, then atomically make it the active one."""
        idx = self._load(version)
        with self._lock:
            self.active = idx
            self.last_error = None
        if persist:
            write_current(version, self.serving_dir)
        self._seen_current = version
        log.info("active search version -> %s (%d docs)", version, idx.n_docs)
        return idx

    def initial_load(self):
        """Use CURRENT if valid, otherwise the newest valid local version."""
        cur = read_current(self.serving_dir)
        candidates = ([cur] if cur else []) + list(reversed(list_local_versions(self.serving_dir)))
        for v in candidates:
            try:
                self.activate(v, persist=(v != cur))
                return v
            except Exception as e:  # noqa: BLE001
                log.warning("cannot load %s: %s", v, e)
                self.last_error = "%s: %s" % (v, e)
        return None

    # ---------------- watching CURRENT ----------------
    def check_current(self):
        cur = read_current(self.serving_dir)
        if cur and cur != self._seen_current:
            self._seen_current = cur
            if self.active is not None and cur == self.active.version:
                return
            try:
                self.activate(cur, persist=False)
            except Exception as e:  # noqa: BLE001
                self.last_error = "switch to %s refused: %s" % (cur, e)
                log.error(self.last_error + " - still serving %s",
                          self.active.version if self.active else None)

    def start_watcher(self):
        def loop():
            while not self._stop.wait(S.WATCH_INTERVAL):
                try:
                    self.check_current()
                except Exception:  # noqa: BLE001
                    log.exception("watcher error")
        threading.Thread(target=loop, name="current-watcher", daemon=True).start()

    def stop(self):
        self._stop.set()

    # ---------------- info ----------------
    def versions_info(self):
        out = []
        active = self.active.version if self.active else None
        for v in list_local_versions(self.serving_dir):
            path = os.path.join(self.serving_dir, v)
            info = {"version": v, "active": v == active, "loaded": v in self._cache}
            try:
                with open(os.path.join(path, "manifest.json")) as f:
                    m = json.load(f)
                info.update({"phase": m.get("phase"), "created_at": m.get("created_at"),
                             "docs": m.get("counts", {}).get("docs"),
                             "pages_snapshot_id": (str(m["pages_snapshot_id"])
                                                   if m.get("pages_snapshot_id") is not None else None),
                             "producer": m.get("producer")})
            except Exception:  # noqa: BLE001
                info["error"] = "no manifest"
            info["complete"] = os.path.isfile(os.path.join(path, "_SUCCESS"))
            out.append(info)
        return out
