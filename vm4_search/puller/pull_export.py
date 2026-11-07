"""
NODE 4 - pull completed exports from HDFS /serving/vN/ into the local SERVING_DIR.

    python -m puller.pull_export                # pull new versions once, activate newest valid
    python -m puller.pull_export --watch 30     # keep polling every 30 s
    python -m puller.pull_export --no-activate  # download + validate only
    python -m puller.pull_export --source-dir /path/to/serving   # copy from a local dir (local mode)

Only versions with a _SUCCESS marker are considered. Each one is downloaded into
SERVING_DIR/.incoming-vN, validated (file sizes, Parquet row counts, required columns,
manifest validation results) and only then renamed to SERVING_DIR/vN. CURRENT is
switched to the newest VALID version, so a broken export never replaces a good one.
"""
import argparse
import json
import logging
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app import settings as S  # noqa: E402  (also puts common/ on sys.path)
from app.store import (VERSION_RE, list_local_versions, read_current, validate_export_dir,  # noqa: E402
                       version_key, write_current)
from dse.webhdfs import WebHDFS  # noqa: E402

log = logging.getLogger("dse.puller")


class Source:
    """Where exports come from: WebHDFS (cluster) or a local directory (local test mode)."""

    def __init__(self, source_dir=None, webhdfs_url=None, hdfs_dir=None):
        self.source_dir = source_dir
        self.hdfs_dir = hdfs_dir or S.HDFS_SERVING_DIR
        self.client = None if source_dir else WebHDFS(webhdfs_url or S.WEBHDFS_URL or None)

    def describe(self):
        return self.source_dir or "%s%s" % (self.client.base, self.hdfs_dir)

    def versions(self):
        if self.source_dir:
            names = os.listdir(self.source_dir) if os.path.isdir(self.source_dir) else []
            return [n for n in names if VERSION_RE.match(n)
                    and os.path.isfile(os.path.join(self.source_dir, n, "_SUCCESS"))]
        out = []
        try:
            entries = self.client.list(self.hdfs_dir)
        except FileNotFoundError:
            return []
        for e in entries:
            n = e["pathSuffix"]
            if e["type"] == "DIRECTORY" and VERSION_RE.match(n) and \
                    self.client.exists("%s/%s/_SUCCESS" % (self.hdfs_dir, n)):
                out.append(n)
        return out

    def read_manifest(self, version):
        if self.source_dir:
            with open(os.path.join(self.source_dir, version, "manifest.json")) as f:
                return json.load(f)
        return json.loads(self.client.read("%s/%s/manifest.json" % (self.hdfs_dir, version)))

    def fetch(self, version, rel, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if self.source_dir:
            shutil.copyfile(os.path.join(self.source_dir, version, rel), dest)
        else:
            self.client.download("%s/%s/%s" % (self.hdfs_dir, version, rel), dest)


def pull_version(src, version, serving_dir):
    final = os.path.join(serving_dir, version)
    tmp = os.path.join(serving_dir, ".incoming-" + version)
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    manifest = src.read_manifest(version)
    t0 = time.time()
    total = 0
    for item in manifest.get("files", []):
        src.fetch(version, item["path"], os.path.join(tmp, item["path"]))
        total += int(item["size"])
    with open(os.path.join(tmp, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    open(os.path.join(tmp, "_SUCCESS"), "w").close()
    ok, errors, _ = validate_export_dir(tmp)
    if not ok:
        bad = os.path.join(serving_dir, ".rejected-" + version)
        shutil.rmtree(bad, ignore_errors=True)
        os.rename(tmp, bad)
        log.error("REJECTED %s: %s (kept in %s)", version, "; ".join(errors), bad)
        return False
    os.rename(tmp, final)
    log.info("pulled %s  (%d files, %.1f KB, %.1fs)  phase=%s docs=%s",
             version, len(manifest.get("files", [])), total / 1024, time.time() - t0,
             manifest.get("phase"), manifest.get("counts", {}).get("docs"))
    return True


def pull_once(src, serving_dir=None, activate=True):
    """Download every new remote version; returns dict summary."""
    serving_dir = serving_dir or S.SERVING_DIR
    os.makedirs(serving_dir, exist_ok=True)
    local = set(list_local_versions(serving_dir))
    remote = sorted(src.versions(), key=version_key)
    new = [v for v in remote if v not in local]
    pulled, failed = [], []
    for v in new:
        try:
            (pulled if pull_version(src, v, serving_dir) else failed).append(v)
        except Exception as e:  # noqa: BLE001
            log.error("pull %s failed: %s", v, e)
            failed.append(v)
            shutil.rmtree(os.path.join(serving_dir, ".incoming-" + v), ignore_errors=True)

    activated = None
    if activate:
        # Only switch to versions pulled in THIS cycle (or anything if nothing is active yet),
        # so a manual rollback to an older version is not undone by the next poll.
        current = read_current(serving_dir)
        candidates = pulled if current else list_local_versions(serving_dir)
        for v in sorted(candidates, key=version_key, reverse=True):
            ok, errors, _ = validate_export_dir(os.path.join(serving_dir, v))
            if ok:
                write_current(v, serving_dir)
                activated = v
                log.info("CURRENT -> %s (was %s)", v, current)
                break
            log.warning("not activating %s: %s", v, errors)
    return {"source": src.describe(), "remote": remote, "pulled": pulled, "failed": failed,
            "activated": activated, "current": read_current(serving_dir)}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--watch", type=float, default=0, help="poll interval in seconds (0 = once)")
    ap.add_argument("--no-activate", action="store_true")
    ap.add_argument("--source-dir", default=S.SOURCE_DIR or None)
    ap.add_argument("--serving-dir", default=S.SERVING_DIR)
    a = ap.parse_args()
    src = Source(source_dir=a.source_dir)
    log.info("source=%s  serving_dir=%s", src.describe(), a.serving_dir)
    while True:
        try:
            r = pull_once(src, a.serving_dir, activate=not a.no_activate)
            log.info("remote=%s pulled=%s failed=%s current=%s", r["remote"], r["pulled"], r["failed"], r["current"])
        except Exception as e:  # noqa: BLE001
            log.error("pull cycle failed: %s", e)
            if not a.watch:
                return 1
        if not a.watch:
            return 0
        time.sleep(a.watch)


if __name__ == "__main__":
    sys.exit(main())
