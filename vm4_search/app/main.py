"""
NODE 4 - FastAPI search service.

    cd vm4_search && uvicorn app.main:app --host 0.0.0.0 --port 8000     (or: bash run_search.sh)

Endpoints
  GET  /                         search UI
  GET  /api/search?q=&k=10&page=1&version=     ranked results (BM25 + PageRank)
  GET  /api/compare?q=&a=v1&b=v2&k=10          same query on two versions (phase 0 vs phase 1)
  GET  /api/doc/{doc_id}?version=              document details, out-links, back-links
  GET  /api/stats?version=                     index statistics + manifest
  GET  /api/versions                           local versions, which one is active
  GET  /api/health
  POST /api/admin/pull                         pull new exports from HDFS now + activate newest valid
  POST /api/admin/activate?version=v1          switch versions instantly (serving-layer rollback)
  POST /api/admin/reload                       re-read CURRENT
Admin endpoints require header X-Admin-Token when ADMIN_TOKEN is set.
"""
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app import settings as S
from app.store import VersionStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("dse.api")
store = VersionStore()
_pull_lock = threading.Lock()


def _do_pull():
    from puller.pull_export import Source, pull_once
    with _pull_lock:
        r = pull_once(Source(source_dir=S.SOURCE_DIR or None), S.SERVING_DIR, activate=True)
    store.check_current()
    if store.active is None:
        store.initial_load()
    return r


def _auto_pull_loop():
    while True:
        try:
            r = _do_pull()
            if r["pulled"]:
                log.info("auto-pull: %s", r)
        except Exception as e:  # noqa: BLE001
            log.warning("auto-pull failed: %s", e)
        time.sleep(S.PULL_INTERVAL)


@asynccontextmanager
async def lifespan(app):
    os.makedirs(S.SERVING_DIR, exist_ok=True)
    v = store.initial_load()
    log.info("serving dir %s -> active version %s", S.SERVING_DIR, v)
    store.start_watcher()
    if S.AUTO_PULL:
        threading.Thread(target=_auto_pull_loop, name="auto-pull", daemon=True).start()
        log.info("auto-pull enabled every %ss from %s", S.PULL_INTERVAL, S.SOURCE_DIR or S.WEBHDFS_URL)
    yield
    store.stop()


app = FastAPI(title="Distributed Search Engine - Node 4", version="1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _index(version):
    try:
        return store.get(version)
    except LookupError as e:
        raise HTTPException(503 if not version else 404, str(e))
    except ValueError as e:
        raise HTTPException(409, str(e))


def _admin(token):
    if S.ADMIN_TOKEN and token != S.ADMIN_TOKEN:
        raise HTTPException(401, "bad or missing X-Admin-Token")


# ------------------------------------------------------------------ API
@app.get("/api/health")
def health():
    a = store.active
    return {"status": "ok" if a else "waiting_for_data", "version": a.version if a else None,
            "docs": a.n_docs if a else 0, "last_error": store.last_error}


@app.get("/api/search")
def search(q: str = Query(..., min_length=1, max_length=500), k: int = Query(10, ge=1, le=100),
           page: int = Query(1, ge=1), version: str = None):
    idx = _index(version)
    return idx.search(q, k=k, offset=(page - 1) * k)


@app.get("/api/compare")
def compare(q: str = Query(..., min_length=1), a: str = None, b: str = None, k: int = Query(10, ge=1, le=50)):
    versions = [v["version"] for v in store.versions_info() if v.get("complete")]
    if len(versions) < 2 and not (a and b):
        raise HTTPException(400, "need at least two versions to compare")
    a = a or versions[-2]
    b = b or versions[-1]
    ia, ib = _index(a), _index(b)
    ra, rb = ia.search(q, k=k), ib.search(q, k=k)
    all_a, all_b = ia.all_scores(q), ib.all_scores(q)
    rank_a = {x["doc_id"]: x["rank"] for x in all_a.values()}
    rank_b = {x["doc_id"]: x["rank"] for x in all_b.values()}
    for r in rb["results"]:
        prev = rank_a.get(r["doc_id"])
        r["prev_rank"] = prev
        r["movement"] = "new" if prev is None else prev - r["rank"]
        r["in_old_corpus"] = r["doc_id"] in ia.pos
    for r in ra["results"]:
        r["next_rank"] = rank_b.get(r["doc_id"])
        r["removed"] = r["doc_id"] not in ib.pos
    return {"query": q, "a": ra, "b": rb}


@app.get("/api/doc/{doc_id}")
def doc(doc_id: str, version: str = None):
    d = _index(version).doc(doc_id)
    if d is None:
        raise HTTPException(404, "doc %s not in this version" % doc_id)
    return d


@app.get("/api/stats")
def stats(version: str = None):
    return _index(version).stats()


@app.get("/api/versions")
def versions():
    return {"active": store.active.version if store.active else None,
            "serving_dir": S.SERVING_DIR, "versions": store.versions_info(),
            "last_error": store.last_error}


@app.post("/api/admin/pull")
def admin_pull(x_admin_token: str = Header(None), profile: str = None):
    _admin(x_admin_token)
    if profile and profile != S.PROFILE:
        raise HTTPException(409, "Node 4 is serving profile '%s', not '%s' (restart it with DSE_PROFILE=%s)"
                            % (S.PROFILE, profile, profile))
    try:
        r = _do_pull()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, "pull failed: %s" % e)
    r["active"] = store.active.version if store.active else None
    return r


@app.post("/api/admin/activate")
def admin_activate(version: str, x_admin_token: str = Header(None)):
    _admin(x_admin_token)
    try:
        idx = store.activate(version)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(409, "cannot activate %s: %s" % (version, e))
    return {"active": idx.version, "docs": idx.n_docs}


@app.post("/api/admin/reload")
def admin_reload(x_admin_token: str = Header(None)):
    _admin(x_admin_token)
    store.check_current()
    if store.active is None:
        store.initial_load()
    return health()


# ------------------------------------------------------------------ UI
app.mount("/static", StaticFiles(directory=S.FRONTEND_DIR), name="static")


@app.get("/", include_in_schema=False)
def ui():
    return FileResponse(os.path.join(S.FRONTEND_DIR, "index.html"))
