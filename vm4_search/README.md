# VM 4: Search service + UI

**Runs:** FastAPI (uvicorn) on port 8000 and the web UI. It has **no** Spark or HDFS daemons and never queries Iceberg. It pulls `/serving/vN` from HDFS over **WebHDFS** (plain HTTP to the NameNode on port 9870), so Hadoop doesn't need to be installed here.

| Path | Purpose |
|---|---|
| `app/engine.py` | In-memory index for one version. Vectorized **BM25** (k1=1.2, b=0.75, identical to `app/bm25_reference.py`) plus PageRank. Builds snippets. |
| `app/store.py` | Local version store: validates exports, keeps the `CURRENT` pointer, hot-swaps only after a successful load, LRU cache for comparisons. |
| `app/main.py` | REST API and static UI. |
| `puller/pull_export.py` | Downloads new `vN` → `.incoming-vN`, validates, renames to `vN`, then switches `CURRENT`. |
| `frontend/` | Search, Compare phases, and Index & versions tabs (vanilla HTML/CSS/JS, no CDN). |

## Setup and run
```bash
cp config/local.env.example config/local.env    # NODE_ID=4 (Java/Spark paths not needed)
bash scripts/setup_hosts.sh                     # WebHDFS redirects to dse-node2/dse-node3 by name
bash vm4_search/setup_node4.sh                  # venv + requirements + /serving
bash vm4_search/run_search.sh                   # auto-pulls new exports every 30 s
```
Open `http://<NODE4_IP>:8000`. Interactive API docs are at `/docs`.

## Ranking
```
final = 0.7 * BM25 / max_BM25  +  0.3 * PageRank / max_PageRank
```
`max_*` is taken over the documents that match the query (`PR_NORM=candidates`). Set `PR_NORM=global` to normalise PageRank by the corpus maximum instead. Only documents containing at least one query term are returned.

## Version switching rules
* Only exports with `_SUCCESS` and a matching manifest are pulled. File sizes, Parquet row counts, required columns, and the export's own validation results must all match.
* A version that fails validation goes to `.rejected-vN` and is never served.
* `CURRENT` switches only to newly pulled valid versions. If loading fails, the old version keeps serving.
* **Rollback:** Index & versions tab → *activate* (or `POST /api/admin/activate?version=v1`).

## API
| Method | Path | |
|---|---|---|
| GET | `/api/search?q=&k=10&page=1&version=` | ranked results with BM25, PageRank, score parts, snippet |
| GET | `/api/compare?q=&a=v1&b=v2` | rank movement between two versions (Phase 0 vs Phase 1) |
| GET | `/api/doc/{doc_id}` | details, out-links, back-links |
| GET | `/api/stats`, `/api/versions`, `/api/health` | |
| POST | `/api/admin/pull`, `/api/admin/activate?version=`, `/api/admin/reload` | header `X-Admin-Token` if `ADMIN_TOKEN` is set |
