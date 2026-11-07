# VM 1: Ingestion gateway

**Runs:** the Spark driver for ingestion (client only). **No** HDFS or Spark daemons run on this VM.
**Needs:** Java 11, Spark 3.3.x (client), the Iceberg jar, Python 3. The Hadoop client (`hdfs` CLI) is optional; without it, uploads fall back to WebHDFS.

| File | Purpose |
|---|---|
| `precheck.py` | Local validation before upload. Uses the same rules as the Spark job (`common/dse/validation.py`). |
| `upload_to_hdfs.sh` | `hdfs dfs -put` with replication 2 (or WebHDFS), then prints the block locations. |
| `jobs/ingest_job.py` | **The Spark job**: validate → dedup (highest version wins) → merge with Iceberg → strip dangling links → one `MERGE INTO`. |
| `run_phase.sh` | Runs all three steps for one phase. |

## One-time setup
```bash
cp config/local.env.example config/local.env    # set NODE_ID=1 and your JAVA/SPARK/HADOOP paths
bash scripts/setup_hosts.sh
bash scripts/install_iceberg.sh
bash scripts/render_configs.sh
bash scripts/check_cluster.sh
```

## Run
```bash
bash vm1_ingestion/run_phase.sh 0            # Phase 0: data/full/pages.jsonl
#   ... Node 3 runs the pipeline, the team checks the results in the UI ...
bash vm1_ingestion/run_phase.sh 1            # Phase 1: data/full/changes.jsonl
bash vm1_ingestion/run_phase.sh 1 --dry-run  # validate + show the diff, commit nothing
```
After each phase, tell the Node 3 member to run `bash vm3_compute/run_pipeline.sh`.

## What the ingestion job prints (and why)
* **Validation counts.** Phase 0 on the full corpus: 1040 lines → 20 `malformed_json` rejected, 20 duplicate doc IDs superseded, 1000 documents.
* **Change detection** against the current Iceberg snapshot: inserted / updated / deleted / links_repaired.
* **Dangling links removed.** Phase 1 on the full data: 7 links in the upserted docs point to pages that don't exist, and they are stripped. Links from surviving pages into pages deleted in a phase are repaired the same way (`links_repaired`). The toy data shows this: deleting doc 2 cleans docs 1 and 3.
* **Idempotency.** Run the same phase again and you get `rows to commit 0 → NOOP`, and no new snapshot is created.

## Rules (also in `common/dse/validation.py`)
* Required: `doc_id`; `url`/`title`/`text` are required for upserts. `links` is optional (missing means `[]`).
* `op`: missing / UPSERT / INSERT / UPDATE all mean upsert; DELETE needs only `doc_id`.
* `version` is optional; if absent, the **phase number** is the version. A stored document is replaced only by a **strictly higher** version. Within one file, the earliest line wins a tie.
* A DELETE leaves a **tombstone** (`is_deleted=true`), so an older record can never bring the document back.
