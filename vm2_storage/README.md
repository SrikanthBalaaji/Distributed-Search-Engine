# VM 2: Storage (HDFS NameNode + DataNode, Spark Worker, Iceberg)

**Runs:** HDFS NameNode, HDFS DataNode #1, and a Spark Worker. It owns the Iceberg warehouse at `hdfs://dse-node2:9000/warehouse`.

| File | Purpose |
|---|---|
| `setup_hdfs.sh` | **One time:** format the NameNode, start NN + DN, create `/dse/raw`, `/warehouse`, `/serving`, `/dse/checkpoints`, `/spark-logs`. |
| `start_storage.sh` / `stop_storage.sh` | Daily start/stop of the NameNode, DataNode, and Spark Worker. |
| `init_tables.sh` → `jobs/init_tables.py` | Creates the Iceberg namespace `dse` and its 6 tables. Doubles as a smoke test of HDFS + Spark + Iceberg. |
| `iceberg_admin.sh` → `jobs/iceberg_admin.py` | Snapshots, history, **time travel**, **rollback**, diff, expire, compact, rejected records, and logs. |
| `hdfs_report.sh` | DataNode status, block locations, and replication factor 2 proof. |
| `reset_profile.sh` | Wipes a profile's tables and exports (e.g. the toy runs). |

## One-time setup
```bash
cp config/local.env.example config/local.env    # NODE_ID=2, paths, HDFS_DATA_ROOT, worker cores/memory
bash scripts/setup_hosts.sh
bash scripts/install_iceberg.sh
bash scripts/render_configs.sh
bash vm2_storage/setup_hdfs.sh                  # formats the NameNode (first time only)
```
Then Node 3 starts its master and DataNode. After that:
```bash
bash vm2_storage/start_storage.sh               # also starts this node's Spark worker
bash vm2_storage/init_tables.sh
bash vm2_storage/hdfs_report.sh                 # expect "Live datanodes (2)"
```

## Iceberg tables (`iceberg_catalog.dse.*`)
| Table | Contents |
|---|---|
| `pages` | One row per doc_id (live docs and delete tombstones), `raw_links` + cleaned `links`, version, phase |
| `postings` | Inverted index postings `(term, doc_id, tf)`, partitioned by term |
| `pagerank` | `(doc_id, rank, out_degree, in_degree)` |
| `rejected_records` | Every rejected or superseded input line, with the reason |
| `ingest_log` | One row per ingestion run (counts, snapshot id, status APPLIED/NOOP) |
| `pipeline_log` | One row per index / pagerank / export run |

`postings` and `pagerank` store the `pages` snapshot they were built from in the table property `dse.source-pages-snapshot`. The export uses it to guarantee consistency.

## Demo commands
```bash
bash vm2_storage/iceberg_admin.sh tables
bash vm2_storage/iceberg_admin.sh snapshots pages        # Phase 0 = append, Phase 1 = overwrite (MERGE)
bash vm2_storage/iceberg_admin.sh doc 94                 # one document across every snapshot
bash vm2_storage/iceberg_admin.sh diff <snapA> <snapB>   # added / updated / deleted docs
bash vm2_storage/iceberg_admin.sh timetravel pages <snapA> --where "doc_id='1'"
bash vm2_storage/iceberg_admin.sh rollback pages <snapA> # recover from a bad update
bash vm2_storage/iceberg_admin.sh rejected               # the dirty data we refused
```
