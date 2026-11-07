# VM 3: Compute (Spark Master + Worker, HDFS DataNode, pipeline drivers)

**Runs:** Spark Master (`spark://<NODE3_IP>:7077`), Spark Worker #2, and HDFS DataNode #2. It also hosts the Spark **drivers** for indexing, PageRank, and export. The tasks themselves run on the workers of Nodes 2 and 3.

| File | Purpose |
|---|---|
| `start_compute.sh` / `stop_compute.sh` | Start/stop the master, worker, and DataNode. |
| `jobs/index_job.py` | Distributed inverted index. Tokenizes on the executors, then shuffles **by term** into the Iceberg `postings` table. Incremental updates come from an Iceberg snapshot diff. |
| `jobs/pagerank_job.py` | PageRank, d=0.85, **at least 10 iterations** (default max 20, stops early once converged). Redistributes dangling mass, uses a co-partitioned RDD join, and checkpoints to HDFS. |
| `jobs/export_job.py` | Validates, then writes Parquet to `/serving/_tmp_*`, atomically renames it to `/serving/vN`, and writes `_SUCCESS` last. |
| `run_pipeline.sh` | Runs index → pagerank → export, then notifies Node 4. |
| `fault_tolerance_demo.sh` | Slow PageRank, so a teammate can kill the Node 2 executor or worker mid-job. |

## One-time setup
```bash
cp config/local.env.example config/local.env    # NODE_ID=3
bash scripts/setup_hosts.sh
bash scripts/install_iceberg.sh
bash scripts/render_configs.sh
```

## Run
```bash
bash vm3_compute/start_compute.sh               # after the Node 2 NameNode is up
# after Node 1 finishes each ingestion phase:
bash vm3_compute/run_pipeline.sh
bash vm3_compute/run_pipeline.sh --full-index --iterations 30
```

## Export validation (nothing is published if any check fails)
`docs_not_empty`, `index_not_empty`, `doc_id_unique`, `required_fields_present`, `no_dangling_links`, `pagerank_for_every_doc`, `pagerank_sums_to_1`, `postings_reference_live_docs`, `tf_df_ranges`, plus a read-back of the written Parquet files.

## Serving layout
```
/serving/vN/docs/part-*.parquet    doc_id url title body length pagerank out_degree in_degree links version phase
/serving/vN/index/part-*.parquet   term doc_id tf df length
/serving/vN/manifest.json          counts, snapshot ids, BM25/PageRank params, validation, file list + sizes
/serving/vN/_SUCCESS
```

## Fault-tolerance demo
```bash
bash vm3_compute/fault_tolerance_demo.sh        # Node 3
bash scripts/simulate_failure.sh executor       # Node 2, after about 3 iterations
bash scripts/simulate_failure.sh restore        # Node 2, afterwards
```
