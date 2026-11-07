# Distributed Search Engine (BD Mini Project, UE24CS343AB2)

A small Google, split across **4 VMs connected by ZeroTier**. Documents are ingested with data-quality rules into **Apache Iceberg** tables on a 2-node **HDFS** cluster. **Spark** builds a distributed inverted index and PageRank. A validated, versioned **Parquet** export is served by **FastAPI** with BM25 + PageRank ranking and a web UI.

Every VM clones this same repo. Each team member works in their own `vmN_*` folder, and all of them share `common/`, `config/` and `scripts/`.

> **New to the project? Start with [EXECUTION_GUIDE.md](EXECUTION_GUIDE.md)**: a simple step-by-step of who runs what, and when.

```
                       ZeroTier private network (10.147.17.0/24 - use your own IPs)
 ┌──────────────────┐   ┌──────────────────────────┐   ┌──────────────────────────┐   ┌──────────────────┐
 │ NODE 1 Ingestion │   │ NODE 2 Storage           │   │ NODE 3 Compute           │   │ NODE 4 Search    │
 │                  │   │                          │   │                          │   │                  │
 │ precheck.py      │   │ HDFS NameNode  :9000     │   │ Spark Master   :7077     │   │ FastAPI  :8000   │
 │ upload (RF=2) ───┼──►│ HDFS DataNode #1         │◄─►│ HDFS DataNode #2         │   │ Web UI           │
 │ ingest driver ───┼──►│ Spark Worker #1          │◄─►│ Spark Worker #2          │   │ /serving/vN      │
 │ (client only)    │   │ Iceberg warehouse        │   │ drivers: index, PageRank,│   │ (in memory)      │
 │                  │   │  /warehouse/dse/*        │   │          export          │   │                  │
 └──────────────────┘   └───────────┬──────────────┘   └───────────┬──────────────┘   └────────▲─────────┘
                                    │  /serving/vN/ (Parquet + manifest + _SUCCESS)│           │
                                    └───────────────── WebHDFS :9870 ──────────────┴───────────┘
```

| Node | Owner | Daemons | This member runs |
|---|---|---|---|
| **1** Ingestion | member 1 | none (Spark/HDFS **client** only) | `vm1_ingestion/run_phase.sh 0\|1` |
| **2** Storage | member 2 | HDFS NameNode + DataNode, Spark Worker | `vm2_storage/setup_hdfs.sh`, `start_storage.sh`, `iceberg_admin.sh` |
| **3** Compute | member 3 | Spark Master + Worker, HDFS DataNode | `vm3_compute/start_compute.sh`, `run_pipeline.sh` |
| **4** Search | member 4 | FastAPI only | `vm4_search/setup_node4.sh`, `run_search.sh` |

---

## 1. Repository layout

```
distributed-search-engine/
├── README.md                     <- you are here
├── config/
│   ├── cluster.env               team-wide: ZeroTier IPs, ports, HDFS paths (COMMIT after editing)
│   ├── local.env.example         per-VM: NODE_ID, JAVA/HADOOP/SPARK paths  (copy to local.env)
│   ├── templates/hadoop/         core-site.xml / hdfs-site.xml / workers templates
│   ├── templates/spark/          spark-defaults.conf / spark-env.sh templates
│   └── generated/nodeN/          rendered configs (git-ignored, made by scripts/render_configs.sh)
├── scripts/                      shared by all VMs
│   ├── env.sh                    sourced by every script (derives URLs, conf dirs, PYTHONPATH)
│   ├── setup_hosts.sh            dse-node1..4 aliases -> ZeroTier IPs in /etc/hosts
│   ├── install_iceberg.sh        Iceberg 1.8.1 runtime jar (as in the hands-on) -> /opt/iceberg
│   ├── render_configs.sh         writes Hadoop + Spark configs for THIS node
│   ├── submit.sh                 spark-submit wrapper (ships common/ as --py-files)
│   ├── check_cluster.sh          reachability, ports, versions, HDFS + Spark status
│   └── simulate_failure.sh       kill executor/worker/datanode for the fault-tolerance demo
├── common/dse/                   shared Python package (shipped to every Spark executor)
│   ├── config.py                 table names, HDFS paths, BM25/PageRank constants
│   ├── textproc.py               wikitext cleaner + tokenizer (same code for index AND query)
│   ├── validation.py             record validation + dedup rules
│   ├── tables.py                 Iceberg DDL, snapshot + property helpers
│   ├── fsutil.py                 HDFS helpers via the Spark JVM
│   ├── sparkutil.py              SparkSession helper
│   └── webhdfs.py                stdlib WebHDFS client (Node 4 pull, Node 1 upload fallback)
├── vm1_ingestion/                NODE 1: precheck.py, upload_to_hdfs.sh, run_phase.sh, jobs/ingest_job.py
├── vm2_storage/                  NODE 2: setup_hdfs.sh, start/stop, init_tables, iceberg_admin, hdfs_report
├── vm3_compute/                  NODE 3: start/stop, jobs/{index,pagerank,export}_job.py, run_pipeline.sh
├── vm4_search/                   NODE 4: app/ (FastAPI), puller/, frontend/, run_search.sh
├── data/
│   ├── full/pages.jsonl          Phase 0 corpus (1040 lines -> 1000 docs)
│   ├── full/changes.jsonl        Phase 1 change set (50 DELETE + 50 UPSERT)
│   ├── toy/                      tiny dataset for debugging
│   └── demo/bad_update.jsonl     valid-but-destructive batch for the rollback demo
├── tools/
│   ├── reference_pipeline.py     pure-Python version of the whole pipeline (laptop dev + cross-check)
│   └── compare_exports.py        diff two /serving/vN exports (Spark vs reference)
└── tests/                        pytest: tokenizer, validation, BM25 == provided snippet, ranking
```

---

## 2. How the pipeline works

### Phase flow
```
NODE 1  pages.jsonl / changes.jsonl
   │ precheck (local)  ->  hdfs dfs -put /dse/raw/phaseN/  (RF=2, 4 MB blocks)
   │ spark-submit ingest_job.py  --master spark://node3:7077   (driver on node 1)
   ▼
ICEBERG  dse.pages  (MERGE INTO -> new snapshot)        dse.rejected_records, dse.ingest_log
   │
NODE 3  run_pipeline.sh
   │ index_job     tokenise on executors -> shuffle by term -> dse.postings  (incremental via snapshot diff)
   │ pagerank_job  >=10 iterations, d=0.85                   -> dse.pagerank
   │ export_job    pin snapshots -> validate -> /serving/_tmp_vN -> rename /serving/vN -> _SUCCESS
   │ curl POST node4:8000/api/admin/pull
   ▼
NODE 4  WebHDFS download -> validate -> /serving/vN -> CURRENT=vN -> hot swap (old version kept if anything fails)
```

### Key design decisions (good to explain in the viva)
| Requirement | How it is met |
|---|---|
| Reject malformed / incomplete records | Executors validate every line (`common/dse/validation.py`). Reasons are stored in `dse.rejected_records`. The full corpus has 20 broken JSON lines. |
| Dedup, highest version wins | Window `row_number()` over `doc_id` ordered by `version DESC, line_no ASC`. The input files have no `version` field, so **version = phase number** unless a record carries one. A stored doc is replaced only by a *strictly higher* version. |
| Deletes | A `DELETE` writes a **tombstone** row (`is_deleted=true`) that keeps the version, so replaying an older phase can't resurrect the doc. |
| Remove dangling links | `raw_links` are stored as received. After every merge, `links` is recomputed for **all** live docs against the new set of live IDs, so links into docs deleted later are repaired too (toy data: deleting doc 2 cleans docs 1 and 3). Full data, Phase 1: 7 links in upserted docs point to missing pages and are stripped. |
| Same phase twice → no change | Only rows whose version, hash, tombstone or links changed are merged. Zero rows means no commit, so no new snapshot. |
| Detect changes from Iceberg | Ingestion diffs against the current snapshot. `index_job` diffs the previously indexed snapshot against the current one (time travel), so it only re-tokenises changed docs. |
| Distributed inverted index | Tokenisation runs in executor `mapPartitions`. Postings are hash-partitioned **by term**. df and doc length are aggregations over postings. |
| PageRank ≥ 10 iterations | RDD power iteration. Dangling-node mass is redistributed so `sum(PR)=1`. Co-partitioned joins, checkpoint every 5 iterations. Runs `max(10, --iterations)` with early stop once converged, never before iteration 10. |
| Spark fault tolerance | Lineage + HDFS checkpoints + `spark.task.maxFailures=4`. Kill a worker mid-PageRank and the job still finishes (`fault_tolerance_demo.sh`). |
| Consistent export | `postings` and `pagerank` record the `pages` snapshot they were built from. The export refuses mixed snapshots and reads pages `VERSION AS OF` that snapshot. |
| Switch only if export succeeded | 9 validation checks, then write to a temp dir, read it back, atomically rename, and write `_SUCCESS` last. Node 4 re-validates before switching `CURRENT`. |
| Keep Phase 0 if Phase 1 fails | Older `vN` dirs are never touched. Node 4 keeps serving the old index until the new one loads. One-click rollback in the UI. |
| BM25 | `vm4_search/app/engine.py` vectorises the provided `bm25.py` (kept verbatim as `app/bm25_reference.py`). A test asserts identical scores. |

---

## 3. Prerequisites (per VM)

| | Node 1 | Node 2 | Node 3 | Node 4 |
|---|---|---|---|---|
| ZeroTier joined | ✔ | ✔ | ✔ | ✔ |
| OpenJDK 11 | ✔ | ✔ | ✔ | – |
| Hadoop 3.3.6 | client (optional) | ✔ | ✔ | – |
| Spark 3.3.4 (`-bin-hadoop3`) | ✔ | ✔ | ✔ | – |
| Iceberg jar 1.8.1 | ✔ | ✔ | ✔ | – |
| Python 3 | 3.8–3.10, **same minor version on 1–3** | ✔ | ✔ | 3.9+ |

These match the Iceberg hands-on (Java 11, Hadoop 3.3.6, Spark 3.3.4, Iceberg 1.8.1) and what you already used for the Kafka + Spark assignment.

### Running on VMware Workstation Pro
* **Resources:** Node 2 and Node 3 need **≥ 4 GB RAM and 2 vCPUs** each (HDFS + Spark worker). Node 1 needs ≥ 3 GB, Node 4 ≥ 2 GB. If you lower `SPARK_WORKER_MEMORY` in `config/local.env`, keep it below the VM's free RAM.
* **Network adapter:** NAT is fine because ZeroTier tunnels through it. If all four VMs run on **one** laptop you can also use the VMnet IPs, but keep using ZeroTier so the setup matches the spec.
* **Cloned VMs (important!):** if you cloned one VM to create the others, each clone has the **same ZeroTier identity** and the same `/etc/machine-id`. ZeroTier will then flap between machines. Fix this on every clone:
  ```bash
  sudo systemctl stop zerotier-one
  sudo rm /var/lib/zerotier-one/identity.public /var/lib/zerotier-one/identity.secret
  sudo systemctl start zerotier-one && sudo zerotier-cli join <NETWORK_ID>   # then authorise it in ZeroTier Central
  sudo rm /etc/machine-id && sudo systemd-machine-id-setup
  ```
  Also give each clone a distinct hostname (`sudo hostnamectl set-hostname vm2`). The HDFS data dirs are created fresh by these scripts, so cloned Hadoop data won't clash.
* **Don't run the repo from a shared folder** (`/mnt/hgfs/...`): permissions and symlinks break. `git clone` it inside each VM's home directory.
* **Clock sync:** suspended VMs drift, and Spark/HDFS dislike skewed clocks. Run `sudo timedatectl set-ntp true`, or enable VMware Tools time sync.
* **Firewall:** Ubuntu's `ufw` is usually inactive. If it's enabled, allow the ZeroTier interface: `sudo ufw allow in on zt+`.
* **Suspend/resume:** after resuming a VM, check `zerotier-cli listnetworks` (status `OK`) before starting daemons.

---

## 4. One-time setup (every VM)

```bash
# 1) clone into your home directory (NOT a VMware shared folder)
git clone <your-repo-url> ~/distributed-search-engine
cd ~/distributed-search-engine

# 2) team lead ONLY: put the four ZeroTier IPs in config/cluster.env, commit, push.
#    Everyone else: git pull
ip -4 addr show | grep -A2 zt            # shows this VM's ZeroTier IP

# 3) per-VM settings
cp config/local.env.example config/local.env
nano config/local.env                     # NODE_ID=1|2|3|4, JAVA_HOME, HADOOP_HOME, SPARK_HOME
#   find your paths:  readlink -f $(which java) | sed 's:/bin/java::'   echo $HADOOP_HOME $SPARK_HOME

# 4) hostnames dse-node1..4 -> ZeroTier IPs (all 4 VMs)
bash scripts/setup_hosts.sh

# 5) Nodes 1, 2, 3 only
bash scripts/install_iceberg.sh
bash scripts/render_configs.sh            # writes config/generated/nodeN/{hadoop,spark}

# 6) Node 4 only
bash vm4_search/setup_node4.sh

# 7) anyone, any time
bash scripts/check_cluster.sh
```

> The scripts never modify `$HADOOP_HOME/etc/hadoop` or `$SPARK_HOME/conf`. `env.sh` points `HADOOP_CONF_DIR` and `SPARK_CONF_DIR` at `config/generated/nodeN/`, so your earlier lab setups keep working. Always run commands through these scripts, or `source scripts/env.sh` first if you run `hdfs` by hand.

> If a script says *permission denied*, run it with `bash script.sh` (that's how all commands in this README are written), or `chmod +x scripts/*.sh vm*/*.sh` once.

---

## 5. Start the cluster (every session)

Order matters: **Node 2 HDFS → Node 3 master → workers**.

```bash
# NODE 2 - first time ever only:
bash vm2_storage/setup_hdfs.sh            # formats the NameNode, starts NN + DN, creates dirs
# NODE 2 - every other time:
bash vm2_storage/start_storage.sh --no-worker

# NODE 3
bash vm3_compute/start_compute.sh         # Spark master + worker + DataNode

# NODE 2
bash vm2_storage/start_storage.sh         # (re)starts the worker now that the master is up
bash vm2_storage/init_tables.sh           # creates Iceberg tables (smoke test)
bash vm2_storage/hdfs_report.sh           # "Live datanodes (2)"

# NODE 4
bash vm4_search/run_search.sh             # UI at http://<NODE4_IP>:8000 (empty until the first export)
```
Check the web UIs:
* Spark master: `http://<NODE3_IP>:8080` should show **2 ALIVE workers**
* HDFS NameNode: `http://<NODE2_IP>:9870` → Datanodes tab should show **2 live**
* Search: `http://<NODE4_IP>:8000`

---

## 6. Phase 0: base corpus

```bash
# NODE 1
bash vm1_ingestion/run_phase.sh 0
#   precheck: 1040 lines, 1020 accepted, 20 malformed_json, 20 duplicates superseded
#   uploaded to /dse/raw/phase0/pages.jsonl (replication 2)
#   Spark: 1000 inserted -> Iceberg snapshot S0

# NODE 3
bash vm3_compute/run_pipeline.sh
#   index (full) -> PageRank (>=10 iterations) -> export /serving/v1 -> notify Node 4

# NODE 4: the UI switches to v1 automatically (header badge: "v1 · phase 0 · 1000 docs")
```
Look at the results: search a few queries and screenshot them for the report.

## 7. Phase 1: change set

```bash
# NODE 1
bash vm1_ingestion/run_phase.sh 1
#   50 deletes + 50 upserts (25 updates, 25 new docs); 7 dangling links stripped -> snapshot S1 (975 live docs)

# NODE 3
bash vm3_compute/run_pipeline.sh          # index updated INCREMENTALLY (only changed docs)

# NODE 4: v2 becomes active (975 docs). Use the "Compare phases" tab: v1 -> v2.
```
Re-running `run_phase.sh 1` prints `rows to commit 0 / INGESTION NOOP`: ingestion is idempotent.

If Phase 1 ingestion, the pipeline, or the export fails, `/serving/v1` stays active on Node 4.

---

## 8. Demo scripts for the evaluation

### Iceberg snapshots, time travel, rollback (Node 2)
```bash
bash vm2_storage/iceberg_admin.sh snapshots pages      # S0 (append), S1 (overwrite = MERGE)
bash vm2_storage/iceberg_admin.sh doc 1                # doc 1: live in S0, DELETED in S1
bash vm2_storage/iceberg_admin.sh diff <S0> <S1>       # 50 deleted, 25 updated, 25 added
bash vm2_storage/iceberg_admin.sh timetravel pages <S0> --where "doc_id='1'"
bash vm2_storage/iceberg_admin.sh rejected             # the dirty input we refused, with reasons
bash vm2_storage/iceberg_admin.sh ingest-log
```

### Recovering from a bad update
```bash
bash vm1_ingestion/run_phase.sh 2 --file data/demo/bad_update.jsonl   # Node 1: deletes 40 hub pages, vandalises 5
bash vm3_compute/run_pipeline.sh                                       # Node 3: v3 is "bad" (search "water")
# instant fix in the serving layer: Node 4 UI -> Index & versions -> activate v2
# permanent fix in the lakehouse:
bash vm2_storage/iceberg_admin.sh snapshots pages                      # Node 2: find S1
bash vm2_storage/iceberg_admin.sh rollback pages <S1>
bash vm3_compute/run_pipeline.sh                                       # Node 3: v4 == v2 content
```

### Spark fault tolerance (Nodes 3 + 2)
```bash
bash vm3_compute/fault_tolerance_demo.sh     # Node 3: PageRank with a 12 s pause per iteration
bash scripts/simulate_failure.sh executor    # Node 2, around iteration 3 (or: worker)
# -> "Lost executor ... ", tasks re-run on node 3, PageRank completes with sum=1.000000
bash scripts/simulate_failure.sh restore     # Node 2
```

### HDFS replication (Nodes 2 + 3)
```bash
bash vm2_storage/hdfs_report.sh              # every block has 2 replicas (node2 + node3)
bash scripts/simulate_failure.sh datanode    # Node 3
hdfs dfs -cat /dse/raw/phase0/pages.jsonl | wc -l   # Node 2: still 1040 - served from the surviving replica
bash scripts/simulate_failure.sh restore     # Node 3
```

---

## 9. Developing without the cluster

### Toy dataset on the real cluster
`DSE_PROFILE=toy` uses separate tables (`dse_toy`) and directories (`/serving_toy`), so it never mixes with the real data:
```bash
DSE_PROFILE=toy bash vm1_ingestion/run_phase.sh 0      # node 1
DSE_PROFILE=toy bash vm3_compute/run_pipeline.sh       # node 3
DSE_PROFILE=toy bash vm4_search/run_search.sh          # node 4
DSE_PROFILE=toy bash vm2_storage/reset_profile.sh --yes   # start over
```

### Whole pipeline on ONE machine (Spark `local[2]`, no HDFS)
Needs Java 11, Spark 3.3.4, and the Iceberg jar locally. `DSE_LOCAL=1` stores everything under `.local/`:
```bash
export DSE_LOCAL=1 DSE_PROFILE=toy
bash vm1_ingestion/run_phase.sh 0 && bash vm3_compute/run_pipeline.sh
bash vm1_ingestion/run_phase.sh 1 && bash vm3_compute/run_pipeline.sh
bash vm4_search/run_search.sh                           # pulls from .local/fs/serving_toy
```

### No Spark at all (Node 4 UI work)
```bash
pip install -r requirements-dev.txt
python3 tools/reference_pipeline.py --out .local/ref data/full/pages.jsonl data/full/changes.jsonl
SERVING_DIR=$PWD/.local/ref bash -c 'cd vm4_search && python3 -m uvicorn app.main:app --port 8000'
python3 tools/compare_exports.py /serving/v2 .local/ref/v2  # cross-check the Spark export (on node 4)
python3 -m pytest -q tests
```

---

## 10. Ports (open these on the ZeroTier interface if you use a firewall)

| Port | Node | Service |
|---|---|---|
| 9000 | 2 | HDFS NameNode RPC |
| 9870 | 2 | NameNode web UI + **WebHDFS** (Node 4 pulls here) |
| 9864 / 9866 / 9867 | 2, 3 | DataNode HTTP / data transfer / IPC |
| 7077 / 8080 | 3 | Spark master / master UI |
| 8081 | 2, 3 | Spark worker UI |
| 4040 | 1, 3 | running application UI (driver) |
| 7078 / 7079 (+retries) | 1, 2, 3 | Spark driver / block manager (executors connect back to the driver) |
| 18080 | 3 | Spark history server (optional) |
| 8000 | 4 | Search API + UI |

---

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| `Live datanodes (1)` | On Node 3 check `jps` for DataNode, then `logs/hadoop/*datanode*.log`. A *clusterID mismatch* means the NameNode was re-formatted: `rm -rf ~/dse-hdfs/datanode` on Node 3 and restart the DataNode. |
| Worker not shown in the Spark UI | `SPARK_LOCAL_IP` must be the ZeroTier IP (`render_configs.sh` sets it). Check `logs/spark/*Worker*.out` and make sure Node 3's master is up. |
| Job hangs at "Initial job has not accepted any resources" | Worker memory is too small for `SPARK_EXECUTOR_MEMORY`, or executors can't reach the driver. Make sure `spark.driver.host` is the driver node's ZeroTier IP and ports 7078/7079 are open. |
| `UnknownHostException: dse-node3` | Run `bash scripts/setup_hosts.sh` on that VM (Node 4 too: WebHDFS redirects to DataNodes by name). |
| `Python in worker has different version` | Install the same Python minor version on Nodes 1–3, or point `PYSPARK_PYTHON` at it in `local.env`. |
| Exception mentioning `distutils` / cloudpickle on Python 3.11+ | Spark 3.3 supports Python ≤ 3.10. Install `python3.10` and set `PYSPARK_PYTHON=python3.10`. |
| `Iceberg catalog ... not configured` | Submit jobs via `scripts/submit.sh` (it passes the generated `spark-defaults.conf`). |
| Export says "pages has a newer snapshot" | Ingestion ran after indexing. Run the full `run_pipeline.sh`. |
| Node 4 never switches | `bash vm4_search/pull_export.sh` prints why. Rejected versions go to `$SERVING_DIR/.rejected-vN`. |
| Safe mode | `hdfs dfsadmin -safemode get` on Node 2. It clears once both DataNodes report. |
