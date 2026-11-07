# Execution Guide: step by step

This guide tells each of us **what to run, on which VM, and when**.
Follow the steps **in order**. Wait for the ✅ check before moving on.

---

## Who is who

| Node | Job | Person | ZeroTier IP |
|:---:|---|---|---|
| **Node 1** | Ingestion: uploads data and runs the ingestion job | Nikhil | `172.27.211.189` |
| **Node 2** | Storage: HDFS NameNode + DataNode, Spark Worker, Iceberg tables | Sathya | `172.27.180.147` |
| **Node 3** | Compute: Spark Master + Worker, HDFS DataNode, runs index / PageRank / export | Srikanth | `172.27.143.38` |
| **Node 4** | Search: website + search API | Sushanth | `172.27.88.66` |

Handy links once the cluster is running:
* Spark master UI: http://172.27.143.38:8080
* HDFS NameNode UI: http://172.27.180.147:9870
* Search website: http://172.27.88.66:8000

> **Rule 1:** run every command from inside the repo folder: `cd ~/Distributed-Search-Engine`
> **Rule 2:** always start commands with `bash` (example: `bash scripts/check_cluster.sh`).
> **Rule 3:** when a step says *"tell Node X"*, message them on the group chat. The next step depends on yours.

---

## The big picture

```
  Node 1                Node 2 + Node 3                  Node 3                 Node 4
  ──────                ───────────────                  ──────                 ──────
  upload data   ──►   stored in HDFS + Iceberg   ──►   index + PageRank   ──►   website shows
  + ingest                                              + export                 the new results
```

For every phase it is always the same 3 steps: **Node 1 → Node 3 → Node 4 (automatic)**.

---

# PART 1: One-time setup (do this only once)

### Step 1 · ALL 4 nodes · make sure ZeroTier is online

The IPs are already filled in `config/cluster.env` (see the table above). Check that yours matches:
```bash
sudo zerotier-cli listnetworks      # status must be OK, and the IP must match the table
ip -4 addr show | grep -A2 zt
```
If your ZeroTier IP ever changes, tell the team lead (Srikanth) so `config/cluster.env` can be updated and pushed.

### Step 2 · ALL 4 nodes · get the code

```bash
cd ~
git clone https://github.com/SrikanthBalaaji/Distributed-Search-Engine.git
cd Distributed-Search-Engine
```
(If you already cloned it, just run `git pull`.)

### Step 3 · ALL 4 nodes · create your personal settings file

```bash
cp config/local.env.example config/local.env
nano config/local.env
```
Change these lines:

| Setting | What to put |
|---|---|
| `NODE_ID` | your node number: `1`, `2`, `3` or `4` |
| `JAVA_HOME` | your Java 11 path (find it: `readlink -f $(which java) \| sed 's:/bin/java::'`) |
| `HADOOP_HOME` | your Hadoop folder (find it: `echo $HADOOP_HOME`) |
| `SPARK_HOME` | your Spark folder (find it: `echo $SPARK_HOME`) |

Node 4 only needs to change `NODE_ID=4`.
Save with `Ctrl+O`, `Enter`, `Ctrl+X`.

### Step 4 · ALL 4 nodes · add hostnames

```bash
bash scripts/setup_hosts.sh
```
✅ It prints `dse-node1` … `dse-node4`, each with the correct IP.

### Step 5 · Nodes 1, 2, 3 · install Iceberg and generate configs

```bash
bash scripts/install_iceberg.sh
bash scripts/render_configs.sh
```
✅ You see `SHA-1 verification: OK` and `[render] done.`

### Step 6 · Node 4 · install Python packages

```bash
bash vm4_search/setup_node4.sh
```
✅ Ends with `[node4] done.`

### Step 7 · Node 2 · format HDFS (FIRST TIME ONLY)

```bash
bash vm2_storage/setup_hdfs.sh
```
✅ Ends with `[hdfs] done.` and a list of folders (`/dse`, `/serving`, `/warehouse` …).
⚠️ **Never run this again** unless you want to wipe everything.

➡️ **Tell Node 3:** "HDFS is up."

---

# PART 2: Start the cluster (every time you meet to work)

Do these **in this exact order**.

### Step 8 · Node 2 · start HDFS

```bash
bash vm2_storage/start_storage.sh --no-worker
```
(Skip this step on the day you ran Step 7: HDFS is already running.)
✅ `jps` shows `NameNode` and `DataNode`.

➡️ **Tell Node 3:** "HDFS started."

### Step 9 · Node 3 · start Spark master, worker and DataNode

```bash
bash vm3_compute/start_compute.sh
```
✅ `jps` shows `Master`, `Worker`, `DataNode`.

➡️ **Tell Node 2:** "Master is up."

### Step 10 · Node 2 · start the Spark worker and create the tables

```bash
bash vm2_storage/start_storage.sh
bash vm2_storage/init_tables.sh
```
✅ `init_tables` prints the 6 tables (`pages`, `postings`, `pagerank`, …).

### Step 11 · Anyone · health check

```bash
bash scripts/check_cluster.sh
```
✅ Everything shows `[ OK ]` (except `Search API`, which starts in the next step).
✅ Open `http://<NODE3_IP>:8080` in a browser: **2 ALIVE workers**.
✅ Open `http://<NODE2_IP>:9870` → *Datanodes* tab: **2 live DataNodes**.

### Step 12 · Node 4 · start the website

```bash
bash vm4_search/run_search.sh
```
✅ Open `http://<NODE4_IP>:8000`: the page loads and says *"no data yet"* (that is correct for now).
Leave this terminal open, because the website stops if you close it.

➡️ **Tell Node 1:** "Cluster ready, start Phase 0."

---

# PART 3: Phase 0 (load the main dataset)

### Step 13 · Node 1 · ingest `pages.jsonl`

```bash
bash vm1_ingestion/run_phase.sh 0
```
✅ At the end you see:
```
rejected                     20      (the 20 broken lines)
duplicates                   20
live documents after merge   1000
INGESTION APPLIED
```

➡️ **Tell Node 3:** "Phase 0 ingested."

### Step 14 · Node 3 · build index, PageRank, export

```bash
bash vm3_compute/run_pipeline.sh
```
✅ You see `### index finished`, then `### pagerank finished`, then `### export finished`.
✅ All the checks say `[PASS]`.
✅ Last line: `Node 4 pulled + activated the new version.`

### Step 15 · Node 4 · check the website

Nothing to run. Refresh `http://<NODE4_IP>:8000`.
✅ Top-right badge shows **`v1 · phase 0 · 1000 docs`**.
✅ Search for `solar system planet`; you should get results.

📸 **Take screenshots now** (needed for the report). Phase 1 will change the results.

➡️ **Tell Node 1:** "Phase 0 looks good, start Phase 1."

---

# PART 4: Phase 1 (apply the changes)

### Step 16 · Node 1 · ingest `changes.jsonl`

```bash
bash vm1_ingestion/run_phase.sh 1
```
✅ You see:
```
deleted                      50
inserted                     25
updated                      25
dangling links removed       7
live documents after merge   975
INGESTION APPLIED
```

➡️ **Tell Node 3:** "Phase 1 ingested."

### Step 17 · Node 3 · run the pipeline again

```bash
bash vm3_compute/run_pipeline.sh
```
✅ The index step says `mode=incremental` and `documents changed ...: 100`.
✅ All checks `[PASS]`, and Node 4 is notified.

### Step 18 · Node 4 · check the website

✅ Badge shows **`v2 · phase 1 · 975 docs`**.
✅ Open the **Compare phases** tab: choose `v1 → v2` and search `cube` to see which pages moved, appeared or disappeared.

🎉 **The main project is complete.**

---

# PART 5: Demos for the evaluation

Run these **after Phase 1**. Each demo is independent.

### Demo A · Iceberg versions and time travel (Node 2)

```bash
bash vm2_storage/iceberg_admin.sh snapshots pages     # list versions of the table; note the 2 snapshot IDs
bash vm2_storage/iceberg_admin.sh doc 1               # doc 1: live in Phase 0, deleted in Phase 1
bash vm2_storage/iceberg_admin.sh diff <ID1> <ID2>    # what changed between the two versions
bash vm2_storage/iceberg_admin.sh rejected            # the bad records we refused, and why
```

### Demo B · Running the same phase twice changes nothing (Node 1)

```bash
bash vm1_ingestion/run_phase.sh 1
```
✅ Prints `rows to commit 0` and `INGESTION NOOP`.

### Demo C · HDFS keeps working when a DataNode dies (Nodes 2 + 3)

```bash
bash vm2_storage/hdfs_report.sh                         # Node 2: every block has 2 copies
bash scripts/simulate_failure.sh datanode               # Node 3: stop its DataNode
hdfs dfs -cat /dse/raw/phase0/pages.jsonl | wc -l       # Node 2: still prints 1040
bash scripts/simulate_failure.sh restore                # Node 3: bring it back
```
(On Node 2, run `source scripts/env.sh` once before using `hdfs` directly.)

### Demo D · Spark survives a worker crash (Nodes 3 + 2)

```bash
bash vm3_compute/fault_tolerance_demo.sh                # Node 3: runs PageRank slowly
bash scripts/simulate_failure.sh executor               # Node 2: wait for "iter 3", then run this
bash scripts/simulate_failure.sh restore                # Node 2: after PageRank finishes
```
✅ Node 3 shows `Lost executor`, but PageRank still finishes with `sum=1.000000`.

### Demo E · Recover from a bad update (Nodes 1 → 3 → 4 → 2 → 3)

| # | Node | Command / action |
|:-:|:-:|---|
| 1 | 2 | `bash vm2_storage/iceberg_admin.sh snapshots pages` → **write down the latest snapshot ID** (= the good Phase 1 state) |
| 2 | 1 | `bash vm1_ingestion/run_phase.sh 2 --file data/demo/bad_update.jsonl` |
| 3 | 3 | `bash vm3_compute/run_pipeline.sh` → website now shows the broken **v3** |
| 4 | 4 | Quick fix: website → **Index & versions** tab → click **activate** next to `v2` |
| 5 | 2 | Permanent fix: `bash vm2_storage/iceberg_admin.sh rollback pages <ID from row 1>` |
| 6 | 3 | `bash vm3_compute/run_pipeline.sh` → new **v4**, identical to v2 |

---

# PART 6: Shut down (end of the session)

| Order | Node | Command |
|:-:|:-:|---|
| 1 | Node 4 | press `Ctrl+C` in the website terminal |
| 2 | Node 2 | `bash vm2_storage/stop_storage.sh` |
| 3 | Node 3 | `bash vm3_compute/stop_compute.sh` |

Next session: start again from **Part 2**. Your data is still there in HDFS.

---

## Quick cheat sheet

| When | Node 1 | Node 2 | Node 3 | Node 4 |
|---|---|---|---|---|
| **Once** | settings, hosts, iceberg, render | settings, hosts, iceberg, render, `setup_hdfs.sh` | settings, hosts, iceberg, render | settings, hosts, `setup_node4.sh` |
| **Every session** | – | `start_storage.sh --no-worker` → (wait for Node 3) → `start_storage.sh` → `init_tables.sh` | `start_compute.sh` | `run_search.sh` |
| **Phase 0** | `run_phase.sh 0` | – | `run_pipeline.sh` | check website |
| **Phase 1** | `run_phase.sh 1` | – | `run_pipeline.sh` | check website |
| **End** | – | `stop_storage.sh` | `stop_compute.sh` | `Ctrl+C` |

---

## Something went wrong?

| Problem | What to do |
|---|---|
| `permission denied` | Put `bash` in front of the command. |
| `Unable to load native-hadoop library` (Node 2, arm64) | Harmless warning, ignore it. |
| `Python in worker has different version` | Nodes 1, 2, 3 must run the same Python minor version. Check with `python3 --version` on each. |
| A node can't be reached | `sudo zerotier-cli listnetworks` must say `OK`. Restart with `sudo systemctl restart zerotier-one`. |
| `NODE_ID not set` | You skipped Step 3. Create `config/local.env`. |
| Only **1** live DataNode | On Node 3: run `jps`. If `DataNode` is missing, run `bash vm3_compute/start_compute.sh` again. |
| Only **1** Spark worker | On Node 2: `bash vm2_storage/start_storage.sh` (Node 3's master must already be running). |
| Job stuck at *"has not accepted any resources"* | A worker is down or out of memory. Check `http://<NODE3_IP>:8080`. |
| `UnknownHostException dse-node…` | Run `bash scripts/setup_hosts.sh` on that VM. |
| Website still shows the old version | On Node 4: `bash vm4_search/pull_export.sh`. It prints the reason. |
| A phase or pipeline step failed | Nothing is lost: the website keeps showing the last good version. Fix the problem and run the same step again. |
| Anything else | Run `bash scripts/check_cluster.sh` and share the output on the group chat. |

For more detail, see [README.md](README.md) and the README inside each `vmN_*` folder.
