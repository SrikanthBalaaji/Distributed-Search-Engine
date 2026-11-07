#!/usr/bin/env python3
"""
NODE 1 - local validation on the gateway BEFORE anything is uploaded to HDFS.

    python3 vm1_ingestion/precheck.py data/full/pages.jsonl --phase 0
    python3 vm1_ingestion/precheck.py data/full/changes.jsonl --phase 1 --json report.json

Uses exactly the same rules as the Spark ingestion job (common/dse/validation.py),
so the numbers printed here must match the job's validation summary.
Exit code 1 only if the file is missing or NOTHING in it is valid.
"""
import argparse
import collections
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
from dse.validation import parse_record, pick_winners  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--phase", type=int, default=0)
    ap.add_argument("--json", help="write the report as JSON to this path")
    ap.add_argument("--show", type=int, default=5, help="sample rejected lines to print")
    a = ap.parse_args()

    if not os.path.isfile(a.file):
        print("[precheck] file not found: %s" % a.file)
        return 1

    accepted, rejected = [], []
    with open(a.file, encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            rec = parse_record(line.rstrip("\r\n"), i, a.phase)
            (rejected if rec["reject_reason"] else accepted).append(rec)

    winners, losers = pick_winners(accepted)
    reasons = collections.Counter(r["reject_reason"] for r in rejected)
    ops = collections.Counter(r["op"] for r in winners.values())
    ids_in_file = {d for d, r in winners.items() if r["op"] == "UPSERT"}
    total_links = sum(len(r["raw_links"]) for r in winners.values())
    dangling_in_file = sum(1 for r in winners.values() for l in r["raw_links"] if l not in ids_in_file)

    report = {
        "file": a.file, "phase": a.phase,
        "total_lines": len(accepted) + len(rejected),
        "accepted": len(accepted), "rejected": len(rejected),
        "unique_doc_ids": len(winners), "duplicates_superseded": len(losers),
        "ops": dict(ops), "rejected_by_reason": dict(reasons),
        "links_total": total_links,
        "links_pointing_outside_this_file": dangling_in_file,
    }

    print("=" * 64)
    print(" NODE 1 :: local precheck  %s  (phase %d)" % (a.file, a.phase))
    print("=" * 64)
    for k in ("total_lines", "accepted", "rejected", "unique_doc_ids", "duplicates_superseded",
              "links_total", "links_pointing_outside_this_file"):
        print("  %-34s %s" % (k, report[k]))
    print("  ops:", dict(ops))
    if reasons:
        print("  rejected by reason:")
        for k, v in reasons.most_common():
            print("     %-28s %d" % (k, v))
        print("  sample rejected lines:")
        for r in rejected[: a.show]:
            print("     line %-6d %-24s %s" % (r["line_no"], r["reject_reason"], (r["raw_preview"] or "")[:60].replace("\n", " ")))
    if a.phase == 0 and dangling_in_file:
        print("  NOTE: links to ids not in this file are dangling and will be stripped by the Spark job.")
    elif a.phase > 0:
        print("  NOTE: links in a change set are checked against the Iceberg table by the Spark job.")

    if a.json:
        with open(a.json, "w") as f:
            json.dump(report, f, indent=2)
        print("  report written to", a.json)

    if not accepted:
        print("[precheck] no valid records - refusing to continue")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
