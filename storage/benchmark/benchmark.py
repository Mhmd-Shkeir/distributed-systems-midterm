"""Benchmark: replication versus erasure coding on two workloads.

For every (workload, durability) pair the script
  1. creates a bucket with that durability policy and uploads the whole dataset (throughput, latency),
  2. measures the storage overhead (bytes stored on the nodes / logical bytes),
  3. reads everything back and verifies every SHA-256 (throughput, latency),
  4. fails one storage node and measures how long detection and repair take and how much data the
     repair job moves between the gateway and the nodes (repair traffic),
  5. restarts the node, deletes the data and checks that the cluster is clean again.

Needs the cluster running (docker compose -f storage/docker-compose.yml up -d). Erasure coding k+m
needs k+m live nodes to restore full redundancy after a failure, so for 2+2 to be repairable start
a fifth node first:  docker compose -f storage/docker-compose.yml --profile scale up -d node5
"""
import argparse
import concurrent.futures as cf
import csv
import hashlib
import json
import os
import statistics
import subprocess
import sys
import threading
import time

import requests

sys.path.insert(0, os.path.dirname(__file__))
from generate_dataset import MiB, workload  # noqa: E402

META = os.environ.get("META_URL", "http://localhost:8000")
CONFIGS = {
    "replication N=3 (W=2,R=2)": dict(durability="replication", copies=3, write_quorum=2, read_quorum=2),
    "erasure coding 2+1": dict(durability="ec", k=2, m=1),
    "erasure coding 2+2": dict(durability="ec", k=2, m=2),
}
SHORT = {"rep3": "replication N=3 (W=2,R=2)", "ec21": "erasure coding 2+1", "ec22": "erasure coding 2+2"}
_tls = threading.local()


def session():
    if not hasattr(_tls, "s"):
        _tls.s = requests.Session()
    return _tls.s


def admin(path, method="get", **params):
    return getattr(requests, method)(f"{META}/admin/{path}", params=params, timeout=600).json()


def nodes():
    return {n["node_id"]: n for n in requests.get(f"{META}/nodes", timeout=10).json()}


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def wait_for(cond, timeout, every=1.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return True
        except requests.RequestException:
            pass
        time.sleep(every)
    return False


def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(p / 100 * len(values)))] if values else 0.0


def timed(fn, items, workers):
    """Run fn(item) over items with a thread pool; return (wall seconds, per-call latencies, errors)."""
    lat, errors = [], []

    def one(item):
        t = time.perf_counter()
        try:
            fn(item)
            return time.perf_counter() - t, None
        except Exception as exc:  # noqa: BLE001
            return None, f"{item[0]}: {exc}"

    start = time.perf_counter()
    with cf.ThreadPoolExecutor(workers) as ex:
        for l, e in ex.map(one, items):
            (lat if e is None else errors).append(l if e is None else e)
    return time.perf_counter() - start, lat, errors


def run_one(workload_name, files, cfg_name, cfg, args):
    slug = "".join(c for c in cfg_name.lower() if c.isalnum())[:24]
    bucket = f"bench-{workload_name}-{slug}"
    logical = sum(len(d) for _, d in files)
    row = {"workload": workload_name, "durability": cfg_name, "files": len(files), "logical_mib": round(logical / MiB, 1)}
    print(f"\n=== {workload_name} / {cfg_name}: {len(files)} files, {logical / MiB:.1f} MiB", flush=True)
    requests.delete(f"{META}/{bucket}")
    r = requests.put(f"{META}/{bucket}", params=cfg)
    assert r.status_code == 201, r.text
    s0 = admin("stats")

    # 1. upload
    def put(item):
        name, data = item
        resp = session().put(f"{META}/{bucket}/{name}", data=data, timeout=600)
        if resp.status_code != 200:
            raise RuntimeError(resp.text[:100])

    workers = args.small_workers if workload_name == "small" else args.large_workers
    wall, lat, errs = timed(put, files, workers)
    row.update(put_seconds=round(wall, 2), put_mib_s=round(logical / MiB / wall, 1), put_ops_s=round(len(files) / wall, 1),
               put_p50_ms=round(pct(lat, 50) * 1000, 1), put_p95_ms=round(pct(lat, 95) * 1000, 1), put_errors=len(errs))
    print(f"  upload: {wall:.1f}s  {row['put_mib_s']} MiB/s  {row['put_ops_s']} files/s  p50 {row['put_p50_ms']} ms  p95 {row['put_p95_ms']} ms  errors {len(errs)}")

    # 2. storage overhead
    s1 = admin("stats")
    raw = s1["raw_bytes"] - s0["raw_bytes"]
    row.update(stored_mib=round(raw / MiB, 1), overhead=round(raw / logical, 3))
    print(f"  stored on nodes: {raw / MiB:.1f} MiB  -> storage overhead {row['overhead']}x")

    # 3. read back and verify
    expect = {n: hashlib.sha256(d).hexdigest() for n, d in files}

    def get(item):
        resp = session().get(f"{META}/{bucket}/{item[0]}", timeout=600)
        if resp.status_code != 200 or hashlib.sha256(resp.content).hexdigest() != expect[item[0]]:
            raise RuntimeError(f"bad read {resp.status_code}")

    wall, lat, errs = timed(get, files, workers)
    row.update(get_seconds=round(wall, 2), get_mib_s=round(logical / MiB / wall, 1), get_ops_s=round(len(files) / wall, 1),
               get_p50_ms=round(pct(lat, 50) * 1000, 1), get_p95_ms=round(pct(lat, 95) * 1000, 1), get_errors=len(errs))
    print(f"  read:   {wall:.1f}s  {row['get_mib_s']} MiB/s  {row['get_ops_s']} files/s  p50 {row['get_p50_ms']} ms  p95 {row['get_p95_ms']} ms  errors {len(errs)}")

    # 4. node failure and repair
    if not args.no_repair:
        victim = args.victim
        usage = admin(f"nodes/{victim}/usage")
        row.update(victim=victim, lost_pieces=usage["pieces"], lost_mib=round(usage["bytes"] / MiB, 1))
        wait_for(lambda: admin("status")["under_protected_chunks"] == 0, 60)
        admin("traffic/reset", method="post")
        t0 = time.time()
        docker("stop", f"dfs-storage-{victim}-1")
        detected = wait_for(lambda: not nodes()[victim]["alive"], 90)
        t_detect = time.time() - t0
        repaired = wait_for(lambda: admin("status")["under_protected_chunks"] == 0, args.repair_timeout, 2)
        t_repair = time.time() - t0
        time.sleep(12)  # let a possible last repair pass finish before reading the counters
        tr = admin("traffic").get("repair", {"bytes_read": 0, "bytes_written": 0, "ops": 0})
        lost = usage["bytes"] or 1
        row.update(detect_seconds=round(t_detect, 1) if detected else None, repair_complete=bool(repaired),
                   repair_seconds=round(t_repair, 1), repair_read_mib=round(tr["bytes_read"] / MiB, 1),
                   repair_written_mib=round(tr["bytes_written"] / MiB, 1),
                   read_amplification=round(tr["bytes_read"] / lost, 2), write_amplification=round(tr["bytes_written"] / lost, 2))
        print(f"  failure of {victim}: held {row['lost_mib']} MiB in {usage['pieces']} pieces; detected after {row['detect_seconds']}s; "
              f"repair {'complete' if repaired else 'INCOMPLETE'} after {t_repair:.0f}s")
        print(f"  repair traffic: read {row['repair_read_mib']} MiB, wrote {row['repair_written_mib']} MiB "
              f"(read x{row['read_amplification']}, write x{row['write_amplification']} of the lost data)")
        wall, lat, errs = timed(get, files, workers)
        row["reads_after_repair_ok"] = len(errs) == 0
        print(f"  all files readable and verified after repair: {len(errs) == 0}")
        docker("start", f"dfs-storage-{victim}-1")
        wait_for(lambda: nodes()[victim]["alive"], 90)

    # 5. clean up
    for name, _ in files:
        requests.delete(f"{META}/{bucket}/{name}")
    requests.delete(f"{META}/{bucket}")
    admin("gc", method="post")
    wait_for(lambda: admin("status")["under_protected_chunks"] == 0, 120, 2)
    time.sleep(3)
    return row


def write_results(rows, out):
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "benchmark_results.json"), "w") as f:
        json.dump(rows, f, indent=1)
    cols = sorted({k for r in rows for k in r}, key=lambda c: list(rows[0]).index(c) if c in rows[0] else 99)
    with open(os.path.join(out, "benchmark_results.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    md = ["# Benchmark results: replication versus erasure coding\n"]
    for title, keys in [
        ("Storage overhead and throughput", ["workload", "durability", "files", "logical_mib", "stored_mib", "overhead",
                                             "put_mib_s", "put_p50_ms", "put_p95_ms", "get_mib_s", "get_p50_ms", "get_p95_ms"]),
        ("Repair after the failure of one node", ["workload", "durability", "lost_mib", "detect_seconds", "repair_seconds",
                                                  "repair_read_mib", "repair_written_mib", "read_amplification",
                                                  "write_amplification", "repair_complete"]),
    ]:
        md += [f"## {title}\n", "| " + " | ".join(keys) + " |", "|" + "---|" * len(keys)]
        md += ["| " + " | ".join(str(r.get(k, "")) for k in keys) + " |" for r in rows]
        md.append("")
    with open(os.path.join(out, "benchmark_summary.md"), "w") as f:
        f.write("\n".join(md))
    print(f"\nresults written to {out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workloads", default="small,large")
    ap.add_argument("--durability", default="rep3,ec21,ec22", help="comma separated, any of: " + ", ".join(f"{k} = {v}" for k, v in SHORT.items()))
    ap.add_argument("--small-count", type=int, default=600)
    ap.add_argument("--large-count", type=int, help="use only the first N large files (8,12,16,24,32,40 MiB)")
    ap.add_argument("--small-workers", type=int, default=8)
    ap.add_argument("--large-workers", type=int, default=2)
    ap.add_argument("--victim", default="node3")
    ap.add_argument("--repair-timeout", type=int, default=240)
    ap.add_argument("--no-repair", action="store_true")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results"))
    args = ap.parse_args()

    alive = sum(n["alive"] for n in nodes().values())
    print(f"cluster: {alive} live storage nodes")
    rows = []
    for wl in args.workloads.split(","):
        files = list(workload(wl, args.small_count if wl == "small" else args.large_count))
        for short in args.durability.split(","):
            cfg_name = SHORT[short.strip()]
            rows.append(run_one(wl, files, cfg_name, CONFIGS[cfg_name], args))
            write_results(rows, args.out)  # keep partial results if the run is interrupted
    print("\n" + open(os.path.join(args.out, "benchmark_summary.md")).read())


if __name__ == "__main__":
    main()
