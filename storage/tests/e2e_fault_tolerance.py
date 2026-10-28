"""Fault tolerance: degraded reads, failure detection, automatic repair, integrity checks, scrubbing."""
import os
import time

import requests
from common import *  # noqa: F401,F403
from common import (META, NODES, MiB, RUN, admin, check, container, docker, finish, get, nodes, on_node, put,
                    under_protected, wait_cluster, wait_for, wipe_bucket)

B = f"fault-{RUN}"
wait_cluster(4)
requests.put(f"{META}/{B}")

# A mixed set: 3x replicated, erasure coded 2+2 (4 shards), and erasure coded 2+1 (3 shards).
objs = {}
for i in range(6):
    objs[f"rep/{i}.bin"] = (os.urandom(200 * 1024 + i * 50000), dict(durability="replication", copies=3))
objs["rep/big.bin"] = (os.urandom(9 * MiB), dict(durability="replication", copies=3))
for i in range(4):
    objs[f"ec22/{i}.bin"] = (os.urandom(300 * 1024 + i * 70000), dict(durability="ec", k=2, m=2))
objs["ec22/big.bin"] = (os.urandom(9 * MiB), dict(durability="ec", k=2, m=2))
for i in range(3):
    objs[f"ec21/{i}.bin"] = (os.urandom(500 * 1024), dict(durability="ec", k=2, m=1))

for key, (data, params) in objs.items():
    assert put(B, key, data, **params).status_code == 200, key


def manifest(key):
    return requests.get(f"{META}/admin/manifest/{B}/{key}").json()["chunks"]


def all_readable(prefixes=("",), consistency="quorum"):
    bad, degraded = [], 0
    for key, (data, _) in objs.items():
        if not key.startswith(tuple(prefixes)):
            continue
        g = get(B, key, headers={"X-DFS-Consistency": consistency})
        if g.status_code != 200 or g.content != data:
            bad.append(key)
        else:
            degraded += int(g.headers["X-DFS-Degraded-Chunks"])
    return bad, degraded


def fully_protected(prefixes, need):
    """Every chunk of the given objects has `need` distinct pieces on live nodes."""
    for key in objs:
        if key.startswith(prefixes):
            for c in manifest(key):
                live = {p["node"] if c["mode"] == "rep" else p["idx"] for p in c["pieces"] if p["alive"]}
                if len(live) < need:
                    return False
    return True


bad, degraded = all_readable()
check(f"all {len(objs)} objects read back correctly with every node up", not bad and degraded == 0, str(bad))
print("storage overhead by durability:", {m["mode"]: m["overhead"] for m in admin("stats")["by_durability"]})

# --- two of four nodes down at once: nothing is lost, reads are served degraded
for n in ("node1", "node2"):
    docker("stop", container(n))
bad, degraded = all_readable(("rep/", "ec22/"), consistency="any")  # both tolerate 2 lost nodes (3 copies / 2 parity shards)
check("two nodes down: every 3x replicated and EC 2+2 object is still readable (single-replica reads)", not bad, str(bad))
check("... some chunks were served in degraded mode (failover or decoding)", degraded > 0, str(degraded))
lost_ec21 = all_readable(("ec21/",))[0]
print(f"EC 2+1 tolerates only 1 failure by design: {len(lost_ec21)} of 3 such objects unavailable while 2 nodes are down")
for n in ("node1", "node2"):
    docker("start", container(n))
wait_cluster(4)
wait_for(lambda: under_protected() == 0, 60)

# --- one node fails: automatic detection and repair
victim = "node3"
t0 = time.time()
docker("stop", container(victim))
check("failed node is detected automatically from missing heartbeats", wait_for(lambda: not nodes()[victim]["alive"], 60, 1))
t_detect = time.time() - t0
check("automatic repair starts without any operator action",
      wait_for(lambda: any(e["kind"] in ("replicate", "reconstruct") for e in admin("repairs", limit=30)), 90, 2))
check("3x replicated and EC 2+1 chunks regain full protection on the 3 surviving nodes",
      wait_for(lambda: fully_protected(("rep/", "ec21/"), 3), 120, 3))
kinds = sorted({e["kind"] for e in admin("repairs", limit=500)})
print(f"failure detected after {t_detect:.0f}s; repair event types: {kinds}")
bad, _ = all_readable()
check("all objects read back correctly after repair (EC 2+2 still served from 3 of 4 shards)", not bad, str(bad))

# --- the failed node comes back: copies that were re-created elsewhere are removed from it
docker("start", container(victim))
wait_cluster(4)


def victim_consistent():
    requests.post(f"{META}/admin/gc")
    for key in objs:
        if key.startswith("rep/"):
            for c in manifest(key):
                if on_node(victim, c["hash"]) != (victim in {p["node"] for p in c["pieces"]}):
                    return False
    return True


check("returned node holds exactly the replicas the metadata lists (stale ones were garbage collected)",
      wait_for(victim_consistent, 90, 3))
wait_for(lambda: under_protected() == 0, 60)

# --- integrity: a corrupted copy is never served, and the scrubber finds silent corruption
rep_chunk = manifest("rep/1.bin")[0]
node, h = rep_chunk["pieces"][0]["node"], rep_chunk["hash"]
docker("exec", container(node), "sh", "-c", f"echo bitrot >> /data/chunks/{h[:2]}/{h[2:4]}/{h}")
check("a read of a corrupted copy is rejected by the node (409)",
      requests.get(f"http://localhost:{NODES[node]}/chunks/{h}").status_code == 409)
check("the object is still served correctly from another replica", get(B, "rep/1.bin").content == objs["rep/1.bin"][0])

ec_chunk = manifest("ec21/0.bin")[0]
piece = ec_chunk["pieces"][0]
loc = [l for l in admin(f"chunks/{ec_chunk['hash']}")["locations"] if l["node_id"] == piece["node"]][0]
s = loc["shard_hash"]
docker("exec", container(piece["node"]), "sh", "-c", f"echo bitrot >> /data/chunks/{s[:2]}/{s[2:4]}/{s}")  # silent: nobody read it
last_event = max([e["id"] for e in admin("repairs", limit=1)] or [0])
report = admin("scrub", method="post")
print("scrub report:", {k: v for k, v in report.items() if k != "repair"}, "| repair:", report["repair"])
check("scrub re-hashed every live node's disk", report["nodes_scrubbed"] == 4 and report["chunks_checked"] > 0, str(report))
check("scrub found the silently corrupted shard", report["corrupted_found"] >= 1, str(report))
check("scrub found the copy that was quarantined earlier as missing", report["missing_found"] >= 1, str(report))
# The corrupted EC shard must be reconstructed. The quarantined replica needs no new copy if the chunk still has
# 3 healthy ones (a repair during the two-nodes-down step can leave a 4th), so only require >= 1 rebuild event.
rebuilt = [e for e in admin("repairs", limit=500) if e["id"] > last_event and e["kind"] in ("replicate", "reconstruct")]
check("the corrupted erasure-coded shard was reconstructed on a healthy node",
      any(e["kind"] == "reconstruct" for e in rebuilt), str(rebuilt))
check("nothing is under-protected any more", under_protected() == 0)
bad, _ = all_readable()
check("everything reads back correctly after scrubbing and repair", not bad, str(bad))
ev = {e["kind"] for e in admin("repairs", limit=200)}
check("the repair log records scrub findings", {"scrub-corrupt", "scrub-missing"} <= ev, str(ev))

wipe_bucket(B)
requests.post(f"{META}/admin/gc")
finish()
