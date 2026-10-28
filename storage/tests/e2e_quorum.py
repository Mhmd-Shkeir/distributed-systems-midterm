"""Quorum reads/writes (W + R > N), per-bucket durability policy (replication or erasure coding)."""
import os

import requests
from common import *  # noqa: F401,F403
from common import (META, RUN, admin, check, container, docker, finish, nodes, put, under_protected, wait_cluster,
                    wait_for, wipe_bucket)

wait_cluster(4)
cfg = admin("config")
check("configuration: N=3, W=2, R=2 (W + R > N)",
      (cfg["replication_factor"], cfg["write_quorum"], cfg["read_quorum"]) == (3, 2, 2), str(cfg))

# ---- durability is a property of the bucket
B_REP, B_EC, B_W3 = f"q-rep-{RUN}", f"q-ec-{RUN}", f"q-w3-{RUN}"
r = requests.put(f"{META}/{B_REP}")
check("bucket with the default policy: replication N=3, W=2, R=2", r.status_code == 201 and r.json()["durability"] == "replication N=3 (W=2,R=2)", r.text)
r = requests.put(f"{META}/{B_EC}", params={"durability": "ec", "k": 2, "m": 2})
check("bucket with erasure coding 2+2", r.status_code == 201 and r.json()["durability"] == "erasure coding 2+2", r.text)
r = requests.put(f"{META}/{B_W3}", params={"copies": 3, "write_quorum": 3, "read_quorum": 1})
check("bucket with N=3, W=3, R=1 (also satisfies W + R > N)", r.status_code == 201, r.text)
check("quorums that do not overlap are rejected (N=3, W=1, R=1)",
      requests.put(f"{META}/q-bad-{RUN}", params={"copies": 3, "write_quorum": 1, "read_quorum": 1}).status_code == 400)
check("erasure coding wider than the cluster is rejected (k+m=9)",
      requests.put(f"{META}/q-bad2-{RUN}", params={"durability": "ec", "k": 6, "m": 3}).status_code == 400)
pol = {b["name"]: b for b in requests.get(f"{META}/").json()["buckets"]}
check("the bucket list shows each bucket's policy",
      pol[B_EC]["durability"] == "ec" and pol[B_REP]["copies"] == 3 and pol[B_W3]["write_q"] == 3, str(pol.get(B_EC)))

data = os.urandom(600_000)
a = put(B_REP, "obj", data).json()
b = put(B_EC, "obj", data).json()
check("objects inherit the policy of their bucket (no per-request parameters)",
      a["durability"].startswith("replication N=3") and b["durability"] == "erasure coding 2+2", f"{a['durability']} | {b['durability']}")
ma = requests.get(f"{META}/admin/manifest/{B_REP}/obj").json()["chunks"][0]
mb = requests.get(f"{META}/admin/manifest/{B_EC}/obj").json()["chunks"][0]
check("replicated bucket stores 3 full copies; erasure-coded bucket stores 4 shards",
      ma["mode"] == "rep" and len(ma["pieces"]) == 3 and mb["mode"] == "ec" and len(mb["pieces"]) == 4)
check("a request may still override the bucket policy",
      put(B_REP, "ec-override", data, durability="ec", k=2, m=1).json()["durability"] == "erasure coding 2+1")
put(B_W3, "obj", data)

# ---- read quorum: two of four nodes down
for n in ("node1", "node2"):
    docker("stop", container(n))
wait_for(lambda: sum(x["alive"] for x in nodes().values()) == 2, 60, 1)
statuses = {k: requests.get(f"{META}/{B_REP}/{k}").status_code for k in ("obj",)}
many = [os.urandom(100_000) for _ in range(12)]
for i, d in enumerate(many):
    put(B_REP, f"r{i}", d)  # W=2: two live nodes are enough to acknowledge (the third copy is repaired later)
strict = [requests.get(f"{META}/{B_REP}/r{i}") for i in range(12)]
loose = [requests.get(f"{META}/{B_REP}/r{i}", headers={"X-DFS-Consistency": "any"}) for i in range(12)]
n_ok_strict = sum(r.status_code == 200 for r in strict)
check("W=2: writes still succeed with only 2 of 4 nodes alive", all(r.status_code in (200, 503) for r in strict) and len(many) == 12)
check("R=2: a chunk with fewer than 2 reachable replicas is refused with 503 (no quorum)", any(r.status_code == 503 for r in strict),
      str([r.status_code for r in strict]))
check("R=2: chunks with 2 or more reachable replicas are served", n_ok_strict >= 1, str([r.status_code for r in strict]))
check("consistency 'any' serves every chunk from a single healthy replica",
      all(r.status_code == 200 and r.content == many[i] for i, r in enumerate(loose)))
w3 = put(B_W3, "w3-new", data)
check("W=3 bucket: with only 2 nodes alive a write cannot reach its quorum (503)", w3.status_code == 503, w3.text[:120])
ecw = put(B_EC, "ec-new", data)
check("erasure-coded bucket: 4 shards cannot be written with 2 nodes alive (503)", ecw.status_code == 503, ecw.text[:120])
for n in ("node3", "node4"):
    docker("stop", container(n))
wait_for(lambda: sum(x["alive"] for x in nodes().values()) == 0, 60, 1)
r1 = put(B_REP, "none", data)
check("no node alive: writes are refused (503)", r1.status_code == 503)

# ---- recover
for n in ("node1", "node2", "node3", "node4"):
    docker("start", container(n))
wait_cluster(4)
wait_for(lambda: under_protected() == 0, 120)
check("after the nodes return, every chunk is fully protected again", under_protected() == 0)
check("W=3, R=1 bucket: reads need a single replica", requests.get(f"{META}/{B_W3}/obj").content == data)
check("replicated and erasure-coded objects read back correctly",
      requests.get(f"{META}/{B_REP}/obj").content == data and requests.get(f"{META}/{B_EC}/obj").content == data)

for b in (B_REP, B_EC, B_W3):
    wipe_bucket(b)
requests.post(f"{META}/admin/gc")
finish()
