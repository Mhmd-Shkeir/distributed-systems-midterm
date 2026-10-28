"""End-to-end checks for the metadata and placement module (needs the cluster running)."""
import hashlib
import os
import sys
import time
import uuid

import requests

META = "http://localhost:8000"
NODE_PORTS = {"node1": 8101, "node2": 8102, "node3": 8103, "node4": 8104}
MiB = 1024 * 1024
RUN = uuid.uuid4().hex[:8]
failures = []
created = []


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def put(name, data, **params):
    created.append(name)
    return requests.put(f"{META}/objects/{name}", params=params, data=data, timeout=300)


def node_chunks():
    return {n: requests.get(f"http://localhost:{p}/health", timeout=10).json()["chunk_count"]
            for n, p in NODE_PORTS.items()}


def on_node(node, h):
    return requests.head(f"http://localhost:{NODE_PORTS[node]}/chunks/{h}", timeout=10).status_code == 200


def sha(b):
    return hashlib.sha256(b).hexdigest()


# --- wait for the cluster
for _ in range(30):
    try:
        if sum(n["alive"] for n in requests.get(f"{META}/nodes", timeout=5).json()) == 4:
            break
    except requests.RequestException:
        pass
    time.sleep(2)
check("4 storage nodes alive and registered", sum(n["alive"] for n in requests.get(f"{META}/nodes").json()) == 4)
baseline = node_chunks()

# --- consistent hashing ring
ring = requests.get(f"{META}/ring").json()
check("ring contains the 4 nodes with 128 virtual nodes each",
      ring["nodes"] == ["node1", "node2", "node3", "node4"] and ring["vnodes_per_node"] == 128, str(ring))
dist = requests.get(f"{META}/ring/distribution", params={"samples": 100000}).json()
shares = [v["share"] for v in dist.values()]
check("ring spreads keys evenly (each node 20-30%)", all(0.20 < s < 0.30 for s in shares), str(dist))
probe = sha(b"probe")
a, b = (requests.get(f"{META}/ring/lookup/{probe}", params={"n": 2}).json()["nodes"] for _ in range(2))
check("ring lookup is deterministic and returns distinct replicas", a == b and len(set(a)) == 2, str(a))

# --- fixed-size chunking, placement, round trip
data = os.urandom(9 * MiB + 12345)
r = put(f"{RUN}/fixed.bin", data, chunking="fixed", chunk_size=4 * MiB)
body = r.json()
check("upload with fixed 4 MiB chunks -> 201", r.status_code == 201, r.text)
check("9 MiB object split into 3 chunks, all new", body["chunks"] == 3 and body["new_chunks"] == 3, str(body))
d = requests.get(f"{META}/objects/{RUN}/fixed.bin")
check("download returns identical bytes", d.status_code == 200 and d.content == data)
check("HEAD reports size", requests.head(f"{META}/objects/{RUN}/fixed.bin").headers.get("Content-Length") == str(len(data)))

man = requests.get(f"{META}/objects/{RUN}/fixed.bin/manifest").json()
ok_place = True
for c in man["chunks"]:
    owner = requests.get(f"{META}/ring/lookup/{c['hash']}").json()["nodes"]
    holders = [n for n in NODE_PORTS if on_node(n, c["hash"])]
    ok_place &= c["nodes"] == owner == holders
check("every chunk is stored on exactly the node the ring chooses", ok_place, str(man["chunks"]))

# --- deduplication with reference counting
before = node_chunks()
r2 = put(f"{RUN}/copy.bin", data, chunking="fixed", chunk_size=4 * MiB).json()
check("identical object: 0 new chunks, everything deduplicated",
      r2["new_chunks"] == 0 and r2["bytes_deduplicated"] == len(data), str(r2))
check("no extra copies appeared on the nodes", node_chunks() == before)
h0 = man["chunks"][0]["hash"]
check("shared chunk has reference count 2", requests.get(f"{META}/chunks/{h0}").json()["refcount"] == 2)

requests.delete(f"{META}/objects/{RUN}/fixed.bin")
check("after deleting one object the shared chunks remain (refcount 1)",
      requests.get(f"{META}/chunks/{h0}").json()["refcount"] == 1 and on_node(man["chunks"][0]["nodes"][0], h0))
check("the other object still downloads correctly", requests.get(f"{META}/objects/{RUN}/copy.bin").content == data)
requests.delete(f"{META}/objects/{RUN}/copy.bin")
check("after deleting the last reference the chunk is removed from metadata",
      requests.get(f"{META}/chunks/{h0}").status_code == 404)
check("... and from the storage node", not on_node(man["chunks"][0]["nodes"][0], h0))
check("node chunk counts are back to the starting values", node_chunks() == baseline, str(node_chunks()))

# repeated identical chunks inside one object
block = os.urandom(MiB)
r3 = put(f"{RUN}/repeat.bin", block * 3, chunking="fixed", chunk_size=MiB).json()
hb = sha(block)
check("one object with 3 identical chunks stores it once with refcount 3",
      r3["chunks"] == 3 and r3["unique_chunks"] == 1 and r3["new_chunks"] == 1
      and requests.get(f"{META}/chunks/{hb}").json()["refcount"] == 3, str(r3))
check("repeated-chunk object downloads correctly", requests.get(f"{META}/objects/{RUN}/repeat.bin").content == block * 3)
requests.delete(f"{META}/objects/{RUN}/repeat.bin")
check("deleting it frees the chunk", requests.get(f"{META}/chunks/{hb}").status_code == 404)

# --- error handling
check("uploading an existing name -> 409", put(f"{RUN}/dup", b"x" * 2048).status_code == 201
      and put(f"{RUN}/dup", b"y" * 2048).status_code == 409)
created.pop()  # second dup attempt created nothing
requests.delete(f"{META}/objects/{RUN}/dup")
check("missing object -> 404", requests.get(f"{META}/objects/{RUN}/nope").status_code == 404)

# --- fixed-size vs content-defined chunking
print("\nComparison: fixed-size vs content-defined chunking (average chunk about 1 MiB, 24 MiB object)")
base = os.urandom(24 * MiB)
scenarios = {
    "insert 64 bytes at the front": b"X" * 64 + base,
    "overwrite 100 bytes in the middle": base[:12 * MiB] + os.urandom(100) + base[12 * MiB + 100:],
}
cfg = {
    "fixed": dict(chunking="fixed", chunk_size=1 * MiB),
    "cdc": dict(chunking="cdc", cdc_min=256 * 1024, cdc_avg=1 * MiB, cdc_max=4 * MiB),
}
results = {}
for strat, params in cfg.items():
    put(f"{RUN}/{strat}-base", base, **params)
    for i, (label, edited) in enumerate(scenarios.items()):
        name = f"{RUN}/{strat}-edit{i}"
        results[(strat, label)] = put(name, edited, **params).json()
        requests.delete(f"{META}/objects/{name}")  # keep the base object for the next scenario
    # each strategy's base object is deleted at the end

print(f"{'scenario':38} {'strategy':8} {'chunks':>6} {'new chunks':>10} {'new MiB':>8} {'deduplicated':>13}")
for label in scenarios:
    for strat in cfg:
        rr = results[(strat, label)]
        print(f"{label:38} {strat:8} {rr['chunks']:>6} {rr['new_chunks']:>10} {rr['bytes_new'] / MiB:>8.2f} "
              f"{rr['bytes_deduplicated'] / rr['size']:>12.1%}")
print()
ins = "insert 64 bytes at the front"
mid = "overwrite 100 bytes in the middle"
check("insertion: fixed-size finds almost no duplicates (>90% rewritten)",
      results[("fixed", ins)]["bytes_new"] > 0.9 * len(base))
check("insertion: content-defined chunking reuses almost everything (<15% rewritten)",
      results[("cdc", ins)]["bytes_new"] < 0.15 * len(base))
check("in-place edit: both strategies rewrite only a few chunks (<25%)",
      all(results[(s, mid)]["bytes_new"] < 0.25 * len(base) for s in cfg))

st = requests.get(f"{META}/stats").json()
print("store stats:", st)
check("stats report the deduplicated bytes", st["bytes_saved"] >= 0 and st["unique_chunks"] > 0)

# --- clean up and confirm nothing leaked
for name in list(created):
    requests.delete(f"{META}/objects/{name}")
for strat in cfg:
    requests.delete(f"{META}/objects/{RUN}/{strat}-base")
time.sleep(1)
check("after cleanup the nodes hold no chunks from this run", node_chunks() == baseline, str(node_chunks()))

print(f"\n{len(failures)} failure(s)")
sys.exit(1 if failures else 0)
