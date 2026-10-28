"""Chunk-level deduplication with reference counting, and fixed-size vs content-defined chunking."""
import os

import requests
from common import *  # noqa: F401,F403
from common import (META, NODES, MiB, RUN, admin, check, delete, finish, get, node_health, on_node, put, sha,
                    wait_cluster, wipe_bucket)

B = f"dedup-{RUN}"
wait_cluster(4)
requests.put(f"{META}/{B}")


def total_pieces():
    return {n: node_health(n)["chunk_count"] for n in list(NODES)[:4]}


baseline = total_pieces()

# --- identical content stored once
data = os.urandom(9 * MiB + 12345)
r = put(B, "one/file.bin", data, chunk_size=4 * MiB).json()
check("9 MiB object splits into 3 chunks (fixed 4 MiB), all new", r["chunks"] == 3 and r["new_chunks"] == 3, str(r))
man = requests.get(f"{META}/admin/manifest/{B}/one/file.bin").json()
placed_ok = True
for c in man["chunks"]:
    owners = requests.get(f"{META}/admin/ring/lookup/{c['hash']}", params={"n": 3}).json()["nodes"]
    placed_ok &= sorted(p["node"] for p in c["pieces"]) == sorted(owners)
check("every chunk sits on the 3 nodes the consistent-hash ring chooses", placed_ok)

after_first = total_pieces()
r2 = put(B, "two/copy.bin", data, chunk_size=4 * MiB).json()
check("identical object: 0 new chunks, 100% deduplicated", r2["new_chunks"] == 0 and r2["bytes_deduplicated"] == len(data), str(r2))
check("no extra pieces were written to any node", total_pieces() == after_first)
h0 = man["chunks"][0]["hash"]
check("shared chunk has reference count 2", admin(f"chunks/{h0}")["refcount"] == 2)

delete(B, "one/file.bin")
check("deleting one object keeps the shared chunks (refcount 1)", admin(f"chunks/{h0}")["refcount"] == 1)
check("the other object still reads correctly", get(B, "two/copy.bin").content == data)
delete(B, "two/copy.bin")
check("deleting the last reference frees the chunk in metadata", requests.get(f"{META}/admin/chunks/{h0}").status_code == 404)
requests.post(f"{META}/admin/gc")
check("... and removes every copy from the storage nodes", not any(on_node(n, h0) for n in list(NODES)[:4]))
check("node piece counts are back to the starting values", total_pieces() == baseline, str(total_pieces()))

# --- the same chunk repeated inside one object
block = os.urandom(MiB)
r3 = put(B, "repeat.bin", block * 4, chunk_size=MiB).json()
check("one object with 4 identical chunks stores it once", r3["chunks"] == 4 and r3["unique_chunks"] == 1 and r3["new_chunks"] == 1, str(r3))
check("its reference count is 4", admin(f"chunks/{sha(block)}")["refcount"] == 4)
check("it still reads back correctly", get(B, "repeat.bin").content == block * 4)
delete(B, "repeat.bin")
check("deleting it frees the chunk", requests.get(f"{META}/admin/chunks/{sha(block)}").status_code == 404)

# --- dedup works across durability modes (first writer decides how the chunk is protected)
blk = os.urandom(2 * MiB)
a = put(B, "m1", blk, durability="ec", k=2, m=2).json()
b = put(B, "m2", blk, durability="replication", copies=3).json()
check("same chunk written with a different durability mode is still deduplicated", a["new_chunks"] == 1 and b["new_chunks"] == 0, f"{a} {b}")
delete(B, "m1"), delete(B, "m2")

# --- fixed-size vs content-defined chunking
print("\nComparison: fixed-size vs content-defined chunking (about 1 MiB average chunk, 24 MiB object)")
base = os.urandom(24 * MiB)
scenarios = {
    "insert 64 bytes at the front": b"X" * 64 + base,
    "overwrite 100 bytes in the middle": base[:12 * MiB] + os.urandom(100) + base[12 * MiB + 100:],
}
cfg = {"fixed": dict(chunking="fixed", chunk_size=1 * MiB, copies=2),
       "cdc": dict(chunking="cdc", cdc_min=256 * 1024, cdc_avg=1 * MiB, cdc_max=4 * MiB, copies=2)}
results = {}
for strat, params in cfg.items():
    put(B, f"{strat}-base", base, **params)
    for i, (label, edited) in enumerate(scenarios.items()):
        results[(strat, label)] = put(B, f"{strat}-edit{i}", edited, **params).json()
        delete(B, f"{strat}-edit{i}")
print(f"{'scenario':38} {'strategy':8} {'chunks':>6} {'new chunks':>10} {'new MiB':>8} {'deduplicated':>13}")
for label in scenarios:
    for strat in cfg:
        rr = results[(strat, label)]
        print(f"{label:38} {strat:8} {rr['chunks']:>6} {rr['new_chunks']:>10} {rr['bytes_new'] / MiB:>8.2f} {rr['bytes_deduplicated'] / rr['size']:>12.1%}")
print()
ins, mid = list(scenarios)
check("insertion: fixed-size reuses almost nothing (>90% rewritten)", results[("fixed", ins)]["bytes_new"] > 0.9 * len(base))
check("insertion: content-defined chunking reuses almost everything (<15% rewritten)", results[("cdc", ins)]["bytes_new"] < 0.15 * len(base))
check("in-place edit: both strategies rewrite only a few chunks (<25%)", all(results[(s, mid)]["bytes_new"] < 0.25 * len(base) for s in cfg))

st = admin("stats")
print("store stats:", {k: st[k] for k in ("objects", "logical_bytes", "unique_bytes", "raw_bytes", "dedup_ratio", "redundancy_overhead")})

for s in cfg:
    delete(B, f"{s}-base")
wipe_bucket(B)
requests.post(f"{META}/admin/gc")
check("after cleanup the nodes hold no pieces from this test", total_pieces() == baseline, str(total_pieces()))
finish()
