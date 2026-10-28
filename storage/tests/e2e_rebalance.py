"""Consistent hashing in practice: adding and removing a node moves only a fraction of the data."""
import os
import subprocess

import requests
from common import *  # noqa: F401,F403
from common import (META, RUN, admin, check, container, docker, finish, get, nodes, put, under_protected,
                    wait_cluster, wait_for, wipe_bucket)

COMPOSE = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
B = f"scale-{RUN}"
wait_cluster(4)
requests.put(f"{META}/{B}")

objs = {}
for i in range(40):
    objs[f"rep/{i}"] = (os.urandom(80_000 + i * 3000), dict(durability="replication", copies=3))
    objs[f"ec/{i}"] = (os.urandom(80_000 + i * 3000), dict(durability="ec", k=2, m=2))
for key, (data, params) in objs.items():
    assert put(B, key, data, **params).status_code == 200


def manifest(key):
    return requests.get(f"{META}/admin/manifest/{B}/{key}").json()["chunks"]


def all_ok():
    return all(get(B, k).content == d for k, (d, _) in objs.items())


def placement_matches_ring():
    for key in objs:
        for c in manifest(key):
            want = 3 if c["mode"] == "rep" else 4
            owners = requests.get(f"{META}/admin/ring/lookup/{c['hash']}", params={"n": want}).json()["nodes"]
            if sorted(p["node"] for p in c["pieces"]) != sorted(owners):
                return False
    return True


hashes = [c["hash"] for k in objs for c in manifest(k)]
total_pieces = sum(3 if k.startswith("rep") else 4 for k in objs)
check("80 objects stored; placement equals the ring's choice on 4 nodes", placement_matches_ring())
before = {n: v["stored_pieces"] for n, v in nodes().items()}
print("pieces per node with 4 nodes:", before)

# --- what a naive scheme would do: owner = hash mod number_of_nodes
changed = sum(1 for h in hashes if int(h, 16) % 4 != int(h, 16) % 5) / len(hashes)
print(f"naive 'hash mod N' placement would reassign {changed:.0%} of the chunks when going from 4 to 5 nodes")

# --- add a fifth node
subprocess.run(["docker", "compose", "-f", COMPOSE, "--profile", "scale", "up", "-d", "node5"], capture_output=True)
check("node5 starts, heartbeats and joins the hash ring",
      wait_for(lambda: "node5" in nodes() and nodes()["node5"]["alive"] and "node5" in admin("config")["ring_nodes"], 90))
check("data written before node5 existed is unaffected and readable", all_ok())

plan = admin("rebalance", method="post", dry_run="true")
print("rebalance plan:", plan)
check("plan: only a fraction of all copies has to move (consistent hashing, ideal is 1/5 = 20%)",
      0.10 <= plan["fraction_moved"] <= 0.32, str(plan))
check("... far less than modulo placement would move", plan["fraction_moved"] < changed / 2)

res = admin("rebalance", method="post")
print("rebalance result:", res)
check("executed rebalance moved what was planned", res["copies_moved"] == plan["copies_moved"], f"{res} vs {plan}")
after = {n: v["stored_pieces"] for n, v in nodes().items()}
print("pieces per node with 5 nodes:", after)
check("node5 now holds about a fifth of the data", 0.10 * total_pieces <= after["node5"] <= 0.30 * total_pieces, str(after))
check("every chunk sits exactly where the 5-node ring says", placement_matches_ring())
check("all objects read back correctly after rebalancing", all_ok())
check("a second rebalance has nothing left to do", admin("rebalance", method="post", dry_run="true")["copies_moved"] == 0)
check("no chunk is under-protected", under_protected() == 0)

# --- remove the node again
dec = admin("nodes/node5/decommission", method="post")
print("decommission:", {k: v for k, v in dec.items() if k != "rebalance"}, "| moved:", dec["rebalance"]["copies_moved"], "copies",
      f"({dec['rebalance']['fraction_moved']:.1%})")
check("decommission drains node5 completely", dec["pieces_left_on_node"] == 0 and dec["safe_to_remove"], str(dec))
check("only about node5's share of the data had to move", 0.10 <= dec["rebalance"]["fraction_moved"] <= 0.32, str(dec["rebalance"]))
check("all objects still read back correctly", all_ok())
check("placement is back to the 4-node ring", placement_matches_ring())
docker("stop", container("node5"))
check("node5 can be switched off without any data loss", all_ok())
check("node5 can be forgotten (no data left on it)", requests.delete(f"{META}/admin/nodes/node5").status_code == 204)
docker("rm", "-f", container("node5"))

wipe_bucket(B)
requests.post(f"{META}/admin/gc")
finish()
