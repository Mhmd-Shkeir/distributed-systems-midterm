"""End-to-end checks against the running storage cluster (see docker-compose.yml)."""
import hashlib
import subprocess
import sys
import time

import requests

NODES = {f"node{i}": f"http://localhost:{8100 + i}" for i in range(1, 5)}
META = "http://localhost:8000"
failures = []


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        failures.append(name)


data = b"hello distributed systems " * 1000
h = hashlib.sha256(data).hexdigest()

for node, url in NODES.items():
    c = f"{url}/chunks/{h}"
    check(f"{node} HEAD missing -> 404", requests.head(c).status_code == 404)
    r = requests.put(c, data=data)
    check(f"{node} PUT -> 201", r.status_code == 201, r.text)
    check(f"{node} PUT again is idempotent -> 200", requests.put(c, data=data).status_code == 200)
    r = requests.head(c)
    check(f"{node} HEAD -> 200 with size", r.status_code == 200 and r.headers.get("Content-Length") == str(len(data)))
    r = requests.get(c)
    check(f"{node} GET returns identical bytes", r.status_code == 200 and r.content == data)

# PUT with a wrong id must be rejected.
r = requests.put(f"{NODES['node1']}/chunks/{'0' * 64}", data=data)
check("PUT with mismatched hash -> 400", r.status_code == 400, r.text)
check("PUT with malformed id -> 400", requests.put(f"{NODES['node1']}/chunks/nothex", data=b"x").status_code == 400)

# Corrupt the stored bytes on node2's disk, then read must be rejected.
path = f"/data/chunks/{h[:2]}/{h[2:4]}/{h}"
subprocess.run(["docker", "exec", "dfs-storage-node2-1", "sh", "-c", f"echo tampered >> {path}"], check=True)
r = requests.get(f"{NODES['node2']}/chunks/{h}")
check("GET of corrupted chunk -> 409", r.status_code == 409, f"{r.status_code} {r.text}")
check("corrupted chunk is no longer served (HEAD 404)", requests.head(f"{NODES['node2']}/chunks/{h}").status_code == 404)

# Delete.
r = requests.delete(f"{NODES['node1']}/chunks/{h}")
check("DELETE -> 204", r.status_code == 204)
check("GET after DELETE -> 404", requests.get(f"{NODES['node1']}/chunks/{h}").status_code == 404)
check("DELETE missing -> 404", requests.delete(f"{NODES['node1']}/chunks/{h}").status_code == 404)

# Heartbeats: all four nodes should be alive and report counts.
time.sleep(6)
nodes = {n["node_id"]: n for n in requests.get(f"{META}/nodes").json()}
check("metadata sees 4 nodes", sorted(nodes) == ["node1", "node2", "node3", "node4"], str(sorted(nodes)))
check("all nodes alive", all(n["alive"] for n in nodes.values()))
check("heartbeat reports free space", all(n["free_bytes"] > 0 for n in nodes.values()))
check("heartbeat chunk counts correct (node1=0,node2=0,node3=1,node4=1)",
      [nodes[f"node{i}"]["chunk_count"] for i in range(1, 5)] == [0, 0, 1, 1],
      str({k: v["chunk_count"] for k, v in nodes.items()}))

print(f"\n{len(failures)} failure(s)")
sys.exit(1 if failures else 0)
