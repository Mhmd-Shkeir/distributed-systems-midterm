"""Shared helpers for the end-to-end tests (they need the cluster running)."""
import hashlib
import os
import subprocess
import sys
import time
import uuid

import requests

META = "http://localhost:8000"
NODES = {"node1": 8101, "node2": 8102, "node3": 8103, "node4": 8104, "node5": 8105}
MiB = 1024 * 1024
RUN = uuid.uuid4().hex[:6]

failures: list[str] = []


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def finish():
    print(f"\n{len(failures)} failure(s)")
    sys.exit(1 if failures else 0)


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def put(bucket, key, data, **params):
    return requests.put(f"{META}/{bucket}/{key}", params=params, data=data, timeout=300)


def get(bucket, key, **kw):
    return requests.get(f"{META}/{bucket}/{key}", timeout=300, **kw)


def delete(bucket, key):
    return requests.delete(f"{META}/{bucket}/{key}", timeout=60)


def admin(path, method="get", **params):
    return getattr(requests, method)(f"{META}/admin/{path}", params=params, timeout=600).json()


def nodes():
    return {n["node_id"]: n for n in requests.get(f"{META}/nodes", timeout=10).json()}


def node_health(name):
    return requests.get(f"http://localhost:{NODES[name]}/health", timeout=10).json()


def on_node(name, h):
    return requests.head(f"http://localhost:{NODES[name]}/chunks/{h}", timeout=10).status_code == 200


def container(name):
    return f"dfs-storage-{name}-1"


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def wait_for(cond, timeout=90, every=2):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return True
        except requests.RequestException:
            pass
        time.sleep(every)
    return False


def wait_cluster(n_alive=4):
    ok = wait_for(lambda: sum(x["alive"] for x in nodes().values()) >= n_alive, 90)
    check(f"{n_alive} storage nodes alive", ok)
    return ok


def under_protected():
    return admin("status")["under_protected_chunks"]


def wipe_bucket(bucket):
    """Delete every object in a bucket, then the bucket (cleanup between tests)."""
    while True:
        r = requests.get(f"{META}/{bucket}", params={"max-keys": 1000}, timeout=60)
        if r.status_code != 200:
            return
        keys = [c["key"] for c in r.json()["contents"]]
        if not keys:
            break
        for k in keys:
            delete(bucket, k)
    requests.delete(f"{META}/{bucket}", timeout=30)
