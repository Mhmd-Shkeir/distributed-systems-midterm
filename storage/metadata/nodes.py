"""Talking to the storage nodes, and the cluster membership view."""
import contextlib
import hashlib
import logging
import threading
from collections import defaultdict

import httpx

from .config import DEAD_AFTER, VNODES
from .ring import HashRing

log = logging.getLogger("metadata.nodes")
http = httpx.Client(timeout=60.0, limits=httpx.Limits(max_connections=64))
ring = HashRing(VNODES)  # nodes that take part in placement (not decommissioned)

# ---- network traffic between the metadata/gateway service and the storage nodes, by purpose
_ctx = threading.local()
_traffic: dict[str, dict[str, int]] = defaultdict(lambda: {"bytes_read": 0, "bytes_written": 0, "ops": 0})
_traffic_lock = threading.Lock()


@contextlib.contextmanager
def purpose(name: str):
    """Tag the node traffic generated inside the block (client, repair, rebalance, scrub, gc)."""
    old = getattr(_ctx, "name", "client")
    _ctx.name = name
    try:
        yield
    finally:
        _ctx.name = old


def _count(read: int = 0, written: int = 0):
    name = getattr(_ctx, "name", "client")
    with _traffic_lock:
        t = _traffic[name]
        t["bytes_read"] += read
        t["bytes_written"] += written
        t["ops"] += 1


def traffic(reset: bool = False) -> dict:
    with _traffic_lock:
        snap = {k: dict(v) for k, v in _traffic.items()}
        if reset:
            _traffic.clear()
    return snap


# ---- node operations
def put(addr: str, h: str, data: bytes):
    r = http.put(f"{addr}/chunks/{h}", content=data)
    _count(written=len(data))
    if r.status_code not in (200, 201):
        raise RuntimeError(f"PUT {addr} -> {r.status_code} {r.text}")


def get(addr: str, h: str) -> bytes | None:
    """Fetch and verify; None if the node is unreachable, lacks it, or it is corrupt."""
    try:
        r = http.get(f"{addr}/chunks/{h}")
    except httpx.HTTPError:
        return None
    _count(read=len(r.content))
    if r.status_code != 200 or hashlib.sha256(r.content).hexdigest() != h:
        return None
    return r.content


def head(addr: str, h: str) -> bool:
    """True if the node holds the chunk (cheap existence check, no data transferred)."""
    try:
        r = http.head(f"{addr}/chunks/{h}")
    except httpx.HTTPError:
        return False
    _count()
    return r.status_code == 200


def delete(addr: str, h: str):
    r = http.delete(f"{addr}/chunks/{h}")
    _count()
    if r.status_code not in (204, 404):
        raise RuntimeError(f"DELETE {addr} -> {r.status_code}")


def scrub(addr: str) -> dict:
    r = http.post(f"{addr}/scrub", timeout=600.0)
    r.raise_for_status()
    return r.json()


def alive(conn) -> dict[str, str]:
    """node_id -> address for every node that heartbeated recently."""
    rows = conn.execute(
        "SELECT node_id, address FROM nodes WHERE last_seen > now() - make_interval(secs => %s)", (DEAD_AFTER,)
    ).fetchall()
    return {r["node_id"]: r["address"] for r in rows}


def addresses(conn) -> dict[str, str]:
    return {r["node_id"]: r["address"] for r in conn.execute("SELECT node_id, address FROM nodes").fetchall()}


def load_ring(conn):
    for r in conn.execute("SELECT node_id FROM nodes WHERE in_ring").fetchall():
        ring.add_node(r["node_id"])
