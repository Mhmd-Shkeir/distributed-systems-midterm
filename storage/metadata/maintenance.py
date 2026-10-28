"""Keeping data safe: garbage collection, repair, rebalancing and scrubbing.

Everything here works from one idea, `reconcile_chunk`: compare where a chunk's
copies (or erasure-coded shards) actually are with where the consistent-hash ring
says they should be, and fix the difference using only healthy sources.

* repair     - copies/shards were lost (node down, corruption): rebuild the missing
               ones on other nodes. Replication re-copies; erasure coding decodes
               from any k surviving shards and regenerates the lost shard.
* rebalance  - the set of ring members changed: move only the copies whose owner
               changed (about 1/N of them), then drop the old ones.
* scrub      - every node re-hashes what is on its disk; corrupted or missing
               copies are forgotten and then repaired like any other loss.
"""
import hashlib
import logging
import threading

from . import ec, nodes
from .db import lock_keys, log_event, pool
from .nodes import ring

log = logging.getLogger("metadata.maint")
_busy = threading.Lock()  # one repair/scrub/rebalance pass at a time


# ------------------------------------------------------------------ reference counting + GC

def queue_gc(conn, rows):
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO gc_queue (hash, node_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", list(rows))


def release_chunk(conn, h: str, count: int) -> bool:
    """Drop `count` references. At zero the chunk is forgotten and its copies queued for removal.

    The caller must hold the advisory locks for the chunk and its stored hashes.
    """
    left = conn.execute(
        "UPDATE chunks SET refcount = refcount - %s WHERE hash=%s RETURNING refcount", (count, h)
    ).fetchone()["refcount"]
    if left > 0:
        return False
    conn.execute(
        "INSERT INTO gc_queue (hash, node_id) SELECT shard_hash, node_id FROM chunk_locations "
        "WHERE chunk_hash=%s ON CONFLICT DO NOTHING", (h,))
    conn.execute("DELETE FROM chunks WHERE hash=%s", (h,))
    return True


def run_gc(limit: int = 500) -> int:
    """Delete node copies that no location row references any more."""
    with pool.connection() as conn:
        todo = conn.execute("SELECT hash, node_id FROM gc_queue LIMIT %s", (limit,)).fetchall()
    done = 0
    for item in todo:
        h, node_id = item["hash"], item["node_id"]
        try:
            with pool.connection() as conn:
                lock_keys(conn, [h])
                # It may have been stored again (same bytes) in the meantime.
                in_use = conn.execute(
                    "SELECT 1 FROM chunk_locations WHERE shard_hash=%s AND node_id=%s", (h, node_id)).fetchone()
                if not in_use:
                    row = conn.execute("SELECT address FROM nodes WHERE node_id=%s", (node_id,)).fetchone()
                    if row is None:
                        raise RuntimeError(f"unknown node {node_id}")
                    nodes.delete(row["address"], h)
                conn.execute("DELETE FROM gc_queue WHERE hash=%s AND node_id=%s", (h, node_id))
            done += 1
        except Exception as exc:  # stays queued (for example the node is down); retried next pass
            log.debug("gc of %s on %s deferred: %s", h[:12], node_id, exc)
    return done


# ------------------------------------------------------------------ reconcile one chunk

def _fetch_any_copy(locs, addr, h):
    for l in locs:
        data = nodes.get(addr[l["node_id"]], h)
        if data is not None:
            return data, l["node_id"]
    return None, None


def _drop_rows(conn, rows):
    for l in rows:
        conn.execute(
            "DELETE FROM chunk_locations WHERE chunk_hash=%s AND shard_idx=%s AND node_id=%s",
            (l["chunk_hash"], l["shard_idx"], l["node_id"]))
    queue_gc(conn, {(l["shard_hash"], l["node_id"]) for l in rows})


def _reconcile_rep(conn, c, locs, alive, addr, rebalance, dry_run):
    h, copies = c["hash"], c["copies"]
    healthy = [l for l in locs if l["node_id"] in alive]
    healthy_nodes = {l["node_id"] for l in healthy}
    dead = [l for l in locs if l["node_id"] not in alive]
    order = ring.lookup(h, max(len(ring.nodes), 1), accept=lambda x: x in alive)
    if rebalance:
        target = order[:copies]
        to_add = [x for x in target if x not in healthy_nodes]
    else:
        target = None
        to_add = [x for x in order if x not in healthy_nodes][: max(0, copies - len(healthy_nodes))]
    if dry_run:
        return {"planned": len(to_add), "bytes": len(to_add) * c["size"]}
    if not to_add and not (rebalance and any(n not in target for n in healthy_nodes)) and not (
            dead and len(healthy_nodes) >= copies):
        return {}
    out = {"copied": 0, "bytes": 0}
    placed = set()
    if to_add:
        data, src = _fetch_any_copy(healthy, addr, h)
        if data is None:
            log_event(conn, "unavailable", h, "no healthy copy could be read; cannot repair")
            return {}
        for x in to_add:
            try:
                nodes.put(alive[x], h, data)
            except Exception as exc:
                log.warning("repair put to %s failed: %s", x, exc)
                continue
            conn.execute(
                "INSERT INTO chunk_locations (chunk_hash, shard_idx, shard_hash, node_id, size) "
                "VALUES (%s, 0, %s, %s, %s) ON CONFLICT DO NOTHING", (h, h, x, len(data)))
            placed.add(x)
            out["copied"] += 1
            out["bytes"] += len(data)
            log_event(conn, "rebalance" if rebalance else "replicate", h, f"copy placed on {x} (read from {src})")
    now_healthy = healthy_nodes | placed
    if dead and len(now_healthy) >= copies:  # fully replicated again: forget the lost copies
        _drop_rows(conn, dead)
        log_event(conn, "forget-lost", h, "dropped copies on failed nodes: " + ", ".join(sorted({l['node_id'] for l in dead})))
    if rebalance:
        extras = [l for l in healthy if l["node_id"] not in target]
        if extras and len(now_healthy - {l["node_id"] for l in extras}) >= copies:
            _drop_rows(conn, extras)
            log_event(conn, "rebalance", h, "removed copy from " + ", ".join(l["node_id"] for l in extras))
    return out


def _reconcile_ec(conn, c, locs, alive, addr, rebalance, dry_run):
    h, k, m, size = c["hash"], c["k"], c["m"], c["size"]
    n = k + m
    healthy = {}
    for l in locs:
        if l["node_id"] in alive and l["shard_idx"] not in healthy:
            healthy[l["shard_idx"]] = l
    used = {l["node_id"] for l in healthy.values()}
    order = ring.lookup(h, max(len(ring.nodes), 1), accept=lambda x: x in alive)
    desired = order[:n]
    lost = [i for i in range(n) if i not in healthy]
    moves = [i for i, l in healthy.items() if rebalance and l["node_id"] not in desired] if rebalance else []
    free = [x for x in (desired if rebalance else order) if x not in used]
    shard_bytes = -(-size // k)
    planned = min(len(lost) + len(moves), len(free))
    if dry_run:
        return {"planned": planned, "bytes": planned * shard_bytes}
    if not planned:
        return {}
    out = {"copied": 0, "bytes": 0}
    shards = None
    if lost:
        if len(healthy) < k:
            log_event(conn, "unavailable", h, f"only {len(healthy)} of {n} shards survive (need {k}); cannot repair")
            return {}
        got = {}
        for idx in sorted(healthy):
            s = nodes.get(addr[healthy[idx]["node_id"]], healthy[idx]["shard_hash"])
            if s is not None:
                got[idx] = s
            if len(got) == k:
                break
        if len(got) < k:
            log_event(conn, "unavailable", h, "could not read enough verified shards to rebuild")
            return {}
        data = ec.decode(got, k, m, size)
        if hashlib.sha256(data).hexdigest() != h:
            log_event(conn, "repair-failed", h, "rebuilt chunk does not match its hash")
            return {}
        shards = ec.encode(data, k, m)
    for idx in lost:
        if not free:
            break
        x = free.pop(0)
        payload = shards[idx]
        shash = hashlib.sha256(payload).hexdigest()
        lock_keys(conn, [shash])
        try:
            nodes.put(alive[x], shash, payload)
        except Exception as exc:
            log.warning("repair put to %s failed: %s", x, exc)
            continue
        stale = [l for l in locs if l["shard_idx"] == idx]
        if stale:
            _drop_rows(conn, stale)
        conn.execute(
            "INSERT INTO chunk_locations (chunk_hash, shard_idx, shard_hash, node_id, size) VALUES (%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING", (h, idx, shash, x, len(payload)))
        out["copied"] += 1
        out["bytes"] += len(payload)
        log_event(conn, "reconstruct", h, f"shard {idx} rebuilt from {len(healthy)} survivors and placed on {x}")
    for idx in moves:
        if not free:
            break
        old = healthy[idx]
        x = free.pop(0)
        payload = nodes.get(addr[old["node_id"]], old["shard_hash"])
        if payload is None:
            continue
        lock_keys(conn, [old["shard_hash"]])
        try:
            nodes.put(alive[x], old["shard_hash"], payload)
        except Exception as exc:
            log.warning("rebalance put to %s failed: %s", x, exc)
            continue
        _drop_rows(conn, [old])
        conn.execute(
            "INSERT INTO chunk_locations (chunk_hash, shard_idx, shard_hash, node_id, size) VALUES (%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING", (h, idx, old["shard_hash"], x, len(payload)))
        out["copied"] += 1
        out["bytes"] += len(payload)
        log_event(conn, "rebalance", h, f"shard {idx} moved {old['node_id']} -> {x}")
    return out


def reconcile_chunk(h: str, rebalance: bool = False, dry_run: bool = False) -> dict:
    with pool.connection() as conn:
        pre = conn.execute("SELECT shard_hash FROM chunk_locations WHERE chunk_hash=%s", (h,)).fetchall()
        lock_keys(conn, [h] + [r["shard_hash"] for r in pre])
        c = conn.execute("SELECT * FROM chunks WHERE hash=%s", (h,)).fetchone()
        if not c:
            return {}
        locs = conn.execute("SELECT * FROM chunk_locations WHERE chunk_hash=%s", (h,)).fetchall()
        alive, addr = nodes.alive(conn), nodes.addresses(conn)
        fn = _reconcile_rep if c["mode"] == "rep" else _reconcile_ec
        return fn(conn, c, locs, alive, addr, rebalance, dry_run)


# ------------------------------------------------------------------ passes

NEEDS_REPAIR_SQL = """
SELECT c.hash FROM chunks c
LEFT JOIN chunk_locations l ON l.chunk_hash = c.hash AND l.node_id = ANY(%s)
GROUP BY c.hash, c.mode, c.copies, c.k, c.m
HAVING (c.mode = 'rep' AND count(DISTINCT l.node_id) < c.copies)
    OR (c.mode = 'ec'  AND count(DISTINCT l.shard_idx) < c.k + c.m)
LIMIT %s
"""


def under_protected(limit: int = 1000) -> list[str]:
    with pool.connection() as conn:
        alive = list(nodes.alive(conn))
        return [r["hash"] for r in conn.execute(NEEDS_REPAIR_SQL, (alive, limit)).fetchall()]


def repair_pass(limit: int = 500) -> dict:
    """Find chunks with fewer healthy copies/shards than required and rebuild them."""
    if not _busy.acquire(blocking=False):
        return {"skipped": "another maintenance pass is running"}
    try:
        todo = under_protected(limit)
        repaired = copies = nbytes = 0
        for h in todo:
            try:
                r = reconcile_chunk(h)
            except Exception as exc:
                log.warning("repair of %s failed: %s", h[:12], exc)
                continue
            if r.get("copied"):
                repaired += 1
                copies += r["copied"]
                nbytes += r["bytes"]
        run_gc()
        return {"under_protected": len(todo), "chunks_repaired": repaired, "copies_rebuilt": copies, "bytes_written": nbytes}
    finally:
        _busy.release()


def rebalance(execute: bool = True) -> dict:
    """Move only the copies whose ring owner changed. With execute=False just report the plan."""
    if not _busy.acquire(blocking=False):
        return {"skipped": "another maintenance pass is running"}
    try:
        with pool.connection() as conn:
            chunks = conn.execute("SELECT hash, mode, copies, k, m FROM chunks").fetchall()
        total = sum(c["copies"] if c["mode"] == "rep" else c["k"] + c["m"] for c in chunks)
        moved = nbytes = chunks_touched = 0
        for c in chunks:
            try:
                r = reconcile_chunk(c["hash"], rebalance=True, dry_run=not execute)
            except Exception as exc:
                log.warning("rebalance of %s failed: %s", c["hash"][:12], exc)
                continue
            n = r.get("planned" if not execute else "copied", 0)
            if n:
                chunks_touched += 1
                moved += n
                nbytes += r.get("bytes", 0)
        if execute:
            run_gc()
        return {
            "executed": execute, "chunks": len(chunks), "copies_total": total, "chunks_touched": chunks_touched,
            "copies_moved": moved, "fraction_moved": round(moved / total, 4) if total else 0.0, "bytes_moved": nbytes,
        }
    finally:
        _busy.release()


def scrub_pass() -> dict:
    """Ask every live node to re-hash its disk, forget corrupted/missing copies, then repair."""
    if not _busy.acquire(blocking=False):
        return {"skipped": "another maintenance pass is running"}
    try:
        with pool.connection() as conn:
            alive = nodes.alive(conn)
        report = {"nodes_scrubbed": 0, "chunks_checked": 0, "bytes_checked": 0, "corrupted_found": 0,
                  "missing_found": 0, "unreferenced_on_nodes": 0}
        for node_id, addr in sorted(alive.items()):
            try:
                with pool.connection() as conn:
                    started = conn.execute("SELECT now() AS t").fetchone()["t"]
                res = nodes.scrub(addr)
            except Exception as exc:
                log.warning("scrub of %s failed: %s", node_id, exc)
                continue
            present = set(res["hashes"])
            report["nodes_scrubbed"] += 1
            report["chunks_checked"] += res["checked"]
            report["bytes_checked"] += res["bytes"]
            with pool.connection() as conn:
                for bad in res["corrupted"]:
                    rows = conn.execute(
                        "DELETE FROM chunk_locations WHERE shard_hash=%s AND node_id=%s RETURNING chunk_hash",
                        (bad, node_id)).fetchall()
                    report["corrupted_found"] += 1
                    for r in rows:
                        log_event(conn, "scrub-corrupt", r["chunk_hash"], f"bad copy of {bad[:12]} on {node_id} quarantined")
                # Only locations recorded before the node listed its disk can be judged missing.
                missing = conn.execute(
                    "DELETE FROM chunk_locations WHERE node_id=%s AND created_at < %s AND NOT (shard_hash = ANY(%s)) "
                    "RETURNING chunk_hash, shard_hash", (node_id, started, list(present))).fetchall()
                for r in missing:
                    report["missing_found"] += 1
                    log_event(conn, "scrub-missing", r["chunk_hash"], f"copy {r['shard_hash'][:12]} not on {node_id}")
                known = {r["shard_hash"] for r in conn.execute(
                    "SELECT DISTINCT shard_hash FROM chunk_locations WHERE node_id=%s", (node_id,)).fetchall()}
                report["unreferenced_on_nodes"] += len(present - known)
    finally:
        _busy.release()
    report["repair"] = repair_pass()
    return report
