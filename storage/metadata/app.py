"""Metadata and placement service.

* Receives heartbeats from storage nodes (registers them on the hash ring).
* Splits uploaded objects into chunks (fixed-size or content-defined).
* Places each new chunk on the node(s) chosen by a consistent hash ring.
* Keeps object metadata, chunk reference counts and chunk locations in Postgres.
* Deduplicates at chunk level: a chunk that already exists is not stored again,
  only its reference count is incremented. A chunk is removed from the nodes
  once no object references it any more.
"""
import asyncio
import hashlib
import logging
import os
import random
import time
from collections import Counter
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from . import chunking
from .ring import HashRing

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://dfs:dfs@postgres:5432/dfs")
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", "1"))
VNODES = int(os.environ.get("VNODES", "128"))
DEAD_AFTER = float(os.environ.get("DEAD_AFTER", "15"))
MAX_OBJECT_BYTES = int(os.environ.get("MAX_OBJECT_BYTES", str(256 * 1024 * 1024)))
GC_INTERVAL = float(os.environ.get("GC_INTERVAL", "30"))

log = logging.getLogger("metadata")
logging.basicConfig(level=logging.INFO)

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id     text PRIMARY KEY,
    address     text NOT NULL,
    free_bytes  bigint NOT NULL,
    chunk_count integer NOT NULL,
    reported_at double precision NOT NULL,
    last_seen   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS objects (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    size        bigint NOT NULL,
    sha256      text NOT NULL,
    chunking    text NOT NULL,
    chunk_count integer NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS chunks (
    hash     text PRIMARY KEY,
    size     integer NOT NULL,
    refcount integer NOT NULL CHECK (refcount >= 0)
);
CREATE TABLE IF NOT EXISTS chunk_locations (
    hash    text NOT NULL REFERENCES chunks(hash) ON DELETE CASCADE,
    node_id text NOT NULL,
    PRIMARY KEY (hash, node_id)
);
CREATE TABLE IF NOT EXISTS object_chunks (
    object_id  bigint NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    seq        integer NOT NULL,
    chunk_hash text NOT NULL REFERENCES chunks(hash),
    PRIMARY KEY (object_id, seq)
);
CREATE INDEX IF NOT EXISTS object_chunks_hash ON object_chunks(chunk_hash);
-- Copies that must be removed from a node once no chunk row references them.
CREATE TABLE IF NOT EXISTS gc_queue (
    hash    text NOT NULL,
    node_id text NOT NULL,
    PRIMARY KEY (hash, node_id)
);
"""

pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=16, open=False, kwargs={"row_factory": dict_row})
http = httpx.Client(timeout=30.0)
ring = HashRing(VNODES)


def lock_hash(conn, chunk_hash: str):
    """Serialise upload / delete / GC of one chunk (released at commit)."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (chunk_hash,))


# ------------------------------------------------------------------ nodes

def node_put(addr: str, h: str, data: bytes):
    r = http.put(f"{addr}/chunks/{h}", content=data)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"PUT {addr} -> {r.status_code} {r.text}")


def node_get(addr: str, h: str) -> bytes | None:
    try:
        r = http.get(f"{addr}/chunks/{h}")
    except httpx.HTTPError:
        return None
    return r.content if r.status_code == 200 else None


def node_delete(addr: str, h: str):
    r = http.delete(f"{addr}/chunks/{h}")
    if r.status_code not in (204, 404):
        raise RuntimeError(f"DELETE {addr} -> {r.status_code}")


def alive_nodes(conn) -> dict[str, str]:
    rows = conn.execute(
        "SELECT node_id, address FROM nodes WHERE last_seen > now() - make_interval(secs => %s)",
        (DEAD_AFTER,),
    ).fetchall()
    return {r["node_id"]: r["address"] for r in rows}


# ------------------------------------------------------------------ garbage collection

def run_gc(limit: int = 500) -> int:
    """Delete node copies that no chunk row references any more."""
    with pool.connection() as conn:
        todo = conn.execute("SELECT hash, node_id FROM gc_queue LIMIT %s", (limit,)).fetchall()
    done = 0
    for item in todo:
        h, node_id = item["hash"], item["node_id"]
        try:
            with pool.connection() as conn:
                lock_hash(conn, h)
                # The chunk may have been uploaded again in the meantime.
                in_use = conn.execute(
                    "SELECT 1 FROM chunk_locations WHERE hash=%s AND node_id=%s", (h, node_id)
                ).fetchone()
                if not in_use:
                    node = conn.execute("SELECT address FROM nodes WHERE node_id=%s", (node_id,)).fetchone()
                    if node is None:
                        raise RuntimeError(f"unknown node {node_id}")
                    node_delete(node["address"], h)
                conn.execute("DELETE FROM gc_queue WHERE hash=%s AND node_id=%s", (h, node_id))
            done += 1
        except Exception as exc:  # leave it queued, retry on the next pass
            log.warning("gc of %s on %s failed: %s", h[:12], node_id, exc)
    return done


async def gc_loop():
    while True:
        await asyncio.sleep(GC_INTERVAL)
        try:
            await run_in_threadpool(run_gc)
        except Exception as exc:
            log.warning("gc pass failed: %s", exc)


# ------------------------------------------------------------------ app

@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open()
    for attempt in range(30):
        try:
            with pool.connection() as conn:
                conn.execute(SCHEMA)
                for r in conn.execute("SELECT node_id FROM nodes").fetchall():
                    ring.add_node(r["node_id"])
            break
        except Exception as exc:
            log.warning("waiting for database (%s)", exc)
            time.sleep(1)
    else:
        raise RuntimeError("database unavailable")
    task = asyncio.create_task(gc_loop())
    yield
    task.cancel()
    pool.close()


app = FastAPI(title="metadata-and-placement", lifespan=lifespan)


class Heartbeat(BaseModel):
    node_id: str
    address: str = ""
    free_bytes: int
    chunk_count: int
    timestamp: float


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    with pool.connection() as conn:
        conn.execute(
            """INSERT INTO nodes (node_id, address, free_bytes, chunk_count, reported_at, last_seen)
               VALUES (%s, %s, %s, %s, %s, now())
               ON CONFLICT (node_id) DO UPDATE SET address=EXCLUDED.address,
                 free_bytes=EXCLUDED.free_bytes, chunk_count=EXCLUDED.chunk_count,
                 reported_at=EXCLUDED.reported_at, last_seen=now()""",
            (hb.node_id, hb.address, hb.free_bytes, hb.chunk_count, hb.timestamp),
        )
    if ring.add_node(hb.node_id):
        log.info("node %s joined the ring", hb.node_id)
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT node_id, address, free_bytes, chunk_count, reported_at AS timestamp, "
            "extract(epoch FROM now() - last_seen) AS age FROM nodes ORDER BY node_id"
        ).fetchall()
    return [
        {
            "node_id": r["node_id"], "address": r["address"], "free_bytes": r["free_bytes"],
            "chunk_count": r["chunk_count"], "timestamp": r["timestamp"],
            "age_seconds": round(r["age"], 1), "alive": r["age"] < DEAD_AFTER,
        }
        for r in rows
    ]


# ------------------------------------------------------------------ objects

def store_object(name: str, data: bytes, strategy: str, params: dict) -> dict:
    try:
        spans = chunking.split(data, strategy, **params)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    pieces = [data[s:e] for s, e in spans]
    hashes = [hashlib.sha256(p).hexdigest() for p in pieces]
    by_hash = dict(zip(hashes, pieces))
    counts = Counter(hashes)
    object_sha = hashlib.sha256(data).hexdigest()

    placed: list[tuple[str, str]] = []  # (hash, node) copies written in this attempt
    new_bytes = 0
    try:
        with pool.connection() as conn:
            if conn.execute("SELECT 1 FROM objects WHERE name=%s", (name,)).fetchone():
                raise HTTPException(409, f"object '{name}' already exists")
            alive = alive_nodes(conn)
            if not alive:
                raise HTTPException(503, "no storage nodes are alive")

            new_chunks = 0
            for h in sorted(counts):  # fixed lock order avoids deadlocks
                lock_hash(conn, h)
                row = conn.execute(
                    """INSERT INTO chunks (hash, size, refcount) VALUES (%s, %s, %s)
                       ON CONFLICT (hash) DO UPDATE SET refcount = chunks.refcount + EXCLUDED.refcount
                       RETURNING (xmax = 0) AS inserted""",
                    (h, len(by_hash[h]), counts[h]),
                ).fetchone()
                if not row["inserted"]:
                    continue  # deduplicated: only the reference count changed
                targets = ring.lookup(h, REPLICATION_FACTOR, accept=lambda n: n in alive)
                if not targets:
                    raise HTTPException(503, "no storage nodes are alive")
                for node_id in targets:
                    try:
                        node_put(alive[node_id], h, by_hash[h])
                    except Exception as exc:
                        raise HTTPException(502, f"storing chunk on {node_id} failed: {exc}")
                    placed.append((h, node_id))
                    conn.execute("INSERT INTO chunk_locations (hash, node_id) VALUES (%s, %s)", (h, node_id))
                new_chunks += 1
                new_bytes += len(by_hash[h])

            obj = conn.execute(
                "INSERT INTO objects (name, size, sha256, chunking, chunk_count) VALUES (%s,%s,%s,%s,%s) RETURNING id",
                (name, len(data), object_sha, strategy, len(hashes)),
            ).fetchone()
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO object_chunks (object_id, seq, chunk_hash) VALUES (%s, %s, %s)",
                    [(obj["id"], i, h) for i, h in enumerate(hashes)],
                )
    except Exception:
        if placed:  # the transaction rolled back: queue the copies for removal
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO gc_queue (hash, node_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", placed
                    )
            run_gc()
        raise

    return {
        "name": name, "size": len(data), "sha256": object_sha, "chunking": strategy,
        "chunks": len(hashes), "unique_chunks": len(counts), "new_chunks": new_chunks,
        "bytes_new": new_bytes, "bytes_deduplicated": len(data) - new_bytes,
    }


@app.put("/objects/{name:path}", status_code=201)
async def put_object(
    name: str,
    request: Request,
    chunking_strategy: str = Query("fixed", alias="chunking", pattern="^(fixed|cdc)$"),
    chunk_size: int = Query(4 * 1024 * 1024, ge=1024, description="fixed-size chunk bytes"),
    cdc_min: int = Query(256 * 1024, ge=64),
    cdc_avg: int = Query(1024 * 1024, ge=256),
    cdc_max: int = Query(4 * 1024 * 1024, ge=1024),
):
    declared = request.headers.get("content-length")
    if declared and int(declared) > MAX_OBJECT_BYTES:
        raise HTTPException(413, "object too large")
    data = await request.body()
    if len(data) > MAX_OBJECT_BYTES:
        raise HTTPException(413, "object too large")
    if not data:
        raise HTTPException(400, "empty object")
    params = {"chunk_size": chunk_size, "min_size": cdc_min, "avg_size": cdc_avg, "max_size": cdc_max}
    return await run_in_threadpool(store_object, name, data, chunking_strategy, params)


def load_manifest(name: str) -> tuple[dict, list[dict]]:
    with pool.connection() as conn:
        obj = conn.execute("SELECT * FROM objects WHERE name=%s", (name,)).fetchone()
        if not obj:
            raise HTTPException(404, "object not found")
        rows = conn.execute(
            """SELECT oc.seq, oc.chunk_hash AS hash, c.size, c.refcount,
                      coalesce(array_agg(n.node_id || '|' || n.address) FILTER (WHERE n.node_id IS NOT NULL), '{}') AS nodes
               FROM object_chunks oc
               JOIN chunks c ON c.hash = oc.chunk_hash
               LEFT JOIN chunk_locations l ON l.hash = c.hash
               LEFT JOIN nodes n ON n.node_id = l.node_id
               WHERE oc.object_id = %s GROUP BY oc.seq, oc.chunk_hash, c.size, c.refcount ORDER BY oc.seq""",
            (obj["id"],),
        ).fetchall()
    return obj, rows


@app.get("/objects/{name:path}/manifest")
def manifest(name: str):
    obj, rows = load_manifest(name)
    return {
        "name": obj["name"], "size": obj["size"], "sha256": obj["sha256"], "chunking": obj["chunking"],
        "chunks": [
            {"seq": r["seq"], "hash": r["hash"], "size": r["size"], "refcount": r["refcount"],
             "nodes": [x.split("|")[0] for x in r["nodes"]]}
            for r in rows
        ],
    }


def read_object(name: str) -> tuple[dict, bytes]:
    obj, rows = load_manifest(name)
    parts = []
    for r in rows:
        data = None
        for entry in r["nodes"]:
            _, addr = entry.split("|", 1)
            data = node_get(addr, r["hash"])
            if data is not None and hashlib.sha256(data).hexdigest() == r["hash"]:
                break
            data = None
        if data is None:
            raise HTTPException(502, f"chunk {r['seq']} ({r['hash'][:12]}) is unavailable")
        parts.append(data)
    return obj, b"".join(parts)


@app.get("/objects/{name:path}")
def get_object(name: str):
    obj, data = read_object(name)
    return Response(content=data, media_type="application/octet-stream", headers={"X-Object-SHA256": obj["sha256"]})


@app.head("/objects/{name:path}")
def head_object(name: str):
    with pool.connection() as conn:
        obj = conn.execute("SELECT size, sha256 FROM objects WHERE name=%s", (name,)).fetchone()
    if not obj:
        return Response(status_code=404)
    return Response(status_code=200, headers={"Content-Length": str(obj["size"]), "X-Object-SHA256": obj["sha256"]})


@app.delete("/objects/{name:path}")
def delete_object(name: str):
    freed = []
    with pool.connection() as conn:
        obj = conn.execute("SELECT id FROM objects WHERE name=%s FOR UPDATE", (name,)).fetchone()
        if not obj:
            raise HTTPException(404, "object not found")
        refs = conn.execute(
            "SELECT chunk_hash, count(*) AS k FROM object_chunks WHERE object_id=%s GROUP BY chunk_hash ORDER BY chunk_hash",
            (obj["id"],),
        ).fetchall()
        conn.execute("DELETE FROM objects WHERE id=%s", (obj["id"],))
        for r in refs:
            h = r["chunk_hash"]
            lock_hash(conn, h)
            left = conn.execute(
                "UPDATE chunks SET refcount = refcount - %s WHERE hash=%s RETURNING refcount", (r["k"], h)
            ).fetchone()["refcount"]
            if left > 0:
                continue  # another object (or another part of this one) still references it
            # Last reference gone: forget the chunk and queue its copies for removal.
            conn.execute(
                "INSERT INTO gc_queue (hash, node_id) SELECT hash, node_id FROM chunk_locations WHERE hash=%s "
                "ON CONFLICT DO NOTHING", (h,))
            conn.execute("DELETE FROM chunks WHERE hash=%s", (h,))
            freed.append(h)
    run_gc()  # after commit; anything that fails stays queued for the background pass
    return {"deleted": name, "chunks_freed": len(freed)}


@app.get("/objects")
def list_objects():
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT name, size, chunking, chunk_count, created_at FROM objects ORDER BY name"
        ).fetchall()
    return rows


# ------------------------------------------------------------------ chunks, ring, stats

@app.get("/chunks/{chunk_hash}")
def chunk_info(chunk_hash: str):
    with pool.connection() as conn:
        c = conn.execute("SELECT hash, size, refcount FROM chunks WHERE hash=%s", (chunk_hash,)).fetchone()
        if not c:
            raise HTTPException(404, "chunk not found")
        c["nodes"] = [r["node_id"] for r in conn.execute(
            "SELECT node_id FROM chunk_locations WHERE hash=%s ORDER BY node_id", (chunk_hash,)).fetchall()]
    return c


@app.get("/ring")
def ring_info():
    return {"vnodes_per_node": ring.vnodes, "replication_factor": REPLICATION_FACTOR, "nodes": ring.nodes}


@app.get("/ring/lookup/{chunk_hash}")
def ring_lookup(chunk_hash: str, n: int = Query(1, ge=1, le=16)):
    if len(chunk_hash) < 16:
        raise HTTPException(400, "need a hex chunk hash")
    try:
        return {"chunk": chunk_hash, "nodes": ring.lookup(chunk_hash, n)}
    except ValueError:
        raise HTTPException(400, "need a hex chunk hash")


@app.get("/ring/distribution")
def ring_distribution(samples: int = Query(100000, ge=100, le=1000000)):
    counts: Counter = Counter()
    rnd = random.Random(1)
    for _ in range(samples):
        counts[ring.lookup(f"{rnd.getrandbits(64):016x}")[0]] += 1
    return {n: {"keys": counts[n], "share": round(counts[n] / samples, 4)} for n in ring.nodes}


@app.get("/stats")
def stats():
    with pool.connection() as conn:
        o = conn.execute("SELECT count(*) AS objects, coalesce(sum(size),0) AS logical_bytes, "
                         "coalesce(sum(chunk_count),0) AS chunk_refs FROM objects").fetchone()
        c = conn.execute("SELECT count(*) AS unique_chunks, coalesce(sum(size),0) AS unique_bytes FROM chunks").fetchone()
    return {
        **o, **c,
        "dedup_ratio": round(o["logical_bytes"] / c["unique_bytes"], 3) if c["unique_bytes"] else None,
        "bytes_saved": int(o["logical_bytes"]) - int(c["unique_bytes"]),
    }
