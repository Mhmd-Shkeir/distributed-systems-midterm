"""Metadata, placement and gateway service with an S3-like REST API.

S3-style (path-style) endpoints: buckets, PUT/GET/HEAD/DELETE object, ListObjectsV2
(prefix, delimiter, max-keys, continuation-token), Range reads, ETag, user metadata
(x-amz-meta-*). Responses are JSON rather than XML and there is no request signing.
Extra: /search, and /admin/* for cluster status, repair, scrub and rebalancing.
"""
import asyncio
import json
from datetime import datetime
import logging
import random
import re
import time
from collections import Counter
from contextlib import asynccontextmanager
from email.utils import format_datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from . import config, maintenance, multipart, nodes, store
from .db import init_schema, pool
from .nodes import ring

log = logging.getLogger("metadata")
logging.basicConfig(level=logging.INFO)

RESERVED = {"admin", "nodes", "heartbeat", "search", "docs", "redoc", "openapi.json", "ui", "cluster"}
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


async def periodic(fn, interval: float, name: str):
    while True:
        await asyncio.sleep(interval)
        try:
            await run_in_threadpool(fn)
        except Exception as exc:
            log.warning("%s pass failed: %s", name, exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if config.WRITE_QUORUM + config.READ_QUORUM <= config.REPLICATION_FACTOR:
        raise RuntimeError(f"invalid quorums: W + R must be > N ({config.WRITE_QUORUM} + {config.READ_QUORUM} <= {config.REPLICATION_FACTOR})")
    pool.open()
    for _attempt in range(30):
        try:
            init_schema()
            with pool.connection() as conn:
                nodes.load_ring(conn)
            break
        except Exception as exc:
            log.warning("waiting for database (%s)", exc)
            time.sleep(1)
    else:
        raise RuntimeError("database unavailable")
    tasks = [
        asyncio.create_task(periodic(maintenance.run_gc, config.GC_INTERVAL, "gc")),
        asyncio.create_task(periodic(maintenance.repair_pass, config.REPAIR_INTERVAL, "repair")),
        asyncio.create_task(periodic(maintenance.scrub_pass, config.SCRUB_INTERVAL, "scrub")),
    ]
    yield
    for t in tasks:
        t.cancel()
    pool.close()


app = FastAPI(title="Distributed object storage: metadata, placement and S3-like gateway", lifespan=lifespan)


# ------------------------------------------------------------------ cluster membership

class Heartbeat(BaseModel):
    node_id: str
    address: str = ""
    free_bytes: int
    chunk_count: int
    timestamp: float


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    with pool.connection() as conn:
        row = conn.execute(
            """INSERT INTO nodes (node_id, address, free_bytes, chunk_count, reported_at, last_seen)
               VALUES (%s,%s,%s,%s,%s, now())
               ON CONFLICT (node_id) DO UPDATE SET address=EXCLUDED.address, free_bytes=EXCLUDED.free_bytes,
                 chunk_count=EXCLUDED.chunk_count, reported_at=EXCLUDED.reported_at, last_seen=now()
               RETURNING (xmax = 0) AS new, in_ring""",
            (hb.node_id, hb.address, hb.free_bytes, hb.chunk_count, hb.timestamp)).fetchone()
    if row["in_ring"] and ring.add_node(hb.node_id):
        log.info("node %s joined the ring%s", hb.node_id, " (new node)" if row["new"] else "")
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT n.node_id, n.address, n.free_bytes, n.chunk_count, n.reported_at AS timestamp, n.in_ring, "
            "extract(epoch FROM now() - n.last_seen) AS age, "
            "(SELECT count(*) FROM chunk_locations l WHERE l.node_id = n.node_id) AS stored_pieces "
            "FROM nodes n ORDER BY n.node_id").fetchall()
    return [
        {"node_id": r["node_id"], "address": r["address"], "free_bytes": r["free_bytes"],
         "chunk_count": r["chunk_count"], "timestamp": r["timestamp"], "in_ring": r["in_ring"],
         "stored_pieces": r["stored_pieces"], "age_seconds": round(r["age"], 1), "alive": r["age"] < config.DEAD_AFTER}
        for r in rows
    ]


# ------------------------------------------------------------------ admin

UI_FILE = Path(__file__).resolve().parent.parent / "ui" / "index.html"


@app.get("/ui", response_class=HTMLResponse)
def web_ui():
    """The React web interface (single page, served by the gateway itself)."""
    return HTMLResponse(UI_FILE.read_text(encoding="utf8"))


@app.get("/cluster/status")
def cluster_status():
    """One call with everything a dashboard needs: node health, chunk distribution, repairs and storage figures."""
    with pool.connection() as conn:
        dist = {r["node_id"]: r for r in conn.execute(
            "SELECT node_id, count(*) AS pieces, coalesce(sum(size),0)::bigint AS bytes FROM chunk_locations GROUP BY node_id").fetchall()}
        modes = conn.execute("SELECT mode, count(*) AS chunks FROM chunks GROUP BY mode").fetchall()
    node_list = list_nodes()
    for n in node_list:
        d = dist.get(n["node_id"], {"pieces": 0, "bytes": 0})
        n["pieces"], n["stored_bytes"] = d["pieces"], d["bytes"]
    st = admin_status()
    return {"nodes": node_list, "chunks_by_durability": modes, "storage": stats(),
            "under_protected_chunks": st["under_protected_chunks"], "failed_nodes": st["failed_nodes"],
            "gc_queue": st["gc_queue"], "recent_repairs": admin_repairs(limit=15),
            "config": {"replication_factor": config.REPLICATION_FACTOR, "write_quorum": config.WRITE_QUORUM,
                       "read_quorum": config.READ_QUORUM, "ec": {"k": config.EC_K, "m": config.EC_M}}}


@app.get("/admin/config")
def admin_config():
    return {"replication_factor": config.REPLICATION_FACTOR, "write_quorum": config.WRITE_QUORUM,
            "read_quorum": config.READ_QUORUM, "ec": {"k": config.EC_K, "m": config.EC_M},
            "default_durability": config.DEFAULT_DURABILITY, "default_chunking": config.DEFAULT_CHUNKING,
            "chunk_size": config.CHUNK_SIZE, "vnodes_per_node": config.VNODES, "dead_after_seconds": config.DEAD_AFTER,
            "repair_interval": config.REPAIR_INTERVAL, "scrub_interval": config.SCRUB_INTERVAL,
            "ring_nodes": ring.nodes}


@app.get("/admin/status")
def admin_status():
    under = maintenance.under_protected()
    with pool.connection() as conn:
        n = conn.execute(
            "SELECT count(*) AS total, count(*) FILTER (WHERE last_seen > now() - make_interval(secs => %s)) AS alive, "
            "count(*) FILTER (WHERE in_ring) AS in_ring FROM nodes", (config.DEAD_AFTER,)).fetchone()
        counts = conn.execute(
            "SELECT (SELECT count(*) FROM objects) AS objects, (SELECT count(*) FROM chunks) AS chunks, "
            "(SELECT count(*) FROM gc_queue) AS gc_queue").fetchone()
        events = conn.execute("SELECT at, kind, chunk_hash, detail FROM repair_log ORDER BY id DESC LIMIT 10").fetchall()
    for e in events:
        e["at"] = e["at"].isoformat()
    return {"nodes": n, "failed_nodes": n["total"] - n["alive"], **counts, "under_protected_chunks": len(under),
            "recent_events": events}


@app.get("/admin/repairs")
def admin_repairs(limit: int = Query(50, ge=1, le=1000)):
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, at, kind, chunk_hash, detail FROM repair_log ORDER BY id DESC LIMIT %s", (limit,)).fetchall()
    for r in rows:
        r["at"] = r["at"].isoformat()
    return rows


@app.post("/admin/repair")
async def admin_repair():
    return await run_in_threadpool(maintenance.repair_pass)


@app.post("/admin/scrub")
async def admin_scrub():
    return await run_in_threadpool(maintenance.scrub_pass)


@app.post("/admin/rebalance")
async def admin_rebalance(dry_run: bool = False):
    return await run_in_threadpool(maintenance.rebalance, not dry_run)


@app.post("/admin/gc")
async def admin_gc():
    return {"removed": await run_in_threadpool(maintenance.run_gc)}


@app.get("/admin/nodes/{node_id}/usage")
def node_usage(node_id: str):
    """How many pieces (copies or shards) and bytes the metadata says a node holds."""
    with pool.connection() as conn:
        r = conn.execute("SELECT count(*) AS pieces, coalesce(sum(size),0)::bigint AS bytes "
                         "FROM chunk_locations WHERE node_id=%s", (node_id,)).fetchone()
    return {"node_id": node_id, **r}


@app.get("/admin/traffic")
def admin_traffic():
    """Bytes moved between the gateway and the storage nodes, by purpose (client, repair, rebalance, scrub, gc)."""
    return nodes.traffic()


@app.post("/admin/traffic/reset")
def admin_traffic_reset():
    return nodes.traffic(reset=True)


@app.post("/admin/nodes/{node_id}/decommission")
async def decommission(node_id: str):
    """Take a node out of the ring and move its data to the nodes that now own it."""
    with pool.connection() as conn:
        row = conn.execute("UPDATE nodes SET in_ring=false WHERE node_id=%s RETURNING node_id", (node_id,)).fetchone()
    if not row:
        raise HTTPException(404, "unknown node")
    ring.remove_node(node_id)
    result = await run_in_threadpool(maintenance.rebalance, True)
    with pool.connection() as conn:
        left = conn.execute("SELECT count(*) AS n FROM chunk_locations WHERE node_id=%s", (node_id,)).fetchone()["n"]
    return {"node": node_id, "pieces_left_on_node": left, "safe_to_remove": left == 0, "rebalance": result}


@app.delete("/admin/nodes/{node_id}", status_code=204)
def forget_node(node_id: str):
    """Remove a decommissioned, empty node from the cluster records."""
    with pool.connection() as conn:
        if conn.execute("SELECT 1 FROM chunk_locations WHERE node_id=%s LIMIT 1", (node_id,)).fetchone():
            raise HTTPException(409, "node still holds data; decommission it first")
        conn.execute("DELETE FROM gc_queue WHERE node_id=%s", (node_id,))
        conn.execute("DELETE FROM nodes WHERE node_id=%s", (node_id,))
    ring.remove_node(node_id)
    return Response(status_code=204)


@app.post("/admin/nodes/{node_id}/join")
def join(node_id: str):
    with pool.connection() as conn:
        row = conn.execute("UPDATE nodes SET in_ring=true WHERE node_id=%s RETURNING node_id", (node_id,)).fetchone()
    if not row:
        raise HTTPException(404, "unknown node")
    ring.add_node(node_id)
    return {"node": node_id, "ring": ring.nodes, "hint": "POST /admin/rebalance to move data onto it"}


@app.get("/admin/chunks/{chunk_hash}")
def chunk_info(chunk_hash: str):
    with pool.connection() as conn:
        c = conn.execute("SELECT * FROM chunks WHERE hash=%s", (chunk_hash,)).fetchone()
        if not c:
            raise HTTPException(404, "chunk not found")
        c["locations"] = conn.execute(
            "SELECT shard_idx, shard_hash, node_id, size FROM chunk_locations WHERE chunk_hash=%s "
            "ORDER BY shard_idx, node_id", (chunk_hash,)).fetchall()
    return c


@app.get("/admin/ring")
def ring_info():
    return {"vnodes_per_node": ring.vnodes, "nodes": ring.nodes}


@app.get("/admin/ring/lookup/{chunk_hash}")
def ring_lookup(chunk_hash: str, n: int = Query(1, ge=1, le=16)):
    try:
        return {"chunk": chunk_hash, "nodes": ring.lookup(chunk_hash, n)}
    except ValueError:
        raise HTTPException(400, "need a hex chunk hash")


@app.get("/admin/ring/distribution")
def ring_distribution(samples: int = Query(100000, ge=100, le=1000000)):
    counts: Counter = Counter()
    rnd = random.Random(1)
    for _ in range(samples):
        counts[ring.lookup(f"{rnd.getrandbits(64):016x}")[0]] += 1
    return {n: {"keys": counts[n], "share": round(counts[n] / samples, 4)} for n in ring.nodes}


@app.get("/admin/stats")
def stats():
    with pool.connection() as conn:
        o = conn.execute("SELECT count(*) AS objects, coalesce(sum(size),0)::bigint AS logical_bytes, "
                         "coalesce(sum(chunk_count),0)::bigint AS chunk_references FROM objects").fetchone()
        c = conn.execute("SELECT count(*) AS unique_chunks, coalesce(sum(size),0)::bigint AS unique_bytes FROM chunks").fetchone()
        raw = conn.execute("SELECT coalesce(sum(size),0)::bigint AS raw_bytes, count(*) AS stored_pieces FROM chunk_locations").fetchone()
        by_mode = conn.execute(
            "SELECT c.mode, count(*) AS chunks, coalesce(sum(c.size),0)::bigint AS unique_bytes, "
            "coalesce(sum(s.raw),0)::bigint AS raw_bytes FROM chunks c LEFT JOIN "
            "(SELECT chunk_hash, sum(size) AS raw FROM chunk_locations GROUP BY chunk_hash) s ON s.chunk_hash=c.hash "
            "GROUP BY c.mode").fetchall()
    ub = c["unique_bytes"]
    return {
        **o, **c, **raw,
        "dedup_ratio": round(o["logical_bytes"] / ub, 3) if ub else None,
        "bytes_saved_by_dedup": int(o["logical_bytes"]) - int(ub),
        "redundancy_overhead": round(raw["raw_bytes"] / ub, 3) if ub else None,
        "by_durability": [{**r, "overhead": round(r["raw_bytes"] / r["unique_bytes"], 3) if r["unique_bytes"] else None}
                          for r in by_mode],
    }


# ------------------------------------------------------------------ search

@app.get("/search")
def search(request: Request, q: str = "", bucket: str = "", prefix: str = "", min_size: int | None = None,
           max_size: int | None = None, content_type: str = "", created_after: str = "", created_before: str = "",
           limit: int = Query(100, ge=1, le=10000)):
    """Find objects by key/metadata text, size range, content type or exact user metadata (meta.<name>=<value>)."""
    # user-defined tags: tag.<name>=<value> (meta.<name>=<value> is accepted as a synonym)
    meta = {k.split(".", 1)[1].lower(): v for k, v in request.query_params.items() if k.startswith(("tag.", "meta."))}
    for label, value in (("created_after", created_after), ("created_before", created_before)):
        if value:
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(400, f"{label} must be an ISO date, for example 2026-10-28 or 2026-10-28T09:00:00")
    results = store.search_objects(q, bucket, prefix, min_size, max_size, content_type, meta, limit,
                                   created_after or None, created_before or None)
    return {"count": len(results), "results": results}


# ------------------------------------------------------------------ S3-like API: buckets

def check_bucket_name(name: str):
    if name in RESERVED or not BUCKET_RE.match(name):
        raise HTTPException(400, "InvalidBucketName: use 3-63 chars of a-z, 0-9, '.' or '-'")


@app.get("/")
def list_buckets():
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT b.name, b.created_at, b.durability, b.copies, b.write_q, b.read_q, b.k, b.m, count(o.id) AS objects, coalesce(sum(o.size),0)::bigint AS bytes "
            "FROM buckets b LEFT JOIN objects o ON o.bucket = b.name GROUP BY b.name, b.created_at, b.durability, b.copies, b.write_q, b.read_q, b.k, b.m ORDER BY b.name").fetchall()
    for r in rows:
        r["created_at"] = r["created_at"].isoformat()
    return {"buckets": rows}


@app.put("/{bucket}", status_code=201)
def create_bucket(
    bucket: str,
    durability: str = Query(config.DEFAULT_DURABILITY, pattern="^(replication|ec)$"),
    copies: int | None = Query(None, ge=1, description="N: copies per chunk (default from config)"),
    write_quorum: int | None = Query(None, ge=1, description="W"),
    read_quorum: int | None = Query(None, ge=1, description="R"),
    k: int | None = Query(None, ge=1), m: int | None = Query(None, ge=1),
):
    """Create a bucket. Its durability policy (N-way replication with quorums, or erasure coding k+m) applies to every
    object written to it unless a request overrides it."""
    check_bucket_name(bucket)
    if durability == "replication":
        n = copies or config.REPLICATION_FACTOR
        same = n == config.REPLICATION_FACTOR
        d = store.Durability("rep", copies=n, w=write_quorum or (config.WRITE_QUORUM if same else 0),
                             r=read_quorum or (config.READ_QUORUM if same else 0))
    else:
        d = store.Durability("ec", k=k or config.EC_K, m=m or config.EC_M)
    store.validate_durability(d)
    with pool.connection() as conn:
        row = conn.execute(
            "INSERT INTO buckets (name, durability, copies, write_q, read_q, k, m) VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING RETURNING name",
            (bucket, "ec" if d.kind == "ec" else "replication", d.copies, d.w, d.r, d.k, d.m)).fetchone()
    if not row:
        raise HTTPException(409, "BucketAlreadyExists")
    return {"bucket": bucket, "durability": d.label()}


@app.head("/{bucket}")
def head_bucket(bucket: str):
    with pool.connection() as conn:
        ok = conn.execute("SELECT 1 FROM buckets WHERE name=%s", (bucket,)).fetchone()
    return Response(status_code=200 if ok else 404)


@app.delete("/{bucket}", status_code=204)
def delete_bucket(bucket: str):
    with pool.connection() as conn:
        if not conn.execute("SELECT 1 FROM buckets WHERE name=%s", (bucket,)).fetchone():
            raise HTTPException(404, "NoSuchBucket")
        if conn.execute("SELECT 1 FROM objects WHERE bucket=%s LIMIT 1", (bucket,)).fetchone():
            raise HTTPException(409, "BucketNotEmpty")
        conn.execute("DELETE FROM buckets WHERE name=%s", (bucket,))
    return Response(status_code=204)


@app.get("/{bucket}")
def list_objects_v2(bucket: str, prefix: str = "", delimiter: str = "",
                    max_keys: int = Query(1000, alias="max-keys", ge=1, le=1000),
                    continuation_token: str = Query("", alias="continuation-token"),
                    start_after: str = Query("", alias="start-after")):
    return store.list_objects(bucket, prefix, delimiter, max_keys, continuation_token or start_after)


# ------------------------------------------------------------------ S3-like API: objects

def user_meta(request: Request) -> dict:
    return {k[len("x-amz-meta-"):]: v for k, v in request.headers.items() if k.startswith("x-amz-meta-")}


def object_headers(obj: dict) -> dict:
    h = {"ETag": f'"{obj["sha256"]}"', "Accept-Ranges": "bytes", "Last-Modified": format_datetime(obj["created_at"], usegmt=True),
         "X-DFS-Durability": obj["durability"], "X-DFS-Chunking": obj["chunking"], "X-DFS-Chunks": str(obj["chunk_count"])}
    h.update({f"x-amz-meta-{k}": str(v) for k, v in obj["meta"].items()})
    return h


@app.put("/{bucket}/{key:path}", status_code=200)
async def put_object(
    bucket: str, key: str, request: Request,
    chunking: str = Query(config.DEFAULT_CHUNKING, pattern="^(fixed|cdc)$"),
    chunk_size: int = Query(config.CHUNK_SIZE, ge=1024),
    cdc_min: int = Query(256 * 1024, ge=64), cdc_avg: int = Query(1024 * 1024, ge=256), cdc_max: int = Query(4 * 1024 * 1024, ge=1024),
    durability: str | None = Query(None, pattern="^(replication|ec)$"),
    copies: int | None = Query(None, ge=1), write_quorum: int | None = Query(None, ge=1),
    read_quorum: int | None = Query(None, ge=1), k: int | None = Query(None, ge=1), m: int | None = Query(None, ge=1),
):
    declared = request.headers.get("content-length")
    if declared and int(declared) > config.MAX_OBJECT_BYTES:
        raise HTTPException(413, "EntityTooLarge")
    data = await request.body()
    if len(data) > config.MAX_OBJECT_BYTES:
        raise HTTPException(413, "EntityTooLarge")
    if not data:
        raise HTTPException(400, "empty objects are not supported")
    upload_id = request.query_params.get("uploadId")
    if upload_id:  # one part of a multipart upload
        part = request.query_params.get("partNumber")
        if not part or not part.isdigit():
            raise HTTPException(400, "partNumber is required")
        etag = await run_in_threadpool(multipart.upload_part, bucket, key, upload_id, int(part), data)
        return Response(content=json.dumps({"partNumber": int(part), "etag": etag}), media_type="application/json",
                        headers={"ETag": f'"{etag}"'})
    d = await run_in_threadpool(store.resolve_durability, bucket, durability=durability, copies=copies,
                                write_quorum=write_quorum, read_quorum=read_quorum, k=k, m=m)
    result = await run_in_threadpool(
        store.put_object, bucket, key, data, chunking,
        {"chunk_size": chunk_size, "min_size": cdc_min, "avg_size": cdc_avg, "max_size": cdc_max},
        d, request.headers.get("content-type", "application/octet-stream"), user_meta(request))
    return Response(content=json.dumps(result), media_type="application/json",
                    headers={"ETag": f'"{result["etag"]}"'})


def parse_range(header: str, size: int) -> tuple[int, int]:
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", header.strip())
    if not m or (not m.group(1) and not m.group(2)):
        raise HTTPException(416, "InvalidRange")
    if not m.group(1):  # suffix: last N bytes
        start, end = max(size - int(m.group(2)), 0), size - 1
    else:
        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else size - 1
    if start >= size or start > end:
        raise HTTPException(416, "InvalidRange")
    return start, min(end, size - 1)


@app.get("/admin/manifest/{bucket}/{key:path}")
def manifest(bucket: str, key: str):
    obj, rows = store.load_manifest(bucket, key)
    return {"bucket": bucket, "key": key, "size": obj["size"], "etag": obj["sha256"], "chunking": obj["chunking"],
            "durability": obj["durability"], "chunks": [
                {"seq": r["seq"], "hash": r["hash"], "size": r["size"], "refcount": r["refcount"], "mode": r["mode"],
                 "pieces": [{"idx": l["idx"], "node": l["node"], "alive": l["alive"]} for l in r["locs"]]}
                for r in rows]}


def read_consistency(request: Request) -> str:
    c = (request.headers.get("x-dfs-consistency") or request.query_params.get("consistency") or "quorum").lower()
    if c not in ("quorum", "any"):
        raise HTTPException(400, "consistency must be 'quorum' or 'any'")
    return c


@app.get("/{bucket}/{key:path}")
def get_object(bucket: str, key: str, request: Request):
    consistency = read_consistency(request)
    rng = request.headers.get("range")
    if rng:
        with pool.connection() as conn:
            row = conn.execute("SELECT size FROM objects WHERE bucket=%s AND key=%s", (bucket, key)).fetchone()
        if not row:
            raise HTTPException(404, "NoSuchKey")
        start, end = parse_range(rng, row["size"])
        obj, data, degraded = store.read_object(bucket, key, start, end, consistency)
        headers = {**object_headers(obj), "Content-Range": f"bytes {start}-{end}/{obj['size']}",
                   "X-DFS-Degraded-Chunks": str(degraded)}
        return Response(content=data, status_code=206, media_type=obj["content_type"], headers=headers)
    obj, data, degraded = store.read_object(bucket, key, consistency=consistency)
    return Response(content=data, media_type=obj["content_type"],
                    headers={**object_headers(obj), "X-DFS-Degraded-Chunks": str(degraded)})


@app.head("/{bucket}/{key:path}")
def head_object(bucket: str, key: str):
    with pool.connection() as conn:
        obj = conn.execute("SELECT * FROM objects WHERE bucket=%s AND key=%s", (bucket, key)).fetchone()
    if not obj:
        return Response(status_code=404)
    return Response(status_code=200, media_type=obj["content_type"],
                    headers={**object_headers(obj), "Content-Length": str(obj["size"])})


@app.post("/{bucket}/{key:path}")
def multipart_control(bucket: str, key: str, request: Request):
    """POST ?uploads starts a multipart upload; POST ?uploadId=.. completes it."""
    if "uploads" in request.query_params:
        meta = user_meta(request)
        uid = multipart.initiate(bucket, key, request.headers.get("content-type", "application/octet-stream"), meta)
        return {"bucket": bucket, "key": key, "uploadId": uid}
    upload_id = request.query_params.get("uploadId")
    if upload_id:
        result = multipart.complete(bucket, key, upload_id)
        return Response(content=json.dumps(result), media_type="application/json", headers={"ETag": f'"{result["etag"]}"'})
    raise HTTPException(400, "use ?uploads to start or ?uploadId=<id> to complete a multipart upload")


@app.delete("/{bucket}/{key:path}", status_code=204)
def delete_object(bucket: str, key: str, request: Request):
    upload_id = request.query_params.get("uploadId")
    if upload_id:  # abort a multipart upload and free its parts
        multipart.abort(bucket, key, upload_id)
        return Response(status_code=204)
    store.delete_object(bucket, key)  # S3 returns 204 even when the key does not exist
    return Response(status_code=204)
