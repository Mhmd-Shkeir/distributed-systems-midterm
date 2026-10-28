"""Object operations: write path, read path, delete, listing and search."""
import hashlib
import json
import logging
from collections import Counter

from fastapi import HTTPException

from . import chunking, ec, nodes
from .config import DEAD_AFTER
from .db import lock_keys, pool
from .maintenance import queue_gc, release_chunk, run_gc
from .nodes import ring

log = logging.getLogger("metadata.store")


class Durability:
    """How a chunk is protected: full copies ('rep') or Reed-Solomon shards ('ec')."""

    def __init__(self, kind: str, copies: int = 0, k: int = 0, m: int = 0):
        self.kind, self.copies, self.k, self.m = kind, copies, k, m

    @property
    def width(self) -> int:  # nodes needed per chunk
        return self.copies if self.kind == "rep" else self.k + self.m

    @property
    def write_quorum(self) -> int:  # copies/shards that must be stored before the write is acknowledged
        return self.copies // 2 + 1 if self.kind == "rep" else min(self.k + 1, self.k + self.m)

    def label(self) -> str:
        return f"replication x{self.copies}" if self.kind == "rep" else f"erasure coding {self.k}+{self.m}"


def validate_durability(d: Durability):
    members = len(ring.nodes)
    if d.kind == "rep":
        if not 1 <= d.copies <= max(members, 1):
            raise HTTPException(400, f"copies must be between 1 and the number of ring nodes ({members})")
    else:
        if d.k < 1 or d.m < 1 or d.k + d.m > members:
            raise HTTPException(400, f"erasure coding k+m must be <= the number of ring nodes ({members}), with k,m >= 1")


def _prepare(h: str, piece: bytes, d: Durability) -> list[tuple[int, str, bytes]]:
    """(shard_idx, stored_hash, bytes) for every piece that has to be written for a new chunk."""
    if d.kind == "rep":
        return [(0, h, piece)]
    return [(i, hashlib.sha256(s).hexdigest(), s) for i, s in enumerate(ec.encode(piece, d.k, d.m))]


def _place(conn, h: str, parts, d: Durability, alive: dict[str, str], placed: list):
    """Write a new chunk to the nodes the ring chooses; returns the number of pieces stored."""
    if d.kind == "rep":
        stored_hash, data = parts[0][1], parts[0][2]
        candidates = ring.lookup(h, len(alive) or 1, accept=lambda n: n in alive)
        ok = 0
        for node_id in candidates:
            if ok == d.copies:
                break
            try:
                nodes.put(alive[node_id], stored_hash, data)
            except Exception as exc:
                log.warning("write of %s to %s failed: %s", h[:12], node_id, exc)
                continue
            placed.append((stored_hash, node_id))
            conn.execute(
                "INSERT INTO chunk_locations (chunk_hash, shard_idx, shard_hash, node_id, size) VALUES (%s,0,%s,%s,%s)",
                (h, stored_hash, node_id, len(data)))
            ok += 1
        return ok
    candidates = ring.lookup(h, d.width, accept=lambda n: n in alive)
    ok = 0
    for (idx, shash, data), node_id in zip(parts, candidates):
        try:
            nodes.put(alive[node_id], shash, data)
        except Exception as exc:
            log.warning("write of shard %d of %s to %s failed: %s", idx, h[:12], node_id, exc)
            continue
        placed.append((shash, node_id))
        conn.execute(
            "INSERT INTO chunk_locations (chunk_hash, shard_idx, shard_hash, node_id, size) VALUES (%s,%s,%s,%s,%s)",
            (h, idx, shash, node_id, len(data)))
        ok += 1
    return ok


def put_object(bucket: str, key: str, data: bytes, strategy: str, cparams: dict, d: Durability,
               content_type: str, meta: dict) -> dict:
    validate_durability(d)
    try:
        spans = chunking.split(data, strategy, **cparams)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    pieces = [data[s:e] for s, e in spans]
    hashes = [hashlib.sha256(p).hexdigest() for p in pieces]
    by_hash = dict(zip(hashes, pieces))
    counts = Counter(hashes)
    object_sha = hashlib.sha256(data).hexdigest()

    with pool.connection() as conn:  # optimistic look so only new chunks are erasure-coded
        have = {r["hash"] for r in conn.execute("SELECT hash FROM chunks WHERE hash = ANY(%s)", (list(counts),)).fetchall()}
        old = conn.execute("SELECT id FROM objects WHERE bucket=%s AND key=%s", (bucket, key)).fetchone()
        old_refs = conn.execute(
            "SELECT chunk_hash, count(*) AS k FROM object_chunks WHERE object_id=%s GROUP BY chunk_hash",
            (old["id"],)).fetchall() if old else []
    prepared = {h: _prepare(h, by_hash[h], d) for h in counts if h not in have}
    keys = set(counts) | {r["chunk_hash"] for r in old_refs}
    for parts in prepared.values():
        keys |= {p[1] for p in parts}

    placed: list[tuple[str, str]] = []
    try:
        with pool.connection() as conn:
            if not conn.execute("SELECT 1 FROM buckets WHERE name=%s", (bucket,)).fetchone():
                raise HTTPException(404, f"NoSuchBucket: {bucket}")
            alive = nodes.alive(conn)
            if not alive:
                raise HTTPException(503, "no storage nodes are alive")
            lock_keys(conn, keys)

            new_chunks = bytes_new = raw_written = 0
            for h in sorted(counts):
                row = conn.execute(
                    """INSERT INTO chunks (hash, size, refcount, mode, copies, k, m) VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (hash) DO UPDATE SET refcount = chunks.refcount + EXCLUDED.refcount
                       RETURNING (xmax = 0) AS inserted""",
                    (h, len(by_hash[h]), counts[h], d.kind, d.copies, d.k, d.m)).fetchone()
                if not row["inserted"]:
                    continue  # deduplicated: identical chunk already stored, only the reference count grew
                parts = prepared.get(h) or _prepare(h, by_hash[h], d)
                lock_keys(conn, [p[1] for p in parts])
                ok = _place(conn, h, parts, d, alive, placed)
                if ok < d.write_quorum or ok == 0:
                    raise HTTPException(503, f"could only store {ok} of {d.width} pieces of a chunk "
                                             f"(need {d.write_quorum}); too many nodes are down")
                new_chunks += 1
                bytes_new += len(by_hash[h])
                raw_written += sum(len(p[2]) for p in parts[:ok]) if d.kind == "ec" else len(by_hash[h]) * ok

            if old:  # S3 semantics: PUT over an existing key replaces it
                conn.execute("DELETE FROM objects WHERE id=%s", (old["id"],))
                for r in old_refs:
                    release_chunk(conn, r["chunk_hash"], r["k"])
            obj = conn.execute(
                """INSERT INTO objects (bucket, key, size, sha256, content_type, meta, chunking, durability, chunk_count)
                   VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s) RETURNING id""",
                (bucket, key, len(data), object_sha, content_type, json.dumps(meta),
                 strategy, d.label(), len(hashes))).fetchone()
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO object_chunks (object_id, seq, chunk_hash) VALUES (%s,%s,%s)",
                                [(obj["id"], i, h) for i, h in enumerate(hashes)])
    except Exception:
        if placed:  # transaction rolled back: queue what was written for removal
            with pool.connection() as conn:
                queue_gc(conn, placed)
            run_gc()
        raise

    return {
        "bucket": bucket, "key": key, "etag": object_sha, "size": len(data), "chunking": strategy,
        "durability": d.label(), "chunks": len(hashes), "unique_chunks": len(counts), "new_chunks": new_chunks,
        "bytes_new": bytes_new, "bytes_deduplicated": len(data) - bytes_new, "raw_bytes_written": raw_written,
        "replaced_existing": bool(old),
    }


# ------------------------------------------------------------------ read path

def load_manifest(bucket: str, key: str):
    with pool.connection() as conn:
        obj = conn.execute("SELECT * FROM objects WHERE bucket=%s AND key=%s", (bucket, key)).fetchone()
        if not obj:
            raise HTTPException(404, "NoSuchKey")
        rows = conn.execute(
            """SELECT oc.seq, c.hash, c.size, c.refcount, c.mode, c.k, c.m, c.copies,
                      coalesce(json_agg(json_build_object('idx', l.shard_idx, 'hash', l.shard_hash, 'node', l.node_id,
                               'addr', n.address, 'alive', n.last_seen > now() - make_interval(secs => %s)))
                               FILTER (WHERE l.node_id IS NOT NULL), '[]') AS locs
               FROM object_chunks oc JOIN chunks c ON c.hash = oc.chunk_hash
               LEFT JOIN chunk_locations l ON l.chunk_hash = c.hash
               LEFT JOIN nodes n ON n.node_id = l.node_id
               WHERE oc.object_id = %s GROUP BY oc.seq, c.hash, c.size, c.refcount, c.mode, c.k, c.m, c.copies
               ORDER BY oc.seq""",
            (DEAD_AFTER, obj["id"])).fetchall()
    return obj, rows


def read_chunk(row) -> tuple[bytes, bool]:
    """Return (chunk bytes, degraded). Tries healthy nodes first; erasure-coded chunks decode when needed."""
    h = row["hash"]
    locs = sorted(row["locs"], key=lambda l: (not l["alive"], l["idx"]))
    if row["mode"] == "rep":
        degraded = False
        for l in locs:
            data = nodes.get(l["addr"], h) if l["alive"] else None
            if data is not None:
                return data, degraded
            degraded = True
        raise HTTPException(502, f"all copies of chunk {h[:12]} are unavailable")
    k, m = row["k"], row["m"]
    got: dict[int, bytes] = {}
    for l in locs:  # data shards first (idx order) so a healthy read needs no decoding
        if len(got) == k:
            break
        if l["alive"] and l["idx"] not in got:
            s = nodes.get(l["addr"], l["hash"])
            if s is not None:
                got[l["idx"]] = s
    if len(got) < k:
        raise HTTPException(502, f"only {len(got)} of {k} shards of chunk {h[:12]} are reachable")
    degraded = not all(i in got for i in range(k))
    data = ec.decode(got, k, m, row["size"])
    if hashlib.sha256(data).hexdigest() != h:
        raise HTTPException(502, f"chunk {h[:12]} failed verification after decoding")
    return data, degraded


def read_object(bucket: str, key: str, start: int = 0, end: int | None = None):
    """Return (object row, bytes[start:end+1], number of chunks read in degraded mode)."""
    obj, rows = load_manifest(bucket, key)
    last = obj["size"] - 1 if end is None else min(end, obj["size"] - 1)
    out, degraded, pos = [], 0, 0
    for r in rows:
        c_start, c_end = pos, pos + r["size"] - 1
        pos += r["size"]
        if c_end < start or c_start > last:
            continue
        data, deg = read_chunk(r)
        degraded += deg
        out.append(data[max(start - c_start, 0): last - c_start + 1])
    return obj, b"".join(out), degraded


# ------------------------------------------------------------------ delete

def delete_object(bucket: str, key: str) -> dict:
    with pool.connection() as conn:
        obj = conn.execute("SELECT id FROM objects WHERE bucket=%s AND key=%s", (bucket, key)).fetchone()
        if not obj:
            return {"deleted": False, "chunks_freed": 0}
        refs = conn.execute(
            "SELECT chunk_hash, count(*) AS k FROM object_chunks WHERE object_id=%s GROUP BY chunk_hash", (obj["id"],)
        ).fetchall()
        stored = conn.execute(
            "SELECT DISTINCT shard_hash FROM chunk_locations WHERE chunk_hash = ANY(%s)", ([r["chunk_hash"] for r in refs],)
        ).fetchall()
    keys = {r["chunk_hash"] for r in refs} | {r["shard_hash"] for r in stored}
    freed = 0
    with pool.connection() as conn:
        lock_keys(conn, keys)
        obj = conn.execute("SELECT id FROM objects WHERE bucket=%s AND key=%s FOR UPDATE", (bucket, key)).fetchone()
        if not obj:
            return {"deleted": False, "chunks_freed": 0}
        conn.execute("DELETE FROM objects WHERE id=%s", (obj["id"],))
        for r in sorted(refs, key=lambda r: r["chunk_hash"]):
            freed += release_chunk(conn, r["chunk_hash"], r["k"])
    run_gc()
    return {"deleted": True, "chunks_freed": freed}


# ------------------------------------------------------------------ listing and search

def _like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def list_objects(bucket: str, prefix: str, delimiter: str, max_keys: int, token: str):
    """S3 ListObjectsV2: prefix filtering, delimiter roll-up into common prefixes, continuation tokens."""
    contents, prefixes = [], []
    after, truncated = token, False
    base = ("FROM objects WHERE bucket=%s AND key LIKE %s ESCAPE '\' AND key COLLATE \"C\" > %s")
    with pool.connection() as conn:
        if not conn.execute("SELECT 1 FROM buckets WHERE name=%s", (bucket,)).fetchone():
            raise HTTPException(404, f"NoSuchBucket: {bucket}")
        like = _like(prefix) + "%"
        while True:
            if len(contents) + len(prefixes) >= max_keys:
                truncated = bool(conn.execute(f"SELECT 1 {base} LIMIT 1", (bucket, like, after)).fetchone())
                break
            rows = conn.execute(
                f"SELECT key, size, sha256, content_type, created_at {base} ORDER BY key COLLATE \"C\" LIMIT 200",
                (bucket, like, after)).fetchall()
            if not rows:
                break
            jumped = False
            for r in rows:
                if len(contents) + len(prefixes) >= max_keys:
                    break
                rest = r["key"][len(prefix):]
                if delimiter and delimiter in rest:
                    cp = prefix + rest[: rest.index(delimiter) + len(delimiter)]
                    prefixes.append(cp)
                    after = _just_before(_successor(cp))  # skip every key under this prefix
                    jumped = True
                    break
                contents.append({"key": r["key"], "size": r["size"], "etag": r["sha256"],
                                 "content_type": r["content_type"], "last_modified": r["created_at"].isoformat()})
                after = r["key"]
            if len(contents) + len(prefixes) >= max_keys:
                continue  # the top of the loop decides whether the listing is truncated
            if not jumped and len(rows) < 200:
                break
    return {"bucket": bucket, "prefix": prefix, "delimiter": delimiter, "max_keys": max_keys,
            "is_truncated": truncated, "next_continuation_token": after if truncated else None,
            "contents": contents, "common_prefixes": prefixes, "key_count": len(contents) + len(prefixes)}


def _successor(s: str) -> str:
    """Smallest string greater than every string that starts with s."""
    return s[:-1] + chr(ord(s[-1]) + 1)


def _just_before(s: str) -> str:
    """A string below `s` and above every string that sorts before it, for `key > after`."""
    return s[:-1] + chr(ord(s[-1]) - 1) + "􏿿"


def search_objects(q: str, bucket: str, prefix: str, min_size, max_size, content_type: str, meta: dict, limit: int):
    where, args = ["TRUE"], []
    if bucket:
        where.append("bucket = %s")
        args.append(bucket)
    if prefix:
        where.append("key LIKE %s ESCAPE '\\'")
        args.append(_like(prefix) + "%")
    if q:
        where.append("(key ILIKE %s ESCAPE '\\' OR meta::text ILIKE %s ESCAPE '\\')")
        args += [f"%{_like(q)}%", f"%{_like(q)}%"]
    if min_size is not None:
        where.append("size >= %s")
        args.append(min_size)
    if max_size is not None:
        where.append("size <= %s")
        args.append(max_size)
    if content_type:
        where.append("content_type = %s")
        args.append(content_type)
    if meta:
        where.append("meta @> %s::jsonb")
        args.append(json.dumps(meta))
    with pool.connection() as conn:
        rows = conn.execute(
            f"SELECT bucket, key, size, sha256 AS etag, content_type, meta, durability, created_at FROM objects "
            f"WHERE {' AND '.join(where)} ORDER BY bucket, key LIMIT %s", (*args, limit)).fetchall()
    for r in rows:
        r["created_at"] = r["created_at"].isoformat()
    return rows
