"""Storage node: content-addressed chunk store with PUT/GET/DELETE/HEAD.

A chunk's id is the SHA-256 of its bytes. PUT rejects bodies whose hash does not
match the id in the URL; GET re-hashes the stored bytes and refuses to serve a
chunk that no longer matches (it is moved to quarantine). A background task
sends heartbeats to the metadata service.
"""
import asyncio
import hashlib
import logging
import os
import re
import shutil
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response

NODE_ID = os.environ.get("NODE_ID", "node-unknown")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
METADATA_URL = os.environ.get("METADATA_URL", "http://metadata:8000")
PUBLIC_ADDR = os.environ.get("PUBLIC_ADDR", "")
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "5"))
MAX_CHUNK_BYTES = int(os.environ.get("MAX_CHUNK_BYTES", str(64 * 1024 * 1024)))

CHUNK_DIR = DATA_DIR / "chunks"
QUARANTINE_DIR = DATA_DIR / "quarantine"
HASH_RE = re.compile(r"^[0-9a-f]{64}$")

log = logging.getLogger("storage-node")
logging.basicConfig(level=logging.INFO)


def chunk_path(chunk_hash: str) -> Path:
    # Two-level fan-out keeps directories small.
    return CHUNK_DIR / chunk_hash[:2] / chunk_hash[2:4] / chunk_hash


def count_chunks() -> int:
    return sum(1 for p in CHUNK_DIR.rglob("*") if p.is_file() and HASH_RE.match(p.name))


class State:
    chunk_count = 0


state = State()


def valid_hash(chunk_hash: str) -> str:
    if not HASH_RE.match(chunk_hash):
        raise HTTPException(400, "chunk id must be a lowercase hex SHA-256 (64 chars)")
    return chunk_hash


async def heartbeat_loop():
    async with httpx.AsyncClient(timeout=3.0) as client:
        while True:
            payload = {
                "node_id": NODE_ID,
                "address": PUBLIC_ADDR,
                "free_bytes": shutil.disk_usage(DATA_DIR).free,
                "chunk_count": state.chunk_count,
                "timestamp": time.time(),
            }
            try:
                await client.post(f"{METADATA_URL}/heartbeat", json=payload)
            except httpx.HTTPError as exc:
                log.warning("heartbeat to %s failed: %s", METADATA_URL, exc)
            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(_: FastAPI):
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    state.chunk_count = count_chunks()
    log.info("%s starting with %d chunks in %s", NODE_ID, state.chunk_count, DATA_DIR)
    task = asyncio.create_task(heartbeat_loop())
    yield
    task.cancel()


app = FastAPI(title=f"storage-{NODE_ID}", lifespan=lifespan)


@app.put("/chunks/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request, response: Response):
    valid_hash(chunk_hash)
    declared = request.headers.get("content-length")
    if declared and int(declared) > MAX_CHUNK_BYTES:
        raise HTTPException(413, "chunk too large")
    data = await request.body()
    if len(data) > MAX_CHUNK_BYTES:
        raise HTTPException(413, "chunk too large")
    actual = hashlib.sha256(data).hexdigest()
    if actual != chunk_hash:
        raise HTTPException(400, f"hash mismatch: body hashes to {actual}")

    path = chunk_path(chunk_hash)
    if path.exists():
        response.status_code = 200  # idempotent: same hash means same bytes
        return {"chunk": chunk_hash, "size": len(data), "stored": False}

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{chunk_hash}.{os.getpid()}.tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)  # atomic: readers never see a partial chunk
    state.chunk_count += 1
    response.status_code = 201
    return {"chunk": chunk_hash, "size": len(data), "stored": True}


@app.get("/chunks/{chunk_hash}")
async def get_chunk(chunk_hash: str):
    valid_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        raise HTTPException(404, "chunk not found")
    if hashlib.sha256(data).hexdigest() != chunk_hash:
        log.error("chunk %s is corrupted on disk; quarantining", chunk_hash)
        os.replace(path, QUARANTINE_DIR / f"{chunk_hash}.{int(time.time())}")
        state.chunk_count -= 1
        raise HTTPException(409, "chunk is corrupted; removed from service")
    return Response(content=data, media_type="application/octet-stream",
                    headers={"X-Chunk-Hash": chunk_hash})


@app.head("/chunks/{chunk_hash}")
async def head_chunk(chunk_hash: str):
    valid_hash(chunk_hash)
    try:
        size = chunk_path(chunk_hash).stat().st_size
    except FileNotFoundError:
        return Response(status_code=404)
    return Response(status_code=200, headers={"Content-Length": str(size),
                                              "X-Chunk-Hash": chunk_hash})


@app.delete("/chunks/{chunk_hash}", status_code=204)
async def delete_chunk(chunk_hash: str):
    valid_hash(chunk_hash)
    try:
        chunk_path(chunk_hash).unlink()
    except FileNotFoundError:
        raise HTTPException(404, "chunk not found")
    state.chunk_count -= 1
    return Response(status_code=204)


@app.get("/health")
async def health():
    return {
        "node_id": NODE_ID,
        "chunk_count": state.chunk_count,
        "free_bytes": shutil.disk_usage(DATA_DIR).free,
    }
