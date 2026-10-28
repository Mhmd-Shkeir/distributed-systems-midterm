"""Minimal stand-in for the metadata service: records heartbeats only.

Replaced by the real metadata service in a later step; it exists so the storage
nodes have something to send heartbeats to and so we can see them arrive.
"""
import os
import time

from fastapi import FastAPI
from pydantic import BaseModel

DEAD_AFTER = float(os.environ.get("DEAD_AFTER", "15"))

app = FastAPI(title="metadata-stub")
nodes: dict[str, dict] = {}


class Heartbeat(BaseModel):
    node_id: str
    address: str = ""
    free_bytes: int
    chunk_count: int
    timestamp: float


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    nodes[hb.node_id] = {**hb.model_dump(), "last_seen": time.time()}
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    return [
        {**n, "age_seconds": round(now - n["last_seen"], 1),
         "alive": now - n["last_seen"] < DEAD_AFTER}
        for n in sorted(nodes.values(), key=lambda n: n["node_id"])
    ]
