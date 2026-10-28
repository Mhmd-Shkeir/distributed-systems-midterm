"""Postgres access: connection pool, schema and advisory locks."""
import logging

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import DATABASE_URL

log = logging.getLogger("metadata.db")
SCHEMA_VERSION = 2

pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=24, open=False, kwargs={"row_factory": dict_row})

TABLES = ["gc_queue", "repair_log", "object_chunks", "chunk_locations", "chunks", "objects", "buckets", "nodes"]

SCHEMA = """
CREATE TABLE nodes (
    node_id     text PRIMARY KEY,
    address     text NOT NULL,
    free_bytes  bigint NOT NULL,
    chunk_count integer NOT NULL,
    reported_at double precision NOT NULL,
    last_seen   timestamptz NOT NULL DEFAULT now(),
    in_ring     boolean NOT NULL DEFAULT true
);
CREATE TABLE buckets (
    name       text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE objects (
    id           bigserial PRIMARY KEY,
    bucket       text NOT NULL REFERENCES buckets(name),
    key          text NOT NULL,
    size         bigint NOT NULL,
    sha256       text NOT NULL,
    content_type text NOT NULL DEFAULT 'application/octet-stream',
    meta         jsonb NOT NULL DEFAULT '{}',
    chunking     text NOT NULL,
    durability   text NOT NULL,
    chunk_count  integer NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket, key)
);
CREATE INDEX objects_key_pattern ON objects (bucket, key text_pattern_ops);
-- A chunk is identified by the SHA-256 of its bytes and stored once (deduplication).
-- mode 'rep': `copies` full copies. mode 'ec': k data + m parity shards.
CREATE TABLE chunks (
    hash     text PRIMARY KEY,
    size     integer NOT NULL,
    refcount integer NOT NULL CHECK (refcount >= 0),
    mode     text NOT NULL,
    copies   integer NOT NULL DEFAULT 1,
    k        integer NOT NULL DEFAULT 0,
    m        integer NOT NULL DEFAULT 0
);
-- Where the bytes live. For replication shard_idx is 0 and shard_hash = chunk hash;
-- for erasure coding there is one row per shard.
CREATE TABLE chunk_locations (
    chunk_hash text NOT NULL REFERENCES chunks(hash) ON DELETE CASCADE,
    shard_idx  integer NOT NULL,
    shard_hash text NOT NULL,
    node_id    text NOT NULL,
    size       integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_hash, shard_idx, node_id)
);
CREATE INDEX chunk_locations_node ON chunk_locations (node_id);
CREATE INDEX chunk_locations_shard ON chunk_locations (shard_hash, node_id);
CREATE TABLE object_chunks (
    object_id  bigint NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    seq        integer NOT NULL,
    chunk_hash text NOT NULL REFERENCES chunks(hash),
    PRIMARY KEY (object_id, seq)
);
CREATE INDEX object_chunks_hash ON object_chunks (chunk_hash);
-- Copies on a node that no chunk row references any more; removed by the GC.
CREATE TABLE gc_queue (
    hash    text NOT NULL,
    node_id text NOT NULL,
    PRIMARY KEY (hash, node_id)
);
CREATE TABLE repair_log (
    id         bigserial PRIMARY KEY,
    at         timestamptz NOT NULL DEFAULT now(),
    kind       text NOT NULL,
    chunk_hash text,
    detail     text NOT NULL
);
"""


def init_schema():
    """Create the schema; a different (older) version is dropped and recreated."""
    with pool.connection() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS schema_meta (version integer NOT NULL)")
        row = conn.execute("SELECT version FROM schema_meta").fetchone()
        if row and row["version"] == SCHEMA_VERSION:
            return
        log.warning("creating schema version %d", SCHEMA_VERSION)
        for t in TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        conn.execute(SCHEMA)
        conn.execute("DELETE FROM schema_meta")
        conn.execute("INSERT INTO schema_meta (version) VALUES (%s)", (SCHEMA_VERSION,))


def lock_keys(conn, keys):
    """Take transaction-scoped advisory locks in one global (sorted) order.

    Every operation that adds, removes or moves a stored hash locks all the hashes
    it touches up front, in sorted order, so concurrent operations cannot deadlock
    and a garbage-collection pass cannot delete a copy that is being re-uploaded.
    """
    for key in sorted(set(keys)):
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (key,))


def log_event(conn, kind: str, chunk_hash: str | None, detail: str):
    conn.execute("INSERT INTO repair_log (kind, chunk_hash, detail) VALUES (%s, %s, %s)", (kind, chunk_hash, detail))
