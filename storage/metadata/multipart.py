"""S3-style multipart upload, for large files.

  POST   /{bucket}/{key}?uploads                      -> {"uploadId": ...}
  PUT    /{bucket}/{key}?uploadId=..&partNumber=N     -> stores one part, returns its ETag
  POST   /{bucket}/{key}?uploadId=..                  -> completes: the parts become one object
  DELETE /{bucket}/{key}?uploadId=..                  -> aborts and frees the parts

Each part is stored like a small object under a hidden key (.multipart/<id>/<part>), so it is chunked,
deduplicated, replicated or erasure coded exactly like any other data. Completing the upload moves the
parts' chunk references to the final object, so nothing is copied and no reference count changes.
"""
import hashlib
import uuid

from fastapi import HTTPException

from . import store
from .db import pool

TMP = ".multipart/"


def _part_key(uid: str, n: int) -> str:
    return f"{TMP}{uid}/{n:05d}"


def _upload(conn, bucket: str, key: str, uid: str):
    row = conn.execute("SELECT * FROM multipart_uploads WHERE upload_id=%s AND bucket=%s AND key=%s", (uid, bucket, key)).fetchone()
    if not row:
        raise HTTPException(404, "NoSuchUpload")
    return row


def initiate(bucket: str, key: str, content_type: str, meta: dict) -> str:
    import json
    with pool.connection() as conn:
        if not conn.execute("SELECT 1 FROM buckets WHERE name=%s", (bucket,)).fetchone():
            raise HTTPException(404, f"NoSuchBucket: {bucket}")
        uid = uuid.uuid4().hex
        conn.execute("INSERT INTO multipart_uploads (upload_id, bucket, key, content_type, meta) VALUES (%s,%s,%s,%s,%s::jsonb)",
                     (uid, bucket, key, content_type, json.dumps(meta)))
    return uid


def upload_part(bucket: str, key: str, uid: str, n: int, data: bytes) -> str:
    if not 1 <= n <= 10000:
        raise HTTPException(400, "partNumber must be between 1 and 10000")
    with pool.connection() as conn:
        _upload(conn, bucket, key, uid)
    d = store.resolve_durability(bucket)
    res = store.put_object(bucket, _part_key(uid, n), data, "fixed", {"chunk_size": 4 * 1024 * 1024}, d,
                           "application/octet-stream", {})
    with pool.connection() as conn:
        conn.execute("INSERT INTO multipart_parts (upload_id, part_number, size, etag) VALUES (%s,%s,%s,%s) "
                     "ON CONFLICT (upload_id, part_number) DO UPDATE SET size=EXCLUDED.size, etag=EXCLUDED.etag",
                     (uid, n, len(data), res["etag"]))
    return res["etag"]


def complete(bucket: str, key: str, uid: str) -> dict:
    with pool.connection() as conn:
        up = _upload(conn, bucket, key, uid)
        parts = conn.execute("SELECT part_number, size, etag FROM multipart_parts WHERE upload_id=%s ORDER BY part_number", (uid,)).fetchall()
    if not parts:
        raise HTTPException(400, "no parts were uploaded")
    store.delete_object(bucket, key)  # S3 semantics: completing over an existing key replaces it
    with pool.connection() as conn:
        objs = conn.execute("SELECT id, key, durability, chunk_count FROM objects WHERE bucket=%s AND key = ANY(%s)",
                            (bucket, [_part_key(uid, p["part_number"]) for p in parts])).fetchall()
        by_key = {o["key"]: o for o in objs}
        if len(by_key) != len(parts):
            raise HTTPException(500, "a part is missing")
        total = sum(p["size"] for p in parts)
        etag = hashlib.sha256("".join(p["etag"] for p in parts).encode()).hexdigest() + f"-{len(parts)}"
        first = by_key[_part_key(uid, parts[0]["part_number"])]
        import json
        new = conn.execute(
            "INSERT INTO objects (bucket, key, size, sha256, content_type, meta, chunking, durability, chunk_count) "
            "VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s) RETURNING id",
            (bucket, key, total, etag, up["content_type"], json.dumps(up["meta"]), "multipart", first["durability"],
             sum(by_key[_part_key(uid, p["part_number"])]["chunk_count"] for p in parts))).fetchone()
        offset = 0
        for p in parts:
            o = by_key[_part_key(uid, p["part_number"])]
            conn.execute("UPDATE object_chunks SET object_id=%s, seq=seq+%s WHERE object_id=%s", (new["id"], offset, o["id"]))
            offset += o["chunk_count"]
        conn.execute("DELETE FROM objects WHERE id = ANY(%s)", ([o["id"] for o in objs],))
        conn.execute("DELETE FROM multipart_uploads WHERE upload_id=%s", (uid,))
    return {"bucket": bucket, "key": key, "etag": etag, "size": total, "parts": len(parts)}


def abort(bucket: str, key: str, uid: str):
    with pool.connection() as conn:
        _upload(conn, bucket, key, uid)
        nums = [r["part_number"] for r in conn.execute("SELECT part_number FROM multipart_parts WHERE upload_id=%s", (uid,)).fetchall()]
    for n in nums:
        store.delete_object(bucket, _part_key(uid, n))
    with pool.connection() as conn:
        conn.execute("DELETE FROM multipart_uploads WHERE upload_id=%s", (uid,))
