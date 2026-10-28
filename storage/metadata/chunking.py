"""Splitting data into chunks: fixed-size and content-defined (CDC).

Both functions return a list of (start, end) byte spans covering the data.

Content-defined chunking uses a Gear rolling hash over a 32-byte window. A cut
point is declared where the hash's top `bits` bits are zero, so boundaries
depend on the bytes around them rather than on absolute offsets. Inserting or
deleting a few bytes therefore only changes the chunks near the edit, which is
what makes deduplication work on modified files. Minimum and maximum chunk sizes
bound the result.
"""
import hashlib
import math

import numpy as np

MiB = 1 << 20
WINDOW = 32
_BLOCK = 8 * MiB

# Deterministic gear table: the same boundaries on every machine and version.
GEAR = np.array(
    [int.from_bytes(hashlib.sha256(b"gear" + bytes([i])).digest()[:4], "big") for i in range(256)],
    dtype=np.uint32,
)


def fixed_chunks(data: bytes, size: int = 4 * MiB) -> list[tuple[int, int]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    return [(s, min(s + size, len(data))) for s in range(0, len(data), size)]


def _candidate_ends(arr: np.ndarray, bits: int) -> np.ndarray:
    """Positions e such that a cut after byte e-1 satisfies the hash condition."""
    mask = np.uint32(((1 << bits) - 1) << (32 - bits))
    n = len(arr)
    out = []
    for s in range(0, n, _BLOCK):
        lo = max(0, s - (WINDOW - 1))
        hi = min(n, s + _BLOCK)
        g = GEAR[arr[lo:hi]]
        h = np.zeros(len(g), dtype=np.uint32)
        for j in range(min(WINDOW, len(g))):
            h[j:] += g[: len(g) - j] << np.uint32(j)
        idx = np.flatnonzero((h & mask) == 0) + lo
        out.append(idx[idx >= s] + 1)
    return np.concatenate(out) if out else np.empty(0, dtype=np.int64)


def cdc_chunks(
    data: bytes,
    min_size: int = 256 * 1024,
    avg_size: int = 1 * MiB,
    max_size: int = 4 * MiB,
) -> list[tuple[int, int]]:
    if not (0 < min_size <= avg_size <= max_size):
        raise ValueError("need 0 < min <= avg <= max")
    n = len(data)
    if n == 0:
        return []
    arr = np.frombuffer(data, dtype=np.uint8)
    bits = max(8, min(31, round(math.log2(max(avg_size - min_size, 256)))))
    ends = _candidate_ends(arr, bits)

    spans = []
    start = 0
    while start < n:
        if n - start <= min_size:
            spans.append((start, n))
            break
        lo = start + min_size
        hi = min(start + max_size, n)
        k = int(np.searchsorted(ends, lo))
        end = int(ends[k]) if k < len(ends) and ends[k] <= hi else hi
        spans.append((start, end))
        start = end
    return spans


def split(data: bytes, strategy: str, **params) -> list[tuple[int, int]]:
    if strategy == "fixed":
        return fixed_chunks(data, params.get("chunk_size", 4 * MiB))
    if strategy == "cdc":
        return cdc_chunks(
            data,
            params.get("min_size", 256 * 1024),
            params.get("avg_size", 1 * MiB),
            params.get("max_size", 4 * MiB),
        )
    raise ValueError(f"unknown chunking strategy: {strategy}")
