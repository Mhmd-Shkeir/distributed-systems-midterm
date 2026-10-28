"""Reed-Solomon erasure coding over GF(256).

A chunk is cut into k data shards and m parity shards; any k of the k+m shards
rebuild the chunk, so up to m lost shards are tolerated at (k+m)/k storage cost
(replication with 3 copies costs 3x and also tolerates 2 losses).

The code is systematic: shards 0..k-1 are the data itself, so a healthy read
needs no decoding. Parity uses a Cauchy matrix, every square submatrix of
[I; C] is invertible, so any k shards are enough.
"""
import numpy as np

_PRIM = 0x11D

_EXP = np.zeros(512, dtype=np.uint16)
_LOG = np.zeros(256, dtype=np.uint16)
_x = 1
for _i in range(255):
    _EXP[_i] = _x
    _LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= _PRIM
for _i in range(255, 512):
    _EXP[_i] = _EXP[_i - 255]

# MUL[c][b] = c * b in GF(256), one 256-entry lookup row per constant.
MUL = np.zeros((256, 256), dtype=np.uint8)
for _c in range(1, 256):
    for_b = np.arange(1, 256)
    MUL[_c, 1:] = _EXP[_LOG[_c] + _LOG[for_b]].astype(np.uint8)


def _inv(a: int) -> int:
    if a == 0:
        raise ZeroDivisionError
    return int(_EXP[255 - _LOG[a]])


def _cauchy(k: int, m: int) -> list[list[int]]:
    if k < 1 or m < 1 or k + m > 256:
        raise ValueError("need k >= 1, m >= 1, k + m <= 256")
    return [[_inv((k + j) ^ i) for i in range(k)] for j in range(m)]


def _row(k: int, m: int, idx: int) -> list[int]:
    """Row of the (k+m) x k generator matrix for shard idx."""
    if idx < k:
        return [1 if i == idx else 0 for i in range(k)]
    return _cauchy(k, m)[idx - k]


def _mul_acc(out: np.ndarray, coef: int, shard: np.ndarray):
    if coef == 1:
        out ^= shard
    elif coef:
        out ^= MUL[coef][shard]


def shard_size(size: int, k: int) -> int:
    return -(-size // k)


def encode(data: bytes, k: int, m: int) -> list[bytes]:
    """Return k data shards followed by m parity shards."""
    L = shard_size(len(data), k)
    arr = np.frombuffer(data.ljust(L * k, b"\0"), dtype=np.uint8).reshape(k, L)
    shards = [arr[i].tobytes() for i in range(k)]
    for coefs in _cauchy(k, m):
        p = np.zeros(L, dtype=np.uint8)
        for i, c in enumerate(coefs):
            _mul_acc(p, c, arr[i])
        shards.append(p.tobytes())
    return shards


def _invert(mat: list[list[int]]) -> list[list[int]]:
    n = len(mat)
    a = [row[:] + [1 if i == j else 0 for j in range(n)] for i, row in enumerate(mat)]
    for col in range(n):
        piv = next((r for r in range(col, n) if a[r][col]), None)
        if piv is None:
            raise ValueError("singular matrix")
        a[col], a[piv] = a[piv], a[col]
        iv = _inv(a[col][col])
        a[col] = [int(MUL[iv][v]) for v in a[col]]
        for r in range(n):
            if r != col and a[r][col]:
                f = a[r][col]
                a[r] = [v ^ int(MUL[f][a[col][j]]) for j, v in enumerate(a[r])]
    return [row[n:] for row in a]


def decode(shards: dict[int, bytes], k: int, m: int, size: int) -> bytes:
    """Rebuild the original `size` bytes from any k shards ({index: bytes})."""
    if len(shards) < k:
        raise ValueError(f"need {k} shards, have {len(shards)}")
    L = shard_size(size, k)
    if all(i in shards for i in range(k)):
        return b"".join(shards[i] for i in range(k))[:size]
    use = sorted(shards)[:k]
    inv = _invert([_row(k, m, i) for i in use])
    got = [np.frombuffer(shards[i], dtype=np.uint8) for i in use]
    out = []
    for r in range(k):
        acc = np.zeros(L, dtype=np.uint8)
        for j in range(k):
            _mul_acc(acc, inv[r][j], got[j])
        out.append(acc.tobytes())
    return b"".join(out)[:size]
