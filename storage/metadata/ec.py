"""Reed-Solomon erasure coding with zfec.

A chunk is cut into k data shards and m parity shards; any k of the k+m shards
rebuild the chunk, so up to m lost shards are tolerated at (k+m)/k storage cost
(replication with 3 copies costs 3x and also tolerates 2 losses).

zfec is systematic: shards 0..k-1 are the data itself, so a healthy read needs no
decoding. Shards are stored on different nodes, each under its own SHA-256.
"""
from zfec import easyfec


def shard_size(size: int, k: int) -> int:
    return -(-size // k)


def _check(k: int, m: int):
    if k < 1 or m < 1 or k + m > 256:
        raise ValueError("need k >= 1, m >= 1, k + m <= 256")


def encode(data: bytes, k: int, m: int) -> list[bytes]:
    """Return k data shards followed by m parity shards."""
    _check(k, m)
    return [bytes(s) for s in easyfec.Encoder(k, k + m).encode(data)]


def decode(shards: dict[int, bytes], k: int, m: int, size: int) -> bytes:
    """Rebuild the original `size` bytes from any k shards ({index: bytes})."""
    _check(k, m)
    if len(shards) < k:
        raise ValueError(f"need {k} shards, have {len(shards)}")
    use = sorted(shards)[:k]
    if use == list(range(k)):  # all data shards present: no decoding needed
        return b"".join(shards[i] for i in use)[:size]
    padlen = (k - size % k) % k
    return bytes(easyfec.Decoder(k, k + m).decode([shards[i] for i in use], use, padlen))[:size]
