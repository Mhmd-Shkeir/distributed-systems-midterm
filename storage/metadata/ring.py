"""Consistent hashing ring with virtual nodes.

Each physical node is placed on the ring at `vnodes` pseudo-random points. A
chunk is owned by the first node clockwise from the chunk's position, and the
next distinct nodes clockwise serve as additional replicas. Adding or removing a
node only moves the keys adjacent to its points (about 1/N of all keys).
"""
import bisect
import hashlib
import threading


def _point(label: str) -> int:
    return int.from_bytes(hashlib.sha256(label.encode()).digest()[:8], "big")


def chunk_point(chunk_hash: str) -> int:
    # Chunk ids are already SHA-256 digests, so their prefix is uniform.
    return int(chunk_hash[:16], 16)


class HashRing:
    def __init__(self, vnodes: int = 128):
        self.vnodes = vnodes
        self._nodes: set[str] = set()
        self._keys: list[int] = []
        self._owners: list[str] = []
        self._lock = threading.Lock()

    def _rebuild(self):
        points = sorted((_point(f"{node}#{i}"), node) for node in self._nodes for i in range(self.vnodes))
        self._keys = [p for p, _ in points]
        self._owners = [n for _, n in points]

    def add_node(self, node_id: str) -> bool:
        with self._lock:
            if node_id in self._nodes:
                return False
            self._nodes.add(node_id)
            self._rebuild()
            return True

    def remove_node(self, node_id: str) -> bool:
        with self._lock:
            if node_id not in self._nodes:
                return False
            self._nodes.discard(node_id)
            self._rebuild()
            return True

    @property
    def nodes(self) -> list[str]:
        with self._lock:
            return sorted(self._nodes)

    def lookup(self, chunk_hash: str, n: int = 1, accept=lambda node: True) -> list[str]:
        """Return up to n distinct nodes for a chunk, walking clockwise.

        `accept` lets the caller skip nodes (for example ones that are down)
        without changing where healthy nodes sit on the ring.
        """
        with self._lock:
            keys, owners = self._keys, self._owners
            if not keys:
                return []
            start = bisect.bisect_right(keys, chunk_point(chunk_hash)) % len(keys)
            found: list[str] = []
            for step in range(len(keys)):
                node = owners[(start + step) % len(keys)]
                if node not in found and accept(node):
                    found.append(node)
                    if len(found) == n:
                        break
            return found
