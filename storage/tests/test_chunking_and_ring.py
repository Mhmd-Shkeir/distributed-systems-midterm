import hashlib
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metadata.chunking import cdc_chunks, fixed_chunks  # noqa: E402
from metadata.ring import HashRing  # noqa: E402

KiB = 1024
CDC = dict(min_size=16 * KiB, avg_size=64 * KiB, max_size=256 * KiB)


def hashes(data, spans):
    return [hashlib.sha256(data[s:e]).hexdigest() for s, e in spans]


def rand(n, seed=1):
    return random.Random(seed).randbytes(n)


# ---------- chunking ----------

def test_fixed_covers_data_with_equal_chunks():
    data = rand(10 * KiB + 7)
    spans = fixed_chunks(data, 4 * KiB)
    assert spans == [(0, 4096), (4096, 8192), (8192, 10247)]
    assert b"".join(data[s:e] for s, e in spans) == data


def test_cdc_reassembles_and_respects_size_limits():
    data = rand(4 * 1024 * KiB)
    spans = cdc_chunks(data, **CDC)
    assert spans[0][0] == 0 and spans[-1][1] == len(data)
    assert all(a[1] == b[0] for a, b in zip(spans, spans[1:]))
    sizes = [e - s for s, e in spans]
    assert all(CDC["min_size"] <= z <= CDC["max_size"] for z in sizes[:-1])
    assert sizes[-1] <= CDC["max_size"]
    avg = sum(sizes) / len(sizes)
    assert 32 * KiB < avg < 160 * KiB, avg


def test_cdc_is_deterministic_and_handles_edges():
    data = rand(1024 * KiB)
    assert cdc_chunks(data, **CDC) == cdc_chunks(data, **CDC)
    assert cdc_chunks(b"", **CDC) == []
    assert cdc_chunks(b"abc", **CDC) == [(0, 3)]


def test_cdc_survives_insertion_but_fixed_does_not():
    base = rand(4 * 1024 * KiB, seed=7)
    edited = b"INSERTED-BYTES-AT-THE-FRONT" + base

    cdc_a, cdc_b = (set(hashes(d, cdc_chunks(d, **CDC))) for d in (base, edited))
    fix_a, fix_b = (set(hashes(d, fixed_chunks(d, 64 * KiB))) for d in (base, edited))

    cdc_shared = len(cdc_a & cdc_b) / len(cdc_a)
    fix_shared = len(fix_a & fix_b) / len(fix_a)
    assert cdc_shared > 0.9, cdc_shared
    assert fix_shared < 0.05, fix_shared


# ---------- hash ring ----------

def keys(n, seed=3):
    r = random.Random(seed)
    return [hashlib.sha256(r.randbytes(16)).hexdigest() for _ in range(n)]


def make_ring(names, vnodes=128):
    ring = HashRing(vnodes)
    for name in names:
        ring.add_node(name)
    return ring


def test_ring_distributes_evenly():
    ring = make_ring(["node1", "node2", "node3", "node4"])
    counts = {}
    for k in keys(20000):
        owner = ring.lookup(k)[0]
        counts[owner] = counts.get(owner, 0) + 1
    assert set(counts) == {"node1", "node2", "node3", "node4"}
    ideal = 20000 / 4
    assert all(0.75 * ideal < c < 1.25 * ideal for c in counts.values()), counts


def test_ring_is_deterministic():
    a, b = make_ring(["n1", "n2", "n3"]), make_ring(["n3", "n1", "n2"])
    assert all(a.lookup(k) == b.lookup(k) for k in keys(500))


def test_adding_a_node_only_moves_keys_to_the_new_node():
    ring = make_ring(["n1", "n2", "n3", "n4"])
    ks = keys(10000)
    before = {k: ring.lookup(k)[0] for k in ks}
    ring.add_node("n5")
    after = {k: ring.lookup(k)[0] for k in ks}
    moved = [k for k in ks if before[k] != after[k]]
    assert all(after[k] == "n5" for k in moved)
    assert 0.10 < len(moved) / len(ks) < 0.30, len(moved) / len(ks)  # ideal 1/5


def test_removing_a_node_only_moves_that_nodes_keys():
    ring = make_ring(["n1", "n2", "n3", "n4"])
    ks = keys(10000)
    before = {k: ring.lookup(k)[0] for k in ks}
    ring.remove_node("n3")
    for k in ks:
        if before[k] != "n3":
            assert ring.lookup(k)[0] == before[k]
        else:
            assert ring.lookup(k)[0] != "n3"


def test_replicas_are_distinct_and_accept_filter_skips_nodes():
    ring = make_ring(["n1", "n2", "n3", "n4"])
    for k in keys(200):
        reps = ring.lookup(k, 3)
        assert len(reps) == 3 and len(set(reps)) == 3
        skipped = ring.lookup(k, 3, accept=lambda n: n != reps[0])
        assert reps[0] not in skipped and len(skipped) == 3
    assert make_ring(["only"]).lookup(keys(1)[0], 3) == ["only"]
    assert HashRing().lookup(keys(1)[0]) == []
