import itertools
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metadata import ec  # noqa: E402


@pytest.mark.parametrize("k,m", [(2, 2), (3, 1), (4, 2), (1, 2), (6, 3)])
@pytest.mark.parametrize("size", [1, 5, 1000, 4096, 100003])
def test_any_k_shards_rebuild_the_chunk(k, m, size):
    data = os.urandom(size)
    shards = ec.encode(data, k, m)
    assert len(shards) == k + m
    assert len({len(s) for s in shards}) == 1
    assert b"".join(shards[:k])[:size] == data  # systematic: data shards are the data
    for keep in itertools.combinations(range(k + m), k):
        assert ec.decode({i: shards[i] for i in keep}, k, m, size) == data, keep


def test_fewer_than_k_shards_is_an_error():
    shards = ec.encode(os.urandom(1000), 3, 2)
    with pytest.raises(ValueError):
        ec.decode({0: shards[0], 4: shards[4]}, 3, 2, 1000)


def test_rebuilt_parity_matches_original():
    data = os.urandom(50000)
    shards = ec.encode(data, 2, 2)
    rebuilt = ec.decode({1: shards[1], 3: shards[3]}, 2, 2, len(data))
    assert ec.encode(rebuilt, 2, 2) == shards


def test_storage_overhead_is_lower_than_replication():
    data = os.urandom(1_000_000)
    stored = sum(len(s) for s in ec.encode(data, 4, 2))
    assert stored / len(data) == pytest.approx(1.5, rel=0.01)  # 3 copies would be 3.0
