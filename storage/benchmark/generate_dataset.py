"""Deterministic workload generators for the benchmark.

  small : many small files, sizes uniform between 1 KiB and 500 KiB
  large : a few large files of several MiB (not GiB, to keep time and network use reasonable)

The content is pseudo-random from a fixed seed, so every run (and every machine) produces
the same files. The benchmark imports `workload()`; the command line writes the files to
disk together with a manifest (name, size, sha256) so the datasets can be inspected or reused.

  python generate_dataset.py small --out datasets/small
  python generate_dataset.py large --out datasets/large
"""
import argparse
import hashlib
import json
import os
import random

KiB, MiB = 1024, 1024 * 1024
DEFAULT_COUNT = {"small": 600}
DEFAULT_LARGE_MIB = [8, 12, 16, 24, 32, 40]


def workload(name: str, count: int | None = None, seed: int = 1):
    """Yield (file name, bytes) for the named workload."""
    rng = random.Random(seed)
    if name == "small":
        for i in range(count or DEFAULT_COUNT["small"]):
            yield f"small/file-{i:05d}.bin", rng.randbytes(rng.randint(1 * KiB, 500 * KiB))
    elif name == "large":
        sizes = DEFAULT_LARGE_MIB[:count] if count else DEFAULT_LARGE_MIB
        for i, mib in enumerate(sizes):
            yield f"large/file-{i:02d}-{mib}MiB.bin", rng.randbytes(mib * MiB)
    else:
        raise ValueError(f"unknown workload {name!r}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("name", choices=["small", "large"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--count", type=int)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    manifest, total = [], 0
    for fname, data in workload(a.name, a.count, a.seed):
        path = os.path.join(a.out, os.path.basename(fname))
        with open(path, "wb") as f:
            f.write(data)
        manifest.append({"file": os.path.basename(fname), "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
        total += len(data)
    with open(os.path.join(a.out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1)
    print(f"{len(manifest)} files, {total / MiB:.1f} MiB written to {a.out}")


if __name__ == "__main__":
    main()
