import os

MiB = 1024 * 1024

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://dfs:dfs@postgres:5432/dfs")

# Durability defaults (overridable per upload)
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", "3"))
EC_K = int(os.environ.get("EC_K", "2"))
EC_M = int(os.environ.get("EC_M", "2"))
DEFAULT_DURABILITY = os.environ.get("DEFAULT_DURABILITY", "replication")  # replication | ec
DEFAULT_CHUNKING = os.environ.get("DEFAULT_CHUNKING", "fixed")  # fixed | cdc
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(4 * MiB)))

VNODES = int(os.environ.get("VNODES", "128"))
DEAD_AFTER = float(os.environ.get("DEAD_AFTER", "15"))  # seconds without heartbeat = failed
MAX_OBJECT_BYTES = int(os.environ.get("MAX_OBJECT_BYTES", str(128 * MiB)))

# Background maintenance
GC_INTERVAL = float(os.environ.get("GC_INTERVAL", "30"))
REPAIR_INTERVAL = float(os.environ.get("REPAIR_INTERVAL", "10"))
SCRUB_INTERVAL = float(os.environ.get("SCRUB_INTERVAL", "300"))
