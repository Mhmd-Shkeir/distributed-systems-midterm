# Benchmark results: replication versus erasure coding

## Storage overhead and throughput

| workload | durability | files | logical_mib | stored_mib | overhead | put_mib_s | put_p50_ms | put_p95_ms | get_mib_s | get_p50_ms | get_p95_ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| small | replication N=3 (W=2,R=2) | 60 | 13.3 | 40.0 | 3.0 | 7.5 | 240.2 | 283.2 | 15.8 | 105.3 | 172.9 |
| small | erasure coding 2+1 | 60 | 13.3 | 20.0 | 1.5 | 5.7 | 269.3 | 602.2 | 18.6 | 93.7 | 121.1 |
| large | replication N=3 (W=2,R=2) | 3 | 36.0 | 108.0 | 3.0 | 32.9 | 584.2 | 663.9 | 88.2 | 228.4 | 251.7 |
| large | erasure coding 2+1 | 3 | 36.0 | 54.0 | 1.5 | 47.0 | 412.0 | 461.5 | 104.8 | 218.0 | 219.2 |

## Repair after the failure of one node

| workload | durability | lost_mib | detect_seconds | repair_seconds | repair_read_mib | repair_written_mib | read_amplification | write_amplification | repair_complete |
|---|---|---|---|---|---|---|---|---|---|
| small | replication N=3 (W=2,R=2) | 11.4 | 15.1 | 21.2 | 11.4 | 11.4 | 1.0 | 1.0 | True |
| small | erasure coding 2+1 | 5.7 | 11.0 | 13.1 | 11.4 | 5.7 | 2.0 | 1.0 | True |
| large | replication N=3 (W=2,R=2) | 28.0 | 11.3 | 13.4 | 28.0 | 28.0 | 1.0 | 1.0 | True |
| large | erasure coding 2+1 | 14.0 | 14.1 | 16.1 | 28.0 | 14.0 | 2.0 | 1.0 | True |
