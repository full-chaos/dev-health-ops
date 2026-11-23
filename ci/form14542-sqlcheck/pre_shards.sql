-- PRE-SHARDS-14542-1 (fcsup lane l14542, 2026-11-23): shard count of the cluster named in the Distributed engine of distributed_signoz_index_v3.
-- Cluster name 'cluster' = the first engine argument of ddl_distributed.sql line 2: Distributed('cluster', 'signoz_traces', 'signoz_index_v3', cityHash64(trace_id)).
-- Output: rows, shards. Counts only. WANT rows > 0 AND shards = 1. Gate: rows = 0 throws STOP-NO-CLUSTER = STOP.
-- No SET, no SETTINGS.
SELECT count() AS rows, countDistinct(shard_num) AS shards
FROM system.clusters
WHERE cluster = 'cluster'
HAVING throwIf(count() = 0, 'STOP-NO-CLUSTER') = 0
