-- PRECHECK-S-14542-1 (fcsup lane l14542, 2026-11-23): S = spans of ALL services in the ts_bucket_start buckets that each scan of FORM-14542-1-r2 reads.
-- Window: START 2026-11-23T06:48:23Z = 1795416503 s, END 2026-11-24T06:48:23Z = 1795502903 s. Lower bound = START_s - 1800 = 1795414703. No timestamp filter (a scan reads every row of those buckets).
-- Self-gate: the scalar cost_gate reads only system.parts of the local table (3 daily partitions that hold the range). STOP-COST-GATE when the rows are 0 (wrong name) or > 20,000,000. Then ONE count() over the primary-key range.
-- WANT S > 0 AND S <= 2857142. S = 0 or S > 2857142 = STOP, no FORM call. No SET, no SETTINGS.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts
    WHERE active AND database = 'signoz_traces' AND table = 'signoz_index_v3'
      AND partition IN ('2026-11-22', '2026-11-23', '2026-11-24')) NOT BETWEEN 1 AND 20000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS s_spans
FROM signoz_traces.distributed_signoz_index_v3
WHERE ts_bucket_start >= 1795414703
  AND ts_bucket_start <= 1795502903
  AND cost_gate = 0
