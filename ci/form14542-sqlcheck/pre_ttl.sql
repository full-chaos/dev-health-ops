-- PRE-TTL-14542-1 (fcsup lane l14542, 2026-11-23): metadata-only retention check of the local SigNoz trace table. Reads system.parts only.
-- Output: active_parts, oldest_days (days from the oldest active part min_time to now()). Counts and days only.
-- WANT active_parts > 0 AND oldest_days >= 2. Gate: an unfilled min_time (1970) throws STOP-MIN-TIME-UNFILLED = STOP, no further read.
-- No SET, no SETTINGS.
SELECT count() AS active_parts, dateDiff('day', min(min_time), now()) AS oldest_days
FROM system.parts
WHERE database = 'signoz_traces' AND table = 'signoz_index_v3' AND active
HAVING throwIf(min(min_time) < toDateTime('2020-01-01'), 'STOP-MIN-TIME-UNFILLED') = 0
